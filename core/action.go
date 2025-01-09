/*
 * Copyright (c) 2024 donnie4w <donnie4w@gmail.com>. All rights reserved.
 * Original source: https://github.com/donnie4w/raftx
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package core

import (
	"context"
	"fmt"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"sort"
	"time"
)

func (n *Node) nodeInfoAction(addr string) (err error) {
	defer sys.Recoverable(&err)
	if mux := n.peers.getAddrMux(addr); mux != nil && mux.TryLock() {
		defer mux.Unlock()
		waitTimeout := time.After(n.waitTimeout)
	START:
		var cn csNet
		if cn, err = n.getCsNet(addr); err == nil {
			syncId := util.UUID64()
			if err = cn.NodeInfo(syncId, &stub.NodeInfo{Rtype: NODEINFO_SYNCID, Peers: n.getPeers(), NodeId: n.id}); err == nil {
				var id any
				if id, err = n.dataWait.Wait(syncId, n.heartbeatInterval+n.electionInterval); err == nil {
					n.peers.setId(addr, id.(int64))
				}
			}
			cn.Close()
		}
		if err != nil {
			select {
			case <-waitTimeout:
				err = fmt.Errorf("wait timeout, nodeInfoAction to node %s", addr)
				log.Error(err)
				return
			default:
				if !n.nodeClose {
					if cn == nil {
						<-time.After(time.Second)
					} else {
						<-time.After(n.heartbeatInterval + n.electionInterval)
					}
					goto START
				}
			}
		}
	}
	return
}

func (n *Node) nodeInfoNotifyAction(cn csNet) (err error) {
	defer sys.Recoverable(&err)
	count := 5
START:
	syncId := util.UUID64()
	if err = cn.NodeInfo(syncId, &stub.NodeInfo{Rtype: NODEINFO_NOTIFY, Peers: n.getPeers(), NodeId: n.id}); err == nil {
		_, err = n.dataWait.Wait(syncId, n.heartbeatInterval+n.electionInterval)
	}
	if err != nil && count > 0 {
		count--
		goto START
	}
	return
}

func (n *Node) nodeInfoNotifyAllAction() {
	for _, v := range n.peers.addrMapClone() {
		if v != 0 && v != n.id {
			if cn, ok := n.peersCn.Get(v); ok {
				n.nodeInfoNotifyAction(cn)
			}
		}
	}
}

func (n *Node) appendEntriesAction(leb *stub.LogEntryBat) error {
	appendEntries := &stub.AppendEntries{Term: n.term, Enstries: leb, NodeId: n.id}
	req := func(syncId int64, cn csNet) error {
		return cn.AppendEntries(syncId, appendEntries)
	}
	ack := func(r any) bool {
		if r != nil {
			return r.(*stub.AppendEntriesAck).Success
		}
		return false
	}
	if b, err := n.proxyCs(req, ack); err != nil {
		log.Error("appendEntriesActionn failed:", err.Error())
		return err
	} else if !b {
		log.Error("Failed to appendEntries Term:", leb.Term, ",NodeId:", leb.NodeId)
		return rafterrors.ErrProposalSyncFailed
	}
	return nil
}

func (n *Node) commitAction(logId, rxId int64) error {
	commit := &stub.Commit{Term: n.term, LogIndex: logId, RxId: rxId, LeaderId: n.id}
	req := func(syncId int64, cn csNet) error {
		return cn.Commit(syncId, commit)
	}
	ack := func(r any) bool {
		if r != nil {
			return r.(*stub.CommitAck).Success
		}
		return false
	}
	if b, err := n.proxyCs(req, ack); err != nil {
		return err
	} else if !b {
		return rafterrors.ErrProposalSyncFailed
	}
	return nil
}

func (n *Node) voteAction() {
	if !n.lock.VoteTl.TryLock() {
		return
	}
	defer n.lock.VoteTl.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bid := n.becomeId.Load()
	ch := make(chan *stub.VoteAck, n.peers.len()-1)
	defer func() {
		close(ch)
		ch = nil
	}()
	voteList := make([]*stub.VoteAck, 0)
	term := n.term
	ps := n.getPeers()
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	count := 0
	for _, peer := range ps {
		if v, b := n.peers.getId(peer); b {
			if v != 0 && v != n.id {
				count++
				go func(addr string) {
					defer sys.Recoverable(nil)
					if r, c, err := n.votePeer(ctx, addr, term); err == nil && ch != nil {
						voteList = append(voteList, r)
						ch <- r
					} else if !c {
						ch <- nil
					}
				}(peer)
			}
		}
	}
	ta := time.After(n.waitTimeout)
	var granted int
	for count > 0 {
		select {
		case v := <-ch:
			if v != nil {
				switch v.GetGranted() {
				case GRANTED_OK:
					granted++
				case GRANTED_DATA_OLD, GRANTED_TERM_OLD, GRANTED_ID_LITTLE, GRANTED_LEADER_ALIVE:
					n.becomeFollower(bid)
					return
				case GRANTED_VOTEOTHER:
					if vote := v.GetVoteFor(); vote != nil && vote.RxTotal > n.bat.RxTotal.Load() {
						n.becomeFollower(bid)
						return
					}
				}
			}
			if granted == n.peers.len()/2 {
				if n.votedFor.Vote == nil {
					n.lock.VoteSyncMux.Lock()
					if n.votedFor.Vote == nil {
						n.votedFor.VoteId, n.votedFor.Vote = n.id, &stub.Vote{NodeId: n.id, Term: n.term}
					}
					n.lock.VoteSyncMux.Unlock()
					n.voteParse(voteList, term, bid)
					return
				}
			}
			if granted > n.peers.len()/2 {
				n.voteToLeader(voteList, term, bid)
				return
			}
		case <-ta:
			n.voteParse(voteList, term, bid)
			return
		}
		count--
	}
	n.voteParse(voteList, term, bid)
}

func (n *Node) voteParse(voteList []*stub.VoteAck, term, bid int64) {
	if len(voteList) >= int(n.peers.len()/2) {
		m := make(map[int64]int, 0)
		hasLeader, isMaxNode, isMaxRx := false, true, true
		granted := GRANTED_VOTEOTHER
		if n.votedFor.Vote == nil {
			n.lock.VoteSyncMux.Lock()
			if n.votedFor.Vote == nil {
				granted = GRANTED_OK
				n.votedFor.VoteId, n.votedFor.Vote = n.id, &stub.Vote{NodeId: n.id, Term: n.term}
			}
			n.lock.VoteSyncMux.Unlock()
		} else if n.votedFor.Vote.NodeId == n.id {
			granted = GRANTED_OK
		}
		voteList = append(voteList, &stub.VoteAck{VoteFor: n.votedFor.Vote, NodeId: n.id, Term: n.term, RxTotal: n.bat.RxTotal.Load(), RxMax: n.bat.RxMax.Load(), LogMax: n.bat.LogMax.Load(), Granted: granted})
		grantedCount := 0
		for _, vt := range voteList {
			if vt.Granted == GRANTED_OK {
				grantedCount++
				if grantedCount > n.peers.len()/2 {
					break
				}
			}
			if vt.VoteFor != nil {
				v, _ := m[vt.VoteFor.NodeId]
				m[vt.VoteFor.NodeId] = 1 + v
				if 1+v > n.peers.len()/2 && vt.VoteFor.NodeId != n.id {
					hasLeader = true
					break
				}
			}
			if vt.RxTotal >= n.bat.RxTotal.Load() && vt.NodeId != n.id {
				isMaxRx = false
			}
			if vt.NodeId > n.id {
				isMaxNode = false
			}
		}
		toLeader := grantedCount > n.peers.len()/2 || (!hasLeader && (isMaxRx || (isMaxNode && len(voteList) >= n.peers.len()-1)))
		if toLeader {
			n.voteToLeader(voteList, term, bid)
		} else {
			n.becomeFollower(bid)
		}
		return
	} else {
		n.becomeFollower(bid)
		return
	}
}

func (n *Node) voteToLeader(voteList []*stub.VoteAck, term, bid int64) {
	if n.isLeader() {
		return
	}
	for _, vt := range voteList {
		n.bat.setRxMax(vt.RxMax)
		n.bat.setLogMax(vt.LogMax)
		n.mem.SetMaxId(vt.MemMaxId)
	}
	go n.becomeLeader(bid, term)
}

func (n *Node) votePeer(ctx context.Context, addr string, term int64) (r *stub.VoteAck, cancel bool, err error) {
	defer sys.Recoverable(&err)
	timeout := time.After(n.waitTimeout)
START:
	var cn csNet
	if cn, err = n.getCsNet(addr); err == nil {
		defer cn.Close()
		syncId := util.UUID64()
		if err = cn.Vote(syncId, &stub.Vote{Term: term, NodeId: n.id, RxTotal: n.bat.RxTotal.Load(), RxMax: n.bat.RxMax.Load(), MemTotal: n.mem.FsmLen()}); err == nil {
			var ack any
			if ack, cancel, err = n.dataWait.WaitWithCancel(ctx, syncId, n.waitTimeout); err == nil {
				r = ack.(*stub.VoteAck)
				return
			}
			if cancel {
				return
			}
		}
	}
	if err != nil {
		select {
		case <-timeout:
			err = rafterrors.ErrNetworkTimeout
		default:
			<-time.After(defaultRestartInterval)
			goto START
		}
	}
	return
}

func (n *Node) heartbeatsAction(leadercn bool) {
	if n.standAlone || !n.isLeader() {
		return
	}
	start := time.Now()

	for k, v := range n.peers.addrMapClone() {
		if !n.isLeader() {
			continue
		}
		go func(k string, v int64) {
			defer sys.Recoverable(nil)
			var cn csNet
			var err error
			if v != 0 && v != n.id {
				if !leadercn {
					if cn, _ = n.hbcs.Get(v); cn == nil {
						var b bool
						if cn, b, err = n.getAnSetHBCsNet(k, v); err == nil && !b && cn != nil {
							defer cn.Close()
						}
					}
					if err == nil && cn != nil && !n.peersCn.Has(v) {
						n.getAnSetPeersCnCsNet(v)
					}
				} else {
					cn, err = n.getAnSetPeersCnCsNet(v)
				}
			} else if v == 0 {
				go n.nodeInfoAction(k)
				return
			}
			if cn != nil && err == nil {
				lease, _ := n.leases.Get(v)
				if lease == 0 {
					lease = time.Millisecond.Nanoseconds()
				}
				syncId := util.UUID64()
				if cn.HeartBeat(syncId, &stub.HeartBeat{Term: n.term, LeaderId: n.id, LeaseTime: lease, Leadercn: leadercn}) == nil {
					var ack any
					if ack, err = n.dataWait.Wait(syncId, n.waitTimeout); err == nil && ack != nil {
						n.leases.Put(v, time.Now().Sub(start).Nanoseconds())
						if hba := ack.(*stub.HeartBeatAck); hba != nil && hba.Reply != HEARTBEAT_OK {
							n.becomeFollower(n.becomeId.Load())
						}
					} else {
						if cn.addNoAck() > defaultHeartbeatRetryTimes {
							cn.Close()
						}
					}
				} else {
					cn.Close()
				}
				if n.missPeer.Has(k) {
					n.missPeer.Del(k)
				}
			} else {
				n.missPeer.Put(k, v)
			}
		}(k, v)
	}
}

//func (n *Node) newPeercn(addr string, id int64) (cn csNet, b bool, err error) {
//	n.numLock.Lock(id)
//	defer n.numLock.Unlock(id)
//	if cn, _ = n.peersCn.Get(id); cn == nil {
//		if cn, err = n.getCsNet(addr); err == nil {
//			log.Debug("newPeercn>>>", cn.Id())
//			n.peersCn.Put(id, cn)
//			b = true
//		}
//	} else {
//		b = true
//	}
//	return
//}

func (n *Node) reqLogEntriesAction(reqLogEntries *stub.ReqLogEntries) *stub.ReqLogEntriesAck {
	ack := &stub.ReqLogEntriesAck{Term: n.term, NodeId: n.id}
	entries := make([]*stub.LogEntryBat, 0)
	for _, id := range reqLogEntries.RxIdList {
		if rxBat := n.getRxBat(id); rxBat != nil {
			if le := n.getLogEntryBat(rxBat.GetLogIndex()); le != nil {
				entries = append(entries, le)
			}
		}
	}
	ack.Entries = entries
	return ack
}

func (n *Node) rxSyncAction(limitPer int) {
	if n.lock.syncRxTl.TryLock() {
		defer n.lock.syncRxTl.Unlock()
		count := 0
		rxm := make([]int64, 0)
		if n.bat.RxMax.Load() > n.bat.RxExecCustor.Load() {
			for i := n.bat.RxExecCustor.Load(); i <= n.bat.RxMax.Load(); i++ {
				if !n.persistent.Has(KEY.RxId(i)) {
					rxm = append(rxm, i)
					count++
					if count >= limitPer || i == n.bat.RxMax.Load() {
						n.rxIdsSync(rxm)
						count, rxm = 0, make([]int64, 0)
					}
				}
			}
		}
	}
}

// ids
//func (n *Node) rxSyncByIdsAction(rxm []int64) (success bool) {
//	if n.lock.syncRxTl.TryLock() {
//		defer n.lock.syncRxTl.Unlock()
//		success = n.rxIdsSync(rxm)
//	}
//	return
//}

// id
func (n *Node) rxSyncByIdAction(rxId int64) bool {
	n.lock.syncRxMux.Lock()
	defer n.lock.syncRxMux.Unlock()
	return n.rxIdSync(rxId)
}
