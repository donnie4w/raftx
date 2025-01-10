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
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/stub"
)

type processor struct {
	localNode *Node
	tran      csNet
	isServ    bool
}

func (p *processor) Id() int64 {
	return p.tran.Id()
}

func (p *processor) IsValid() bool {
	return p.tran.IsValid()
}

func newProcessor(n *Node, c csNet, serv bool) csNet {
	return &processor{localNode: n, tran: c, isServ: serv}
}

func (p *processor) Chap(syncId int64, chap *stub.Chap) (err error) {
	log.Debug("chap:", chap)
	return
}

func (p *processor) ChapAck(syncId int64, chap *stub.Chap) (err error) {
	log.Debug("chap:", chap)
	return
}

func (p *processor) Auth(syncId int64, auth *stub.Auth) (err error) {
	p.tran.AuthAck(syncId, &stub.Auth{NodeId: p.localNode.id})
	return
}

func (p *processor) AuthAck(syncId int64, auth *stub.Auth) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, auth)
	return
}

func (p *processor) Vote(syncId int64, vote *stub.Vote) (err error) {
	Granted := GRANTED_OK
	var voted *stub.Vote
	b1 := vote.GetRxTotal() - p.localNode.bat.RxTotal.Load()
	b2 := vote.GetRxMax() - p.localNode.bat.RxMax.Load()
	b3 := vote.GetMemTotal() - p.localNode.MemLen()

	if !p.localNode.isBlock() && (b1 < 0 || (b1 == 0 && b3 < 0) || (b1 == 0 && b3 == 0 && b2 < 0)) {
		Granted = GRANTED_DATA_OLD
	} else if p.localNode.term > vote.Term {
		Granted = GRANTED_TERM_OLD
	} else if p.localNode.term <= vote.Term {
		if p.localNode.isFollower() && p.localNode.leaderId != 0 {
			if p.localNode.detectLeader() {
				Granted = GRANTED_LEADER_ALIVE
			}
		}
	}
	p.localNode.lock.VoteSyncMux.Lock()
	if Granted == GRANTED_OK {
		if vv := p.localNode.votedFor.Vote; vv != nil && vv.NodeId != vote.NodeId {
			Granted = GRANTED_VOTEOTHER
			voted = vv
		}
	}
	if Granted == GRANTED_OK {
		p.localNode.votedFor.VoteId = vote.NodeId
		p.localNode.votedFor.Vote = vote
	}
	p.localNode.lock.VoteSyncMux.Unlock()
	rxTotal := int64(0)
	if !p.localNode.isBlock() {
		rxTotal = p.localNode.bat.RxTotal.Load()
	}
	p.tran.VoteAck(syncId, &stub.VoteAck{Granted: Granted, NodeId: p.localNode.id, RxMax: p.localNode.bat.RxMax.Load(), RxTotal: rxTotal, LogMax: p.localNode.bat.LogMax.Load(), VoteFor: voted, MemMaxId: p.localNode.getMemMaxId()})
	return
}

func (p *processor) VoteAck(syncId int64, voteAck *stub.VoteAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, voteAck)
	return
}

func (p *processor) Commit(syncId int64, commit *stub.Commit) (err error) {
	success := false
	if commit.Term >= p.localNode.term && commit.LeaderId == p.localNode.leaderId {
		if p.localNode.bat.commit(commit.RxId, &stub.RxBat{LogIndex: commit.LogIndex, Term: commit.Term}) == nil {
			success = true
		}
	}
	p.tran.CommitAck(syncId, &stub.CommitAck{Success: success})
	return
}

func (p *processor) CommitAck(syncId int64, cmmitAck *stub.CommitAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, cmmitAck)
	return
}

func (p *processor) RollBack(syncId int64, rollback *stub.RollBack) (err error) {
	var success bool
	if p.localNode.rollbackExec(rollback.GetRxId()) == nil {
		success = true
	}
	p.tran.RollBackAck(syncId, &stub.RollBackAck{Success: success, NodeId: p.localNode.id, Term: p.localNode.term})
	return
}

func (p *processor) RollBackAck(syncId int64, rollbackAck *stub.RollBackAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, rollbackAck)
	return
}

func (p *processor) AppendEntries(syncId int64, appendEntries *stub.AppendEntries) (err error) {
	success := false
	if appendEntries.Term >= p.localNode.term && appendEntries.NodeId == p.localNode.leaderId && p.localNode.isFollower() {
		p.localNode.updateHeartbeatTime()
		if p.localNode.saveLogEntryBat(appendEntries.Enstries) == nil {
			success = true
		}
	}
	p.tran.AppendEntriesAck(syncId, &stub.AppendEntriesAck{Success: success, Term: p.localNode.term})
	return
}

func (p *processor) AppendEntriesAck(syncId int64, appendEntriesAck *stub.AppendEntriesAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, appendEntriesAck)
	return
}

func (p *processor) ReqLogEntries(syncId int64, reqLogEntries *stub.ReqLogEntries) (err error) {
	p.tran.ReqLogEntriesAck(syncId, p.localNode.reqLogEntriesAction(reqLogEntries))
	return
}

func (p *processor) ReqLogEntriesAck(syncId int64, reqLogEntriesAck *stub.ReqLogEntriesAck) (err error) {
	if syncId != 0 {
		p.localNode.dataWait.CloseAndPut(syncId, reqLogEntriesAck)
	}
	return
}

func (p *processor) HeartBeat(syncId int64, heartBeat *stub.HeartBeat) (err error) {
	p.localNode.lock.HeartBeatMux.Lock()
	defer p.localNode.lock.HeartBeatMux.Unlock()
	reply := HEARTBEAT_OK
	if heartBeat.Term < p.localNode.term {
		reply = HEARTBEAT_TERM_OLD
	} else if heartBeat.Term > p.localNode.term {
		p.localNode.setTerm(heartBeat.Term)
		if !p.localNode.isFollower() {
			p.localNode.becomeFollower(p.localNode.becomeId.Load())
		}
	} else {
		if p.localNode.leaderId != 0 && p.localNode.leaderId != heartBeat.LeaderId {
			reply = HEARTBEAT_PARTITION
		}
	}
	if reply == HEARTBEAT_OK {
		p.localNode.updateHeartbeatTime()
		p.localNode.heartbeatLease = heartBeat.LeaseTime
		p.localNode.leaderId = heartBeat.LeaderId
		if p.localNode.term < heartBeat.Term {
			p.localNode.setTerm(heartBeat.Term)
		}
		if p.localNode.votedFor.Vote != nil {
			p.localNode.votedFor = newVoteFor()
		}
	}
	p.tran.HeartBeatAck(syncId, &stub.HeartBeatAck{Reply: reply, Term: p.localNode.term})
	return
}

func (p *processor) HeartBeatAck(syncId int64, heartBeatAck *stub.HeartBeatAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, heartBeatAck)
	return
}

func (p *processor) Ping(syncId int64, ping *stub.Ping) (err error) {
	switch ping.Detect {
	case 1:
		if p.localNode.isLeader() && ping.Term == p.localNode.term {
			p.tran.Pong(syncId, &stub.Pong{Result: 1})
		}
	}
	return
}

func (p *processor) Pong(syncId int64, pong *stub.Pong) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, pong)
	return
}

func (p *processor) Proxy(syncId int64, proxy *stub.Proxy) (err error) {
	success := false
	if p.localNode.isLeader() && proxy.Term <= p.localNode.term {
		if e := p.localNode.Command(proxy.Cmd); e == nil {
			success = true
		}
	}
	if err = p.tran.ProxyAck(syncId, &stub.ProxyAck{Success: success}); err != nil {
		log.Errorf("ProxyAck err:%v", err)
	}
	return
}

func (p *processor) ProxyAck(syncId int64, ack *stub.ProxyAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, ack)
	return
}

func (p *processor) Close() (err error) {
	return
}

func (p *processor) NodeInfo(syncId int64, ni *stub.NodeInfo) (err error) {
	p.localNode.lock.NodeInfoMux.Lock()
	defer p.localNode.lock.NodeInfoMux.Unlock()
	switch ni.Rtype {
	case NODEINFO_SYNCID:
		for _, addr := range ni.Peers {
			if !p.localNode.peers.hasAddr(addr) {
				p.localNode.peers.add(addr)
				go func() {
					p.localNode.nodeInfoAction(addr)
					if p.localNode.isFollower() && p.localNode.leaderCn != nil {
						p.localNode.nodeInfoNotifyAction(p.localNode.leaderCn)
					} else if p.localNode.isLeader() {
						p.localNode.nodeInfoNotifyAllAction()
					}
				}()
			}
		}
	case NODEINFO_ADDPEER:
	case NODEINFO_DELPEER:
	case NODEINFO_NOTIFY:
		for _, addr := range ni.Peers {
			if !p.localNode.peers.hasAddr(addr) {
				p.localNode.peers.add(addr)
				go func() {
					p.localNode.nodeInfoAction(addr)
					if p.localNode.isLeader() {
						p.localNode.nodeInfoNotifyAllAction()
					}
				}()
			}
		}
	}
	p.tran.NodeInfoAck(syncId, &stub.NodeInfo{NodeId: p.localNode.id})
	return
}

func (p *processor) NodeInfoAck(syncId int64, ni *stub.NodeInfo) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, ni.NodeId)
	return
}

func (p *processor) ProxyRead(syncId int64, rbean *stub.ReadBean) (err error) {
	if p.localNode.isLeader() {
		if rbean.GetPtype() == 2 { //list
			if rs, err := p.localNode.GetValueList(rbean.Keys); err == nil {
				rbean.Keys = make([][]byte, 0)
				rbean.Values = make([][]byte, 0)
				for _, vs := range rs {
					rbean.Keys = append(rbean.Keys, vs[0])
					rbean.Values = append(rbean.Values, vs[1])
				}
			} else {
				rbean.ErrCode = SYNC_ERROR
			}
		} else if rbean.GetPtype() == 1 { //single
			if rs, err := p.localNode.GetValue(rbean.Key); err == nil {
				rbean.Value = rs
			} else {
				rbean.ErrCode = SYNC_ERROR
			}
		}
	} else {
		rbean.ErrCode = SYNC_NOT_LEADER
	}
	p.tran.ProxyReadAck(syncId, rbean)
	return
}

func (p *processor) ProxyReadAck(syncId int64, rbean *stub.ReadBean) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, rbean)
	return
}

func (p *processor) addNoAck() int32 {
	return 0
}

func (p *processor) RxSync(syncId int64, rxEntries *stub.RxEntries) (err error) {
	var em map[int64]*stub.SyncBean
	var sb *stub.SyncBean
	if rxEntries.GetRxId() > 0 {
		sb = p.localNode.getSyncBean(rxEntries.GetRxId())
	}
	if len(rxEntries.GetEnstries()) > 0 {
		em = p.localNode.getSyncBeanMap(rxEntries.GetEnstries())
	}
	log.Debug("RxSync:", rxEntries, p.localNode.listenAddr, ",ack:", sb)
	p.tran.RxSyncAck(syncId, &stub.RxEntriesAck{EntryMap: em, Bean: sb})
	return
}

func (p *processor) RxSyncAck(syncId int64, rxEntriesAck *stub.RxEntriesAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, rxEntriesAck)
	return
}

func (p *processor) MemApply(syncId int64, memApply *stub.MemApply) (err error) {
	success := false
	if memApply.GetTerm() >= p.localNode.GetTerm() {
		p.localNode.mem.Apply(memApply.GetMb())
		success = true
	}
	p.tran.MemApplyAck(syncId, &stub.MemApplyAck{Success: success})
	return
}

func (p *processor) MemApplyAck(syncId int64, memApplyAck *stub.MemApplyAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, memApplyAck)
	return
}

func (p *processor) MemSync(syncId int64, memSync *stub.MemSync) (err error) {
	mb := p.localNode.mem.GetLogEntry(memSync.GetMemId())
	log.Debug("MemSync:", p.localNode.listenAddr, ",", mb)
	if mb == nil && p.localNode.isLeader() {
		mb = &stub.MemBean{MemId: memSync.GetMemId()}
	}
	p.tran.MemSyncAck(syncId, &stub.MemSyncAck{Mb: mb})
	return
}

func (p *processor) MemSyncAck(syncId int64, memSyncAck *stub.MemSyncAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, memSyncAck)
	return
}

func (p *processor) MemProxy(syncId int64, memProxy *stub.MemProxy) (err error) {
	success := false
	if err := p.localNode.mem.Command(memProxy.GetKey(), memProxy.GetValue(), memProxy.GetTtl(), raft.MTYPE(memProxy.GetPType())); err == nil {
		success = true
	}
	p.tran.MemProxyAck(syncId, &stub.MemProxyAck{Success: success})
	return
}

func (p *processor) MemProxyAck(syncId int64, memProxyAck *stub.MemProxyAck) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, memProxyAck)
	return
}

func (p *processor) MemProxyRead(syncId int64, rbean *stub.ReadBean) (err error) {
	if p.localNode.isLeader() {
		switch rbean.GetPtype() {
		case 1:
			if rs, ok := p.localNode.mem.GetValue(rbean.Key); ok {
				rbean.Value = rs
			} else {
				log.Error(err)
				rbean.ErrCode = SYNC_ERROR
			}
		case 2:
			if rs := p.localNode.mem.GetValueList(rbean.Keys); rs != nil {
				rbean.Keys = make([][]byte, 0)
				rbean.Values = make([][]byte, 0)
				for _, vs := range rs {
					rbean.Keys = append(rbean.Keys, vs[0])
					rbean.Values = append(rbean.Values, vs[1])
				}
			} else {
				log.Error(err)
				rbean.ErrCode = SYNC_ERROR
			}
		case 3:
			if rs, ok := p.localNode.mem.GetMultiValue(rbean.Key); ok && rs != nil {
				rbean.Values = rs
			} else {
				log.Error(err)
				rbean.ErrCode = SYNC_ERROR
			}
		case 4:
			if rs := p.localNode.mem.GetMultiValueList(rbean.Keys); rs != nil {
				rbean.Keys = make([][]byte, 0)
				rbean.Values = make([][]byte, 0)
				for _, vs := range rs {
					rbean.Keys = append(rbean.Keys, vs[0])
					rbean.Values = append(rbean.Values, vs[1])
				}
			} else {
				log.Error(err)
				rbean.ErrCode = SYNC_ERROR
			}
		}
	} else {
		rbean.ErrCode = SYNC_NOT_LEADER
	}
	p.tran.MemProxyReadAck(syncId, rbean)
	return
}

func (p *processor) MemProxyReadAck(syncId int64, readBean *stub.ReadBean) (err error) {
	p.localNode.dataWait.CloseAndPut(syncId, readBean)
	return
}
