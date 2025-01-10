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
	"github.com/donnie4w/gofer/hashmap"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"time"
)

func (n *Node) syncRollBack(addr string, nodeId, rxid int64) (r bool) {
	var cn csNet
	var err error
	if nodeId != 0 {
		if n.isLeader() {
			cn, _ = n.peersCn.Get(nodeId)
		}
		if cn == nil {
			if cn, err = n.getCsNetById(nodeId); err == nil {
				defer cn.Close()
			}
		}
	} else if addr != "" {
		if cn, err = n.getCsNet(addr); err == nil {
			defer cn.Close()
		}
	}
	if cn == nil || err != nil {
		return false
	}
	syncId := util.UUID64()
	cn.RollBack(syncId, &stub.RollBack{NodeId: n.id, Term: n.term, RxId: rxid})
	if ack, err := n.dataWait.Wait(syncId, n.waitTimeout); err == nil {
		if rback := ack.(*stub.RollBackAck); rback.Success {
			r = true
		}
	}
	return
}

func (n *Node) rollback() {
	defer sys.Recoverable(nil)
	if n.lock.RollBackTl.TryLock() {
		defer n.lock.RollBackTl.Unlock()
		if n.bat.RollbackIncrement.Load() > n.bat.RollbacCustor.Load() {
			for i := n.bat.RollbacCustor.Load() + 1; i <= n.bat.RollbackIncrement.Load(); i++ {
				if bs, err := n.persistent.Get(KEY.RollbackId(i)); err == nil {
					rxid := util.BytesToInt64(bs)
					failedNum := 0
					sendNum := 0
					for k, v := range n.peers.addrMapClone() {
						if v == 0 {
							n.nodeInfoAction(k)
							v, _ = n.peers.getId(k)
						}
						if v != 0 && v != n.id {
							sendNum++
							if !n.syncRollBack(k, v, rxid) {
								failedNum++
								n.saveToRollback(v, rxid, k)
							}
						}
					}
					n.removeRollbackId(i)
					if failedNum == sendNum {
						break
					}
				}
			}
		}
	}
}

func (n *Node) removeRollbackId(id int64) {
	if n.persistent.Del(KEY.RollbackId(id)) == nil {
		if n.persistent.Put(KEYSYS.RollbackCursor(), util.Int64ToBytes(id)) == nil {
			n.bat.RollbacCustor.Store(id)
		}
	}
}

func (n *Node) rollbackExec(rxid int64) (err error) {
	if err = n.applymsg.Rollback(rxid); err == nil {
		if rb := n.getRxBat(rxid); rb != nil {
			if rb.GetLogIndex() != 0 {
				if err = n.logStorage.Del(rb.GetLogIndex()); err == nil {
					err = n.saveRxBat(rxid, &stub.RxBat{LogIndex: 0, Term: rb.GetTerm()})
				}
			} else {
				err = n.saveRxBat(rxid, &stub.RxBat{LogIndex: 0, Term: rb.GetTerm()})
			}
		}
	}
	return
}

func (n *Node) toRollback() {
	if n.lock.ToRollBackTl.TryLock() {
		defer n.lock.ToRollBackTl.Unlock()
		if n.bat.ToRollbackIncrement.Load() >= n.bat.ToRollbacCustor.Load() {
			trId := n.bat.ToRollbacCustor.Load()
			for trId != 0 {
				if trb := n.getToRollack(trId); trb != nil {
					log.Debug("toRollback >>>>", trId)
					if n.syncRollBack(trb.GetAddr(), trb.GetNodeId(), trb.GetRxId()) {
						n.removeToRollbackId(trb)
						log.Debug("toRollback finish>>>>", trId)
					}
					trId = trb.GetNext()
				} else {
					break
				}
			}
		}
	}
}

func (n *Node) rxIdsSync(rxids []int64) {
	log.Debug("rxIdsSync>>>", rxids, ",", n.listenAddr)
	docn := func(bm *hashmap.MapL[int64, *stub.SyncBean], cn csNet) {
		syncId := util.UUID64()
		cn.RxSync(syncId, &stub.RxEntries{Enstries: rxids})
		if ack, er := n.dataWait.Wait(syncId, n.waitTimeout); er == nil {
			if ra := ack.(*stub.RxEntriesAck); ra != nil {
				for k, v := range ra.GetEntryMap() {
					if v.GetAckType() == SYNCTYPE_EXIST || !bm.Has(k) {
						bm.Put(k, v)
					}
				}
			}
		}
	}
	bm := hashmap.NewMapL[int64, *stub.SyncBean]()
	tot := time.After(n.waitTimeout)
	if n.isFollower() && n.leaderId != 0 {
		if cn, err := n.getAndSetLeaderCsNet(); err == nil && cn != nil {
			docn(bm, cn)
		}
	} else if n.isLeader() {
		ch := make(chan struct{}, n.peers.len()-1)
		defer close(ch)
		for k, v := range n.peers.addrMapClone() {
			go func(k string, v int64) {
				defer sys.Recoverable(nil)
				defer func() { ch <- struct{}{} }()
				if v != 0 && v != n.id {
					if cn, err := n.getAnSetPeersCnCsNet(v); err == nil {
						docn(bm, cn)
					}
				}
			}(k, v)
		}
		for {
			select {
			case <-ch:
				if bm.Len() == int64(len(rxids)) {
					goto END
				}
			case <-tot:
				goto END
			}
		}
	}
END:
	for _, rxId := range rxids {
		if sb, b := bm.Get(rxId); b {
			switch sb.GetAckType() {
			case SYNCTYPE_EXIST:
				if le := sb.GetLeb(); le != nil && le.GetLogIndex() != 0 {
					if n.saveLogEntryBat(le) == nil {
						n.bat.commit(rxId, &stub.RxBat{Term: n.term, LogIndex: le.GetLogIndex()})
					}
				} else if mc := sb.GetMc(); mc != nil {
					n.bat.commit(rxId, &stub.RxBat{Term: n.term, Mc: mc})
				}
			case SYNCTYPE_EMPTY:
				n.bat.commit(rxId, &stub.RxBat{Term: n.term, LogIndex: 0})
			case SYNCTYPE_MISS:
				if n.isLeader() {
					n.bat.commit(rxId, &stub.RxBat{Term: n.term, LogIndex: 0})
				}
			}
		}
	}
	return
}

func (n *Node) rxIdSync(rxid int64) (success bool) {
	log.Debug("rxIdSync>>>", rxid, ",", n.listenAddr)
	docn := func(cn csNet) *stub.SyncBean {
		syncId := util.UUID64()
		if cn.RxSync(syncId, &stub.RxEntries{RxId: rxid}) == nil {
			if ack, err := n.dataWait.Wait(syncId, n.waitTimeout); err == nil {
				if ra := ack.(*stub.RxEntriesAck); ra != nil {
					return ra.GetBean()
				}
			}
		}
		return nil
	}
	var sb *stub.SyncBean
	if n.isFollower() && n.leaderId != 0 {
		if cn, err := n.getAndSetLeaderCsNet(); err == nil && cn != nil {
			sb = docn(cn)
		}
	} else if n.isLeader() {
		if !n.standAlone {
			for _, v := range n.peers.addrMapClone() {
				if v != 0 && v != n.id {
					if cn, err := n.getAnSetPeersCnCsNet(v); err == nil {
						if sb = docn(cn); sb != nil {
							break
						}
					}
				}
			}
		} else {
			n.bat.commit(rxid, &stub.RxBat{Term: n.term, LogIndex: 0})
			return
		}
	}
	if sb != nil {
		log.Debug("get syncBean:", sb)
		switch sb.GetAckType() {
		case SYNCTYPE_EXIST:
			if le := sb.GetLeb(); le != nil && le.GetLogIndex() != 0 {
				if n.saveLogEntryBat(le) == nil {
					n.bat.commit(rxid, &stub.RxBat{Term: n.term, LogIndex: le.GetLogIndex()})
				}
			} else if mc := sb.GetMc(); mc != nil {
				n.bat.commit(rxid, &stub.RxBat{Term: n.term, Mc: mc})
			}
		case SYNCTYPE_EMPTY:
			n.bat.commit(rxid, &stub.RxBat{Term: n.term, LogIndex: 0})
		case SYNCTYPE_MISS:
			if n.isLeader() {
				n.bat.commit(rxid, &stub.RxBat{Term: n.term, LogIndex: 0})
			}
		}
		success = true
	}
	return
}
