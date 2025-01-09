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
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"sync"
	"sync/atomic"
	"time"
)

func (n *Node) Command(cmd []byte) (err error) {
	if n.isClose() {
		return rafterrors.ErrNodeNotInCluster
	}
	if n.raftxSync {
		n.lock.RaftxSyncMux.Lock()
		defer n.lock.RaftxSyncMux.Unlock()
	}
	defer sys.Recoverable(&err)
	atomic.AddInt64(&n.metrics.ProposalCount, 1)
	if n.isLeader() {
		logId := n.bat.newLogId()
		leb := &stub.LogEntryBat{Command: cmd, Term: n.term, LogIndex: logId, NodeId: n.id}
		if err = n.saveLogEntryBat(leb); err == nil {
			var rxId int64
			if n.standAlone {
				rxId = n.bat.newRxId()
			} else if err = n.appendEntriesAction(leb); err == nil {
				rxId = n.bat.newRxId()
				if err = n.commitAction(logId, rxId); err != nil {
					go n.bat.rollBack(rxId, &stub.RxBat{Term: n.term, LogIndex: 0})
				}
			}
			if err == nil {
				if n.commitMode == raft.ORDEREDPOLL {
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						if _, err = n.dataWait.Wait(logId, n.clientTimeout); err != nil {
							log.Error("Command time out:", logId)
						}
					}()
					go n.bat.commit(rxId, &stub.RxBat{Term: n.term, LogIndex: logId})
					wg.Wait()
				} else {
					err = n.bat.commit(rxId, &stub.RxBat{Term: n.term, LogIndex: logId})
				}
			}
		} else {
			log.Error("saveLogEntryBat failed:", err)
		}
	} else if n.isFollower() {
		var cn csNet
		if cn, err = n.getAndSetLeaderCsNet(); err == nil {
			syncId := util.UUID64()
			if err = cn.Proxy(syncId, &stub.Proxy{NodeId: n.id, Term: n.term, Cmd: cmd}); err == nil {
				if ack, e := n.dataWait.Wait(syncId, n.clientTimeout); e == nil {
					if ra := ack.(*stub.ProxyAck); ra != nil && !ra.Success {
						err = rafterrors.ErrProposalSyncFailed
					}
				} else {
					err = rafterrors.ErrProxyTimeOutFailed
				}
			} else {
				err = rafterrors.ErrProxyFailed
			}
		}
	} else if n.isCandidate() {
		err = rafterrors.ErrLeaderNotFound
	}
	if err != nil {
		log.Error(err)
		atomic.AddInt64(&n.metrics.ProposalFailed, 1)
	}
	return
}

func (n *Node) GetValue(key []byte) (value []byte, err error) {
	defer sys.Recoverable(&err)
	if n.isLeader() {
		return n.applymsg.Get(key)
	} else if n.isFollower() && n.leaderId != n.id {
		var cn csNet
		if cn, err = n.getAndSetLeaderCsNet(); err == nil {
			syncId := util.UUID64()
			if e := cn.ProxyRead(syncId, &stub.ReadBean{Ptype: 1, Key: key}); e != nil {
				return nil, rafterrors.ErrProxyFailed
			}
			if ack, e := n.dataWait.Wait(syncId, n.waitTimeout); e == nil {
				rb := ack.(*stub.ReadBean)
				value = rb.Value
			} else {
				err = rafterrors.ErrClusterSyncTimeout
			}
			return
		}
	}
	return nil, rafterrors.ErrLeaderNotFound
}

func (n *Node) GetValueList(key [][]byte) (result [][2][]byte, err error) {
	defer sys.Recoverable(&err)
	if n.isLeader() {
		return n.applymsg.GetList(key)
	} else if n.isFollower() && n.leaderId != n.id {
		var cn csNet
		if cn, err = n.getAndSetLeaderCsNet(); err == nil {
			syncId := util.UUID64()
			if e := cn.ProxyRead(syncId, &stub.ReadBean{Ptype: 2, Keys: key}); e != nil {
				return nil, rafterrors.ErrProxyFailed
			}
			if ack, e := n.dataWait.Wait(syncId, n.waitTimeout); e == nil {
				rb := ack.(*stub.ReadBean)
				result = make([][2][]byte, 0)
				for i := range rb.Keys {
					result = append(result, [2][]byte{rb.Keys[i], rb.Values[i]})
				}
				return
			} else {
				err = rafterrors.ErrClusterSyncTimeout
			}
			return
		}
	}
	return nil, rafterrors.ErrLeaderNotFound
}

func (n *Node) GetLocalValue(key []byte) (value []byte, err error) {
	return n.applymsg.Get(key)
}

func (n *Node) GetLocalValueList(key [][]byte) (result [][2][]byte, err error) {
	return n.applymsg.GetList(key)
}

// LastCommitTxId The transaction ID has been committed
func (n *Node) LastCommitTxId() int64 {
	return n.bat.RxExecCustor.Load()
}

// LastTransactionId last transaction ID
func (n *Node) LastTransactionId() int64 {
	return n.bat.RxMax.Load()
}

func (n *Node) GetNodeId() int64 {
	return n.id
}

func (n *Node) GetTerm() int64 {
	return n.term
}

func (n *Node) GetLeaderId() (s string, i int64) {
	i = n.leaderId
	if n.isLeader() {
		i = n.id
	}
	if i != 0 {
		s, _ = n.peers.getAddr(i)
	}
	return
}

func (n *Node) GetNodeTime() (int64, int64) {
	return n.startTime, time.Now().UnixNano()
}

func (n *Node) GetPeers() []string {
	return n.getPeers()
}

func (n *Node) GetPeerInfo() map[string]int64 {
	return n.peers.getPeerInfo()
}

func (n *Node) GetState() raft.STATE {
	return n.state
}

func (n *Node) AddNode(address string) error {
	if !n.peers.add(address) {
		return rafterrors.ErrAddExistAddress
	}
	if n.standAlone {
		return rafterrors.ErrNodeStandAlone
	}
	return n.nodeInfoAction(address)
}

func (n *Node) RemoveNode(address string) bool {
	return n.RemoveByAddr(address)
}

func (n *Node) RemoveRx(fromId, toId int64) error {
	ids := make([][]byte, 0)
	for i := fromId; i <= toId; i++ {
		ids = append(ids, util.Int64ToBytes(i))
	}
	if len(ids) > 0 {
		return n.persistent.BatchDel(ids)
	}
	return nil
}

func (n *Node) GetMetrics() *raft.Metrics {
	if n.isLeader() {
		n.metrics.LeaderUptime = time.Now().UnixMilli() - n.leaderTime
		n.metrics.FollowerLatency = make(map[int64]int64)
		for _, v := range n.peers.ids() {
			if v != n.id {
				l, _ := n.leases.Get(v)
				n.metrics.FollowerLatency[v] = l
			}
		}
	}
	n.metrics.MetricsBat = &raft.MetricsBat{
		RxExecCustor:        n.bat.RxExecCustor.Load(),
		RxMax:               n.bat.RxMax.Load(),
		RxTotal:             n.bat.RxTotal.Load(),
		LogMax:              n.bat.LogMax.Load(),
		RollbackIncrement:   n.bat.RollbackIncrement.Load(),
		RollbacCustor:       n.bat.RollbacCustor.Load(),
		ToRollbackIncrement: n.bat.ToRollbackIncrement.Load(),
		ToRollbacCustor:     n.bat.ToRollbacCustor.Load(),
		ToRollbacMax:        n.bat.ToRollbacMax.Load(),
	}

	n.metrics.MetricsMem = n.mem.GetMetricsMem()
	return &n.metrics
}

func (n *Node) Running() bool {
	return n.isRunStat()
}

func (n *Node) WaitRun() error {
	return n.waitRun()
}
