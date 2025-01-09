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
)

func (n *Node) memApplyAction(mb *stub.MemBean) error {
	memApply := &stub.MemApply{Term: n.term, NodeId: n.id, Mb: mb}
	req := func(syncId int64, cn csNet) error {
		return cn.MemApply(syncId, memApply)
	}
	ack := func(r any) bool {
		if r != nil {
			return r.(*stub.MemApplyAck).Success
		}
		return false
	}
	if b, err := n.proxyCs(req, ack); err != nil {
		return err
	} else if !b {
		log.Error("Failed to call memApplyAction memKey:", mb.GetKey())
		return rafterrors.ErrProposalSyncFailed
	}
	return nil
}

func (n *Node) memSyncAction(memId int64) (err error, success bool) {
	log.Debug("memSyncAction:", n.listenAddr, ",memId:", memId)
	if n.isFollower() {
		var cn csNet
		if cn, err = n.getAndSetLeaderCsNet(); err == nil {
			syncId := util.UUID64()
			if err = cn.MemSync(syncId, &stub.MemSync{MemId: memId}); err == nil {
				var ack any
				if ack, err = n.dataWait.Wait(syncId, n.waitTimeout); err == nil {
					mb := ack.(*stub.MemSyncAck).GetMb()
					n.mem.Apply(mb)
					success = true
				}
			}
		}
	} else if n.isLeader() {
		if !n.standAlone {
			for k, v := range n.peers.addrMapClone() {
				if v != 0 && v != n.id {
					cn, _ := n.peersCn.Get(v)
					if cn == nil {
						if cn, err = n.getCsNet(k); err == nil {
							defer cn.Close()
						}
					}
					if cn == nil || err != nil {
						continue
					}
					syncId := util.UUID64()
					if err = cn.MemSync(syncId, &stub.MemSync{MemId: memId}); err == nil {
						var ack any
						if ack, err = n.dataWait.Wait(syncId, n.waitTimeout); err == nil {
							n.mem.Apply(ack.(*stub.MemSyncAck).GetMb())
							log.Debug("memSyncAction success:", ack.(*stub.MemSyncAck).GetMb())
							success = true
							break
						}
					}
				}
			}
		}
		if !success {
			n.mem.Apply(&stub.MemBean{MemId: memId})
			success = true
		}
	}
	return
}

func (n *Node) memProxyAction(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error) {
	var cn csNet
	if cn, err = n.getAndSetLeaderCsNet(); err == nil {
		syncId := util.UUID64()
		if err = cn.MemProxy(syncId, &stub.MemProxy{Key: key, Value: value, Ttl: ttl, PType: int32(ptype)}); err == nil {
			var ack any
			if ack, err = n.dataWait.Wait(syncId, n.waitTimeout); err != nil {
				if !ack.(*stub.MemProxyAck).Success {
					return rafterrors.ErrProxyFailed
				}
			}
		}
	}
	return
}
