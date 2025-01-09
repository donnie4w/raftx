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
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
)

func (n *Node) getPeers() []string {
	return n.peers.addrs()
}

func getUUID() int64 {
	u := util.UUID64()
	if u < 0 {
		return -u
	}
	return u
}

func (n *Node) getCsNet(addr string) (csNet, error) {
	c := newConnect(n)
	return c, c.open(addr)
}

func (n *Node) getCsNetById(id int64) (r csNet, err error) {
	if id == 0 {
		return nil, rafterrors.ErrInvalidRequestFormat
	}
	if addr, ok := n.peers.getAddr(id); ok {
		conn := newConnect(n)
		r, err = conn, conn.open(addr)
	}
	if err != nil {
		return nil, err
	} else if r == nil {
		return nil, rafterrors.ErrConnectionFailed
	}
	return
}

func (n *Node) getAndSetLeaderCsNet() (r csNet, err error) {
	if r = n.leaderCn; r != nil {
		return
	} else if n.leaderId != 0 {
		n.numLock.Lock(n.leaderId)
		defer n.numLock.Unlock(n.leaderId)
		if n.leaderCn == nil {
			if n.leaderCn, err = n.getCsNetById(n.leaderId); err == nil {
				return n.leaderCn, nil
			}
		} else {
			return n.leaderCn, nil
		}
	}
	return nil, rafterrors.ErrLeaderNotFound
}

func (n *Node) getAnSetHBCsNet(addr string, id int64) (cn csNet, b bool, err error) {
	n.numLock.Lock(id)
	defer n.numLock.Unlock(id)
	if cn, _ = n.hbcs.Get(id); cn == nil {
		if cn, err = n.getCsNet(addr); err == nil {
			n.hbcs.Put(id, cn)
			b = true
		}
	} else {
		b = true
	}
	return
}

func (n *Node) getAnSetPeersCnCsNet(id int64) (r csNet, err error) {
	if id == 0 || !n.isLeader() || id == n.id {
		return nil, rafterrors.ErrInvalidRequestFormat
	}
	if cn, ok := n.peersCn.Get(id); ok {
		return cn, nil
	} else {
		n.numLock.Lock(n.id)
		defer n.numLock.Unlock(n.id)
		if cn, ok := n.peersCn.Get(id); ok {
			return cn, nil
		} else {
			if r, err = n.getCsNetById(id); err == nil {
				n.peersCn.Put(id, r)
				return
			}
		}
	}
	return nil, rafterrors.ErrConnectionFailed
}

func (n *Node) getLogEntryBat(logId int64) *stub.LogEntryBat {
	if logId == 0 {
		return nil
	}
	if v, ok := n.logEntryBatPool.Get(logId); ok {
		return v
	}
	if bs, _ := n.logStorage.Get(logId); len(bs) > 0 {
		leb := &stub.LogEntryBat{}
		if stub.Unmarshal(bs, leb) != nil {
			return nil
		}
		return leb
	}
	return nil
}

func (n *Node) getSyncBean(rxid int64) *stub.SyncBean {
	if rb := n.getRxBat(rxid); rb != nil {
		if rb.GetLogIndex() != 0 {
			if le := n.getLogEntryBat(rb.GetLogIndex()); le != nil {
				return &stub.SyncBean{AckType: SYNCTYPE_EXIST, Leb: le}
			}
		} else if rb.GetMc() != nil {
			return &stub.SyncBean{AckType: SYNCTYPE_EXIST, Mc: rb.GetMc()}
		} else {
			return &stub.SyncBean{AckType: SYNCTYPE_EMPTY}
		}
	} else {
		return &stub.SyncBean{AckType: SYNCTYPE_MISS}
	}
	return nil
}

func (n *Node) getSyncBeanMap(rxIds []int64) (r map[int64]*stub.SyncBean) {
	r = make(map[int64]*stub.SyncBean)
	for _, rxid := range rxIds {
		if rb := n.getRxBat(rxid); rb != nil {
			if rb.GetLogIndex() != 0 {
				if le := n.getLogEntryBat(rb.GetLogIndex()); le != nil {
					r[rxid] = &stub.SyncBean{AckType: SYNCTYPE_EXIST, Leb: le}
				}
			} else if rb.GetMc() != nil {
				r[rxid] = &stub.SyncBean{AckType: SYNCTYPE_EXIST, Mc: rb.GetMc()}
			} else {
				r[rxid] = &stub.SyncBean{AckType: SYNCTYPE_EMPTY}
			}
		} else {
			r[rxid] = &stub.SyncBean{AckType: SYNCTYPE_MISS}
		}
	}
	return
}

func (n *Node) getRxBat(rxid int64) *stub.RxBat {
	if v, ok := n.rxBatPool.Get(rxid); ok {
		return v
	}
	if bs, _ := n.persistent.Get(KEY.RxId(rxid)); len(bs) > 0 {
		rxBat := &stub.RxBat{}
		if stub.Unmarshal(bs, rxBat) != nil {
			return nil
		}
		return rxBat
	}
	return nil
}

func (n *Node) getToRollack(toRollackId int64) *stub.ToRollack {
	if toRollackId == 0 {
		return nil
	}
	if bs, _ := n.persistent.Get(KEY.ToRollbackId(toRollackId)); len(bs) > 0 {
		torollack := &stub.ToRollack{}
		if stub.Unmarshal(bs, torollack) != nil {
			return nil
		}
		return torollack
	}
	return nil
}

func (n *Node) getMemMaxId() int64 {
	return n.mem.GetMaxId()
}
