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
	"sync/atomic"
)

func (n *Node) getMemValue(ptype int32, key []byte, keys [][]byte) (r *stub.ReadBean, err error) {
	if !n.isFollower() {
		err = rafterrors.ErrLeaderNotFound
		return nil, err
	}
	var cn csNet
	if cn, err = n.getAndSetLeaderCsNet(); err == nil {
		syncId := util.UUID64()
		if e := cn.MemProxyRead(syncId, &stub.ReadBean{Ptype: ptype, Key: key, Keys: keys}); e != nil {
			return nil, rafterrors.ErrProxyFailed
		}
		if ack, e := n.dataWait.Wait(syncId, n.waitTimeout); e == nil {
			r = ack.(*stub.ReadBean)
		} else {
			err = rafterrors.ErrClusterSyncTimeout
		}
		return
	}
	return
}

func (n *Node) GetMemValue(key []byte) (value []byte, err error) {
	if n.isLeader() {
		value, _ = n.mem.GetValue(key)
	} else if n.isFollower() {
		var rb *stub.ReadBean
		if rb, err = n.getMemValue(1, key, nil); err == nil && rb != nil {
			value = rb.Value
		}
	} else {
		return nil, rafterrors.ErrLeaderNotFound
	}
	return
}

func (n *Node) GetMemValueList(keys [][]byte) (result [][2][]byte, err error) {
	if n.isLeader() {
		result = n.mem.GetValueList(keys)
	} else if n.isFollower() {
		var rb *stub.ReadBean
		if rb, err = n.getMemValue(2, nil, keys); err == nil && rb != nil {
			result = make([][2][]byte, 0)
			for i := range rb.Keys {
				result = append(result, [2][]byte{rb.Keys[i], rb.Values[i]})
			}
		}
	}
	return nil, rafterrors.ErrLeaderNotFound
}

func (n *Node) GetMemMultiValue(key []byte) (value [][]byte, err error) {
	if n.isLeader() {
		value, _ = n.mem.GetMultiValue(key)
	} else if n.isFollower() {
		var rb *stub.ReadBean
		if rb, err = n.getMemValue(3, key, nil); err == nil && rb != nil {
			value = rb.Values
		}
	} else {
		return nil, rafterrors.ErrLeaderNotFound
	}
	return
}

func (n *Node) GetMultiValueList(keys [][]byte) (result [][2][]byte, err error) {
	if n.isLeader() {
		result = n.mem.GetMultiValueList(keys)
	} else if n.isFollower() {
		var rb *stub.ReadBean
		if rb, err = n.getMemValue(4, nil, keys); err == nil && rb != nil {
			result = make([][2][]byte, 0)
			for i := range rb.Keys {
				result = append(result, [2][]byte{rb.Keys[i], rb.Values[i]})
			}
		}
	} else {
		return nil, rafterrors.ErrLeaderNotFound
	}
	return
}

func (n *Node) GetLocalMemValue(key []byte) (value []byte) {
	value, _ = n.mem.GetValue(key)
	return
}

func (n *Node) GetLocalMemValueList(key [][]byte) (result [][2][]byte) {
	return n.mem.GetValueList(key)
}

func (n *Node) GetLocalMemMultiValue(key []byte) (value [][]byte) {
	value, _ = n.mem.GetMultiValue(key)
	return
}

func (n *Node) GetLocalMemMultiValueList(key [][]byte) (result [][2][]byte) {
	return n.mem.GetMultiValueList(key)
}

func (n *Node) MemCommand(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error) {
	if n.isClose() {
		return rafterrors.ErrNodeNotInCluster
	}
	defer sys.Recoverable(&err)
	atomic.AddInt64(&n.metrics.MemProposalCount, 1)
	if err = n.mem.Command(key, value, ttl, ptype); err != nil {
		log.Error(err)
		atomic.AddInt64(&n.metrics.MemProposalFailed, 1)
	}
	return
}

// MemLen returns the number of active MemBean items currently stored in fsm
func (n *Node) MemLen() int64 {
	return n.mem.FsmLen()
}

func (n *Node) Watch(key []byte, watchTypes []raft.WatchType, isSync bool, watchFunc func(key, value []byte, watchType raft.WatchType)) {
	n.mem.Watch(key, watchTypes, isSync, watchFunc)
}

func (n *Node) UnWatch(key []byte) {
	n.mem.UnWatch(key)
}

func (n *Node) UnWatchWithType(key []byte, wt raft.WatchType) {
	n.mem.UnWatchWithType(key, wt)
}
