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
	"github.com/donnie4w/gofer/hashmap"
	"github.com/donnie4w/gofer/lock"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"sync"
	"sync/atomic"
	"time"
)

// simpleMem represents a volatile data storage structure designed for use in distributed systems, primarily as an in-memory cache.
// It includes various synchronization mechanisms to ensure data consistency in concurrent environments and provides functionalities like command execution, data retrieval, and expired data cleanup.
type simpleMem struct {
	mux sync.Mutex

	// Tracks the last unexecuted operation ID and its timestamp to detect long-unexecuted operations
	noExecs [3]int64

	// Atomic variable indicating the last successfully executed operation ID
	cursor atomic.Int64

	// Atomic variable indicating the current maximum MemBean ID assigned
	maxId atomic.Int64

	// Mutex protecting updates to maxId
	idMux sync.Mutex

	// stores MemBean objects awaiting execution, keyed by MemId
	execm *hashmap.Map[int64, *stub.MemBean]

	// fsm is a hashmap that maintains recently used MemBean objects in insertion order, keyed by hashed Key
	fsm *hashmap.MapL[uint64, *stub.MemBean]

	// logm is a capacity-limited hash map storing log entries, keyed by MemId
	logm *hashmap.LimitHashMap[int64, *stub.MemBean]

	ep *expiredPool

	node *Node

	watchmap *hashmap.Map[uint64, *hashmap.Map[raft.WatchType, *watchBean]]

	watchlock *lock.Numlock

	ctx context.Context

	cancel context.CancelFunc
}

// newMem initializes a new instance of simpleMem and starts background goroutines for expiring data and checking command execution.
func newMem(node *Node) mem {
	ctx, cancel := context.WithCancel(context.Background())
	m := &simpleMem{
		node:      node,
		execm:     hashmap.NewMap[int64, *stub.MemBean](),
		fsm:       hashmap.NewMapL[uint64, *stub.MemBean](),
		logm:      hashmap.NewLimitHashMap[int64, *stub.MemBean](int(node.memLogEntryLimit)),
		watchmap:  hashmap.NewMap[uint64, *hashmap.Map[raft.WatchType, *watchBean]](),
		watchlock: lock.NewNumLock(1 << 7),
		ctx:       ctx,
		cancel:    cancel,
		ep:        newExpiredPool(ctx),
	}
	go m.expireTk() // Starts the timer for expiring data
	go m.execTK()   // Starts the timer for checking command execution
	return m
}

// FsmLen returns the number of active MemBean items currently stored in fsm.
func (sm *simpleMem) FsmLen() int64 {
	return sm.fsm.Len()
}

// newMemId allocates a new unique MemId.
func (sm *simpleMem) newMemId() int64 {
	return sm.maxId.Add(1)
}

// Command receives a command and decides how to process it based on whether the node is a Leader or Follower.
// If the node is a Leader, it applies the command; if a Follower, it forwards the command to the Leader.
func (sm *simpleMem) Command(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error) {
	if sm.node.memSync {
		sm.node.lock.MemSyncMux.Lock()
		defer sm.node.lock.MemSyncMux.Unlock()
	}
	if sm.node.isLeader() {
		var et uint64
		if ptype == raft.MEM_PUT || ptype == raft.MEM_APPEND {
			if ttl == 0 {
				ttl = uint64(sm.node.memExpiredTime)
			}
			et = uint64(time.Now().UnixNano()) + ttl*1e9
		}
		mb := &stub.MemBean{
			MemId:       sm.newMemId(),
			Key:         util.FNVHash64(key),
			Value:       value,
			PType:       int32(ptype),
			ExpiredTime: et,
		}
		if sm.node.standAlone {
			sm.Apply(mb)
		} else if err = sm.node.memApplyAction(mb); err == nil {
			sm.Apply(mb)
		}
	} else if sm.node.isFollower() {
		err = sm.node.memProxyAction(key, value, ttl, ptype)
	} else {
		err = rafterrors.ErrLeaderNotFound
	}
	return
}

// Apply applies a new MemBean to the storage system and initializes the cursor and maximum ID.
func (sm *simpleMem) Apply(mb *stub.MemBean) {
	sm.logm.Put(mb.GetMemId(), mb)
	sm.execm.Put(mb.GetMemId(), mb)
	sm.initCursor(mb.GetMemId())
	sm.SetMaxId(mb.GetMemId())
	go sm.exec()
}

// initCursor initializes the cursor position, ensuring it does not exceed the known maximum ID.
func (sm *simpleMem) initCursor(id int64) {
	if sm.cursor.Load() == 0 && id > 1 {
		sm.idMux.Lock()
		defer sm.idMux.Unlock()
		if sm.cursor.Load() == 0 {
			sm.cursor.Store(id - 1)
		}
	}
}

// SetMaxId updates the maximum ID value, ensuring it remains globally unique.
func (sm *simpleMem) SetMaxId(id int64) {
	current := sm.maxId.Load()
	if current >= id {
		return
	}
	for i := 0; i < 15; i++ {
		if sm.maxId.CompareAndSwap(current, id) {
			return
		}
		current = sm.maxId.Load()
	}
	sm.idMux.Lock()
	defer sm.idMux.Unlock()
	if sm.maxId.Load() < id {
		sm.maxId.Store(id)
	}
}

func (sm *simpleMem) GetMaxId() int64 {
	return sm.maxId.Load()
}

func (sm *simpleMem) GetMetricsMem() *raft.MetricsMem {
	return &raft.MetricsMem{
		Cursor:    sm.cursor.Load(),
		MaxId:     sm.maxId.Load(),
		FsmLength: sm.fsm.Len(),
	}
}

// exec executes all pending operations and moves them from execm to fsm.
func (sm *simpleMem) exec() (err error) {
	defer sys.Recoverable(&err)
	if !sm.mux.TryLock() {
		return nil
	}
	defer sm.mux.Unlock()
START:
	for i := sm.cursor.Load() + 1; i <= sm.maxId.Load(); i++ {
		if me, ok := sm.execm.Get(i); ok {
			if me != nil && me.GetKey() > 0 {
				switch v := raft.MTYPE(me.GetPType()); v {
				case raft.MEM_PUT, raft.MEM_APPEND:
					sm.put(me)
				case raft.MEM_DEL, raft.MEM_DELKV:
					sm.del(v, me)
				}
			}
			sm.cursor.Store(i)
			sm.execm.Del(i)
		} else {
			if i != sm.noExecs[0] && sm.noExecs[0] > 0 && sm.noExecs[2] == 1 && time.Now().UnixNano()-sm.noExecs[1] < sm.node.getWaitTime().Nanoseconds() && i-sm.noExecs[0] < 10000 {
				if _, ok := sm.node.memSyncAction(i); ok {
					goto START
				}
			}
			if sm.noExecs[0] == i {
				if time.Now().UnixNano()-sm.noExecs[1] > sm.node.getWaitTime().Nanoseconds() {
					sm.noExecs[1] = time.Now().UnixNano()
					if _, ok := sm.node.memSyncAction(i); ok {
						sm.noExecs[2] = 1
						goto START
					}
				}
			} else {
				sm.noExecs = [3]int64{i, time.Now().UnixNano(), 0}
			}
			break
		}
	}
	return
}

func (sm *simpleMem) put(mb *stub.MemBean) {
	key := mb.GetKey()
	var iu bool
	if raft.MTYPE(mb.GetPType()) == raft.MEM_PUT {
		_, iu = sm.fsm.Put(key, mb)
	} else {
		var mm *stub.MemBean
		if mm, iu = sm.fsm.Get(key); !iu {
			mb.Mvmap = make(map[string]bool)
			mb.Mvmap[string(mb.GetValue())] = false
			sm.fsm.Put(key, mb)
		} else {
			mm.Value = mb.GetValue()
			mm.Mvmap[string(mb.GetValue())] = false
			mm.ExpiredTime = mb.GetExpiredTime()
		}
	}
	sm.ep.Put(mb.GetExpiredTime(), mb.GetKey())
	if mm, ok := sm.watchmap.Get(key); ok {
		if iu {
			if wb, b := mm.Get(raft.UPDATE); b {
				if wb.sync {
					defer sys.Recoverable(nil)
					wb.fn(wb.key, mb.GetValue(), raft.UPDATE)
				} else {
					go func() {
						defer sys.Recoverable(nil)
						wb.fn(wb.key, mb.GetValue(), raft.UPDATE)
					}()
				}
			}
		} else {
			if wb, b := mm.Get(raft.ADD); b {
				if wb.sync {
					defer sys.Recoverable(nil)
					wb.fn(wb.key, mb.GetValue(), raft.ADD)
				} else {
					go func() {
						defer sys.Recoverable(nil)
						wb.fn(wb.key, mb.GetValue(), raft.ADD)
					}()
				}
			}
		}
	}
}

func (sm *simpleMem) del(ptype raft.MTYPE, mb *stub.MemBean) {
	id := mb.GetKey()
	var value []byte
	if ptype == raft.MEM_DEL {
		sm.fsm.Del(id)
	} else {
		if value = mb.GetValue(); value != nil {
			if mm, b := sm.fsm.Get(id); b {
				delete(mm.Mvmap, string(value))
				if len(mm.Mvmap) == 0 {
					sm.fsm.Del(id)
				}
			}
		}
	}
	if mm, ok := sm.watchmap.Get(id); ok {
		if wb, b := mm.Get(raft.DELETE); b {
			if wb.sync {
				defer sys.Recoverable(nil)
				wb.fn(wb.key, value, raft.DELETE)
			} else {
				go func() {
					defer sys.Recoverable(nil)
					wb.fn(wb.key, value, raft.DELETE)
				}()
			}
		}
	}
}

// GetValue looks up and returns the corresponding value for a given key. Returns an empty value if the key does not exist or has expired.
func (sm *simpleMem) GetValue(key []byte) (value []byte, ok bool) {
	id := util.FNVHash64(key)
	if v, b := sm.fsm.Get(id); b {
		if !sm.checkExpire(v.GetExpiredTime()) {
			value = v.GetValue()
			ok = true
		} else {
			sm.fsm.Del(id)
		}
	}
	return
}

// GetMultiValue looks up and returns the corresponding value for a given key. Returns an empty value if the key does not exist or has expired.
func (sm *simpleMem) GetMultiValue(key []byte) (r [][]byte, ok bool) {
	id := util.FNVHash64(key)
	if v, b := sm.fsm.Get(id); b {
		if !sm.checkExpire(v.GetExpiredTime()) {
			if length := len(v.GetMvmap()); length > 0 {
				r = make([][]byte, 0, length)
				for k := range v.GetMvmap() {
					r = append(r, []byte(k))
				}
			} else {
				r = [][]byte{v.GetValue()}
			}
			ok = true
		} else {
			sm.fsm.Del(id)
		}
	}
	return
}

// GetValueList retrieves a list of values for multiple keys.
func (sm *simpleMem) GetValueList(keys [][]byte) (result [][2][]byte) {
	result = make([][2][]byte, 0)
	for _, key := range keys {
		id := util.FNVHash64(key)
		if v, b := sm.fsm.Get(id); b {
			if !sm.checkExpire(v.GetExpiredTime()) {
				result = append(result, [2][]byte{key, v.GetValue()})
			} else {
				sm.fsm.Del(id)
			}
		}
	}
	return
}

// GetMultiValueList retrieves a list of values for multiple keys.
func (sm *simpleMem) GetMultiValueList(keys [][]byte) (result [][2][]byte) {
	result = make([][2][]byte, 0)
	for _, key := range keys {
		id := util.FNVHash64(key)
		if v, b := sm.fsm.Get(id); b {
			if !sm.checkExpire(v.GetExpiredTime()) {
				if len(v.GetMvmap()) > 0 {
					for k := range v.GetMvmap() {
						result = append(result, [2][]byte{key, []byte(k)})
					}
				} else {
					result = append(result, [2][]byte{key, v.GetValue()})
				}
			} else {
				sm.fsm.Del(id)
			}
		}
	}
	return
}

// GetLogEntry retrieves a specific MemBean object by its MemId.
func (sm *simpleMem) GetLogEntry(memId int64) *stub.MemBean {
	v, _ := sm.logm.Get(memId)
	return v
}

// execTK periodically triggers checks to ensure all pending operations are processed in a timely manner.
func (sm *simpleMem) execTK() {
	tk := time.NewTicker(time.Second)
	defer sm.cancel()
	for !sm.node.nodeClose {
		select {
		case <-tk.C:
			sm.exec()
		}
	}
}

// expireTk periodically triggers the cleanup of expired data to maintain only valid entries in fsm.
func (sm *simpleMem) expireTk() {
	tk := time.NewTicker(200 * time.Millisecond)
	for !sm.node.nodeClose {
		select {
		case <-tk.C:
			sm.expiredData()
		}
	}
}

// checkExpire checks whether the given timestamp has exceeded the preset expiration time.
func (sm *simpleMem) checkExpire(t uint64) bool {
	return uint64(time.Now().UnixNano()) >= t
}

// expiredData removes expired data items from fsm, ensuring only valid data remains.
func (sm *simpleMem) expiredData() {
START:
	limit := 1 << 10
	if ids := sm.ep.getExpiredKey(limit); len(ids) > 0 {
		for id := range ids {
			if v, b := sm.fsm.Get(id); b {
				if sm.checkExpire(v.GetExpiredTime()) {
					sm.del(raft.MEM_DEL, v)
				}
			}
		}
		if len(ids) >= limit {
			time.Sleep(time.Millisecond)
			goto START
		}
	}
}

func (sm *simpleMem) String() string {
	return fmt.Sprintf("cursor:%d,maxId:%d,fsm-len:%d", sm.cursor.Load(), sm.maxId.Load(), sm.fsm.Len())
}

func (sm *simpleMem) UnWatchWithType(key []byte, wt raft.WatchType) {
	kid := util.FNVHash64(key)
	sm.watchlock.Lock(int64(kid))
	defer sm.watchlock.Unlock(int64(kid))
	if mm, ok := sm.watchmap.Get(kid); ok {
		mm.Del(wt)
	}
}

func (sm *simpleMem) UnWatch(key []byte) {
	sm.watchmap.Del(util.FNVHash64(key))
}

func (sm *simpleMem) Watch(key []byte, watchTypes []raft.WatchType, isSync bool, watchFunc func(key, value []byte, watchType raft.WatchType)) {
	kid := util.FNVHash64(key)
	sm.watchlock.Lock(int64(kid))
	defer sm.watchlock.Unlock(int64(kid))
	if mm, ok := sm.watchmap.Get(kid); ok {
		for _, watchType := range watchTypes {
			mm.Put(watchType, &watchBean{key: key, fn: watchFunc, sync: isSync})
		}
	} else {
		hm := hashmap.NewMap[raft.WatchType, *watchBean]()
		for _, watchType := range watchTypes {
			hm.Put(watchType, &watchBean{key: key, fn: watchFunc, sync: isSync})
		}
		sm.watchmap.Put(kid, hm)
	}
}

type KV struct {
	key   uint64
	value uint64
}

type expiredPool struct {
	expiredmap *hashmap.TreeMap[uint64, uint64]
	maplist    *hashmap.Map[uint64, []uint64]
	ctx        context.Context
	ch         chan *KV
}

func newExpiredPool(ctx context.Context) (r *expiredPool) {
	r = &expiredPool{ctx: ctx, maplist: hashmap.NewMap[uint64, []uint64](), expiredmap: hashmap.NewTreeMap[uint64, uint64](256), ch: make(chan *KV, 1<<17)}
	go r.poll()
	return
}

func (ep *expiredPool) Put(key uint64, value uint64) {
	ep.ch <- &KV{key: key, value: value}
}

func (ep *expiredPool) set(key, value uint64) {
	if prev, ok := ep.expiredmap.Put(key, value); ok {
		array, b := ep.maplist.Get(key)
		if b {
			array = append(array, value)
		} else {
			array = []uint64{prev, value}
		}
		ep.maplist.Put(key, array)
	}
}

func (ep *expiredPool) getExpiredKey(limit int) map[uint64]byte {
	rs := make(map[uint64]byte, 0)
	dels := make([]uint64, 0)
	ep.expiredmap.Ascend(func(et uint64, id uint64) bool {
		if et <= uint64(time.Now().UnixNano()) && len(rs) <= limit {
			dels = append(dels, et)
			if array, b := ep.maplist.Get(et); b {
				for _, v := range array {
					rs[v] = 0
				}
			} else {
				rs[id] = 0
			}
			return true
		} else {
			return false
		}
	})
	for _, et := range dels {
		ep.expiredmap.Del(et)
		ep.maplist.Del(et)
	}
	return rs
}

func (ep *expiredPool) Del(keyTime uint64) {
	ep.expiredmap.Del(keyTime)
	ep.maplist.Del(keyTime)
}

func (ep *expiredPool) poll() {
	for {
		select {
		case <-ep.ctx.Done():
			goto END
		case kv := <-ep.ch:
			ep.set(kv.key, kv.value)
		}
	}
END:
}
