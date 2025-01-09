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

type Bat struct {
	noExecs             [3]int64
	node                *Node
	RxExecCustor        atomic.Int64
	RxMax               atomic.Int64
	RxTotal             atomic.Int64
	LogMax              atomic.Int64
	RollbackIncrement   atomic.Int64
	RollbacCustor       atomic.Int64
	ToRollbackIncrement atomic.Int64
	ToRollbacCustor     atomic.Int64
	ToRollbacMax        atomic.Int64
	rxmaxmux            sync.Mutex
	logincmux           sync.Mutex
	execTl              sync.Mutex
	bmp                 *bitmap
}

func newBat(node *Node) *Bat {
	return &Bat{node: node, bmp: newBitmap(1 << 25)}
}

func (b *Bat) newRxId() (r int64) {
	r = b.RxMax.Add(1)
	go b.saveRxId()
	return
}

func (b *Bat) saveRxId() {
	if b.rxmaxmux.TryLock() {
		defer b.rxmaxmux.Unlock()
	START:
		r := b.RxMax.Load()
		b.node.persistent.Put(KEYSYS.RxMaxId(), util.Int64ToBytes(b.RxMax.Load()))
		if b.RxMax.Load() > r {
			goto START
		}
	}
}

// setRxMax must be a async function
func (b *Bat) setRxMax(rxId int64) {
START:
	if v := b.RxMax.Load(); v < rxId {
		if b.RxMax.CompareAndSwap(v, rxId) {
			b.saveRxId()
		} else if b.RxMax.Load() < rxId {
			goto START
		}
	}
}

func (b *Bat) newLogId() (r int64) {
	r = b.LogMax.Add(1)
	if b.logincmux.TryLock() {
		defer b.logincmux.Unlock()
		b.node.persistent.Put(KEYSYS.LogMaxId(), util.Int64ToBytes(b.LogMax.Load()))
	}
	return
}

func (b *Bat) setLogMax(logId int64) error {
	if b.LogMax.Load() < logId {
		b.logincmux.Lock()
		defer b.logincmux.Unlock()
		if b.LogMax.Load() < logId {
			b.LogMax.Store(logId)
			b.node.persistent.Put(KEYSYS.LogMaxId(), util.Int64ToBytes(b.LogMax.Load()))
		}
	}
	return nil
}

func (b *Bat) init() (err error) {
	defer sys.Recoverable(&err)
	if bs, err := b.node.persistent.Get(KEYSYS.RxExecCursor()); err == nil && len(bs) > 0 {
		b.RxExecCustor.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.RxMaxId()); err == nil && len(bs) > 0 {
		b.RxMax.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.RxTotal()); err == nil && len(bs) > 0 {
		b.RxTotal.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.LogMaxId()); err == nil && len(bs) > 0 {
		b.LogMax.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.ToRollbackMax()); err == nil && len(bs) > 0 {
		b.ToRollbacMax.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.RollBackIncrement()); err == nil && len(bs) > 0 {
		b.RollbackIncrement.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.ToRollBackIncrement()); err == nil && len(bs) > 0 {
		b.ToRollbackIncrement.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.RollbackCursor()); err == nil && len(bs) > 0 {
		b.RollbacCustor.Store(util.BytesToInt64(bs))
	}
	if bs, err := b.node.persistent.Get(KEYSYS.ToRollbackCursor()); err == nil && len(bs) > 0 {
		b.ToRollbacCustor.Store(util.BytesToInt64(bs))
	}
	if b.node.commitMode == raft.ORDEREDPOLL {
		go b.tickerExPoll()
	} else {
		go b.tickerExMvcc()
	}
	return
}

// execPoll Execute a transaction that was not originally completed
func (b *Bat) execPoll() (err error) {
	defer sys.Recoverable(&err)
	if b.execTl.TryLock() {
		defer b.execTl.Unlock()
	START:
		for i := b.RxExecCustor.Load() + 1; i <= b.RxMax.Load(); i++ {
			if b.rxExecPoll(i) {
				continue
			}
			if b.node.isRunStat() {
				if i != b.noExecs[0] && b.noExecs[0] > 0 && b.noExecs[2] == 1 && time.Now().UnixNano()-b.noExecs[1] < b.node.getWaitTime().Nanoseconds() && i-b.noExecs[0] < 10000 {
					if b.node.rxSyncByIdAction(i) {
						goto START
					}
				}
				if b.noExecs[0] == i {
					if time.Now().UnixNano()-b.noExecs[1] > b.node.getWaitTime().Nanoseconds() {
						log.Warn("no found rxid:", i)
						b.noExecs[1] = time.Now().UnixNano()
						if b.node.rxSyncByIdAction(i) {
							b.noExecs[2] = 1
							goto START
						}
					}
				} else {
					b.noExecs = [3]int64{i, time.Now().UnixNano(), 0}
				}
			}
			break
		}
	}
	return
}

func (b *Bat) rxExecPoll(rxId int64) (ok bool) {
	if rxBat := b.node.getRxBat(rxId); rxBat != nil {
		logId := rxBat.GetLogIndex()
		if rxBat.GetMc() != nil {
			if b.node.applymsg.ApplyMc(rxBat.GetMc()) == nil {
				ok = true
			}
		} else if logId == 0 {
			if b.node.applymsg.Apply(rxId, nil) == nil {
				ok = true
			}
		} else if logEntryBat := b.node.getLogEntryBat(logId); logEntryBat != nil {
			if b.node.applymsg.Apply(rxId, logEntryBat.GetCommand()) == nil {
				ok = true
			}
		}
		if ok {
			l := 1
			var batch [2][2][]byte
			batch[0] = [2][]byte{KEYSYS.RxExecCursor(), util.Int64ToBytes(rxId)}
			if !b.bmp.Has(rxId) {
				b.bmp.Set(rxId)
				batch[1] = [2][]byte{KEYSYS.RxTotal(), util.Int64ToBytes(b.RxTotal.Add(1))}
				l = 2
			}
			if err := b.node.persistent.BatchPut(batch[:l]); err == nil {
				b.RxExecCustor.Store(rxId)
				b.node.delLogEntryBatPool(rxBat.GetLogIndex())
				b.node.delRxBatPool(rxId)
				if b.node.isLeader() && logId != 0 && !b.node.standAlone {
					go b.node.dataWait.Close(logId)
				}
			}
		}
	}
	return
}

func (b *Bat) rollBack(rxId int64, rxBat *stub.RxBat) (err error) {
	log.Debug("rollBack:", rxId, ",", rxBat)
	if err = b.node.saveRollbackRxBat(rxId, rxBat); err == nil {
		go b.setRxMax(rxId)
	}
	if err == nil {
		if b.node.commitMode == raft.ORDEREDPOLL {
			go b.execPoll()
		} else {
			b.execMvcc(rxId, rxBat)
		}
		go b.node.rollback()
	} else {
		log.Error(err)
	}
	return
}

func (b *Bat) commit(rxId int64, rxBat *stub.RxBat) (err error) {
	if b.bmp.Has(rxId) {
		if b.node.applymsg.HasRx(rxId) {
			return rafterrors.ErrRepeatedRxId
		}
	}
	if err = b.node.saveRxBat(rxId, rxBat); err == nil {
		go b.setRxMax(rxId)
	}
	if err == nil {
		if b.node.commitMode == raft.ORDEREDPOLL {
			go b.execPoll()
		} else if !b.execMvcc(rxId, rxBat) {
			return rafterrors.ErrPersistenceUpdateFailed
		}
	} else {
		log.Error(err)
		err = rafterrors.ErrPersistenceUpdateFailed
	}
	return
}

func (b *Bat) tickerExPoll() {
	tk := time.NewTicker(b.node.heartbeatInterval)
	for {
		select {
		case <-tk.C:
			b.execPoll()
		}
	}
}
