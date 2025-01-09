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
	"github.com/donnie4w/raftx/stub"
	"time"
)

func (b *Bat) execMvcc(rxId int64, rxBat *stub.RxBat) (ok bool) {
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
		if !b.bmp.Has(rxId) {
			b.addRxTotal()
			b.bmp.Set(rxId)
		}
		b.node.delLogEntryBatPool(rxBat.GetLogIndex())
		b.node.delRxBatPool(rxId)
	}
	return
}

func (b *Bat) addRxTotal() {
	r := b.RxTotal.Add(1)
	go b.saveRxTotal(r)
}

func (b *Bat) saveRxTotal(v int64) {
	if b.node.lock.RxTotalTL.TryLock() {
		b.node.lock.RxTotalTL.Unlock()
		for {
			t := b.RxTotal.Load()
			if t > v {
				b.node.persistent.Put(KEYSYS.RxTotal(), util.Int64ToBytes(b.RxTotal.Load()))
			}
			if b.RxTotal.Load() == t {
				break
			}
		}
	}
}

func (b *Bat) setRxCustor(rxId int64) {
START:
	if v := b.RxExecCustor.Load(); v < rxId {
		if b.RxExecCustor.CompareAndSwap(v, rxId) {
			go b.saveRxCustor()
		} else if b.RxExecCustor.Load() < rxId {
			goto START
		}
	}
}

func (b *Bat) saveRxCustor() {
	if b.node.lock.RxCustorTL.TryLock() {
		defer b.node.lock.RxCustorTL.Unlock()
	START:
		r := b.RollbacCustor.Load()
		b.node.persistent.Put(KEYSYS.RxExecCursor(), util.Int64ToBytes(b.RollbacCustor.Load()))
		if b.RollbacCustor.Load() > r {
			goto START
		}
	}
}

func (b *Bat) tickerExMvcc() {
	tk := time.NewTicker(time.Second)
	for {
		select {
		case <-tk.C:
		START:
			if b.RxMax.Load() > b.RxExecCustor.Load() {
				for i := b.RxExecCustor.Load() + 1; i <= b.RxMax.Load(); i++ {
					if b.node.applymsg.HasRx(i) {
						b.setRxCustor(i)
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
		}
	}
}
