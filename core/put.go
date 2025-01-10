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
	"github.com/donnie4w/raftx/stub"
	"time"
)

func (n *Node) saveLogEntryBat(logEntryBat *stub.LogEntryBat) (err error) {
	if err = n.logStorage.Append(logEntryBat.GetLogIndex(), stub.Marshal(logEntryBat)); err == nil {
		n.bat.setLogMax(logEntryBat.GetLogIndex())
		n.logEntryBatPool.Put(logEntryBat.GetLogIndex(), logEntryBat)
	}
	return
}

func (n *Node) saveRxBat(rxId int64, rxBat *stub.RxBat) (err error) {
	if err = n.persistent.Put(KEY.RxId(rxId), stub.Marshal(rxBat)); err == nil {
		n.rxBatPool.Put(rxId, rxBat)
	}
	return
}

func (n *Node) saveRollbackRxBat(rxId int64, rxBat *stub.RxBat) (err error) {
	n.lock.RollBackMux.Lock()
	defer n.lock.RollBackMux.Unlock()
	rollbackId := n.bat.RollbackIncrement.Load() + 1
	var batch [3][2][]byte
	batch[0] = [2][]byte{KEY.RollbackId(rollbackId), util.Int64ToBytes(rxId)}
	batch[1] = [2][]byte{KEY.RxId(rxId), stub.Marshal(rxBat)}
	batch[2] = [2][]byte{KEYSYS.RollBackIncrement(), util.Int64ToBytes(rollbackId)}
	to := time.After(n.waitTimeout)
START:
	if err = n.persistent.BatchPut(batch[:]); err == nil {
		n.rxBatPool.Put(rxId, rxBat)
		n.bat.RollbackIncrement.Store(rollbackId)
	} else {
		<-time.After(defaultRestartInterval)
		select {
		case <-to:
			return
		default:
			goto START
		}
	}
	return
}

func (n *Node) saveToRollback(nodeId, rxid int64, addr string) {
	n.lock.ToRollBackMux.Lock()
	defer n.lock.ToRollBackMux.Unlock()
	var maxTR *stub.ToRollack
	if maxId := n.bat.ToRollbacMax.Load(); maxId != 0 {
		maxTR = n.getToRollack(maxId)
	}
	var batch = make([][2][]byte, 0)
	toRollbackId := n.bat.ToRollbackIncrement.Load() + 1
	newTr := &stub.ToRollack{RxId: rxid, NodeId: nodeId, Addr: addr, TrId: toRollbackId}
	if maxTR != nil {
		maxTR.Next = newTr.GetTrId()
		newTr.Prev = maxTR.GetTrId()
		batch = append(batch, [2][]byte{KEY.ToRollbackId(maxTR.GetTrId()), stub.Marshal(maxTR)})
	}
	if n.bat.ToRollbacCustor.Load() == 0 {
		batch = append(batch, [2][]byte{KEYSYS.ToRollbackCursor(), util.Int64ToBytes(toRollbackId)})
		n.bat.ToRollbacCustor.Store(toRollbackId)
	}
	batch = append(batch, [2][]byte{KEYSYS.ToRollBackIncrement(), util.Int64ToBytes(toRollbackId)})
	batch = append(batch, [2][]byte{KEYSYS.ToRollbackMax(), util.Int64ToBytes(toRollbackId)})
	batch = append(batch, [2][]byte{KEY.ToRollbackId(toRollbackId), stub.Marshal(newTr)})

	to := time.After(n.waitTimeout)
START:
	if err := n.persistent.BatchPut(batch); err != nil {
		<-time.After(defaultRestartInterval)
		select {
		case <-to:
			return
		default:
			goto START
		}
	} else {
		n.bat.ToRollbacMax.Store(toRollbackId)
		n.bat.ToRollbackIncrement.Store(toRollbackId)
	}
}

func (n *Node) removeToRollbackId(trb *stub.ToRollack) (err error) {
	n.lock.ToRollBackMux.Lock()
	defer n.lock.ToRollBackMux.Unlock()
	trId := trb.GetTrId()
	prev, next := trb.GetPrev(), trb.GetNext()
	prevTR, nextTR := n.getToRollack(prev), n.getToRollack(next)
	batch := make([][2][]byte, 0)
	if prevTR != nil {
		prevTR.Next = next
		batch = append(batch, [2][]byte{KEY.ToRollbackId(prev), stub.Marshal(prevTR)})
	}
	if nextTR != nil {
		nextTR.Prev = prev
		batch = append(batch, [2][]byte{KEY.ToRollbackId(next), stub.Marshal(nextTR)})
	}
	if n.bat.ToRollbacMax.Load() == trId {
		batch = append(batch, [2][]byte{KEYSYS.ToRollbackMax(), util.Int64ToBytes(0)})
		n.bat.ToRollbacMax.Store(0)
	}
	if trId == n.bat.ToRollbacCustor.Load() {
		batch = append(batch, [2][]byte{KEYSYS.ToRollbackCursor(), util.Int64ToBytes(next)})
		n.bat.ToRollbacCustor.Store(next)
	}
	if err = n.persistent.BatchPut(batch); err == nil {
		err = n.persistent.Del(KEY.ToRollbackId(trId))
	}
	return
}

func (n *Node) delLogEntryBatPool(logId int64) {
	n.logEntryBatPool.Del(logId)
}

func (n *Node) delRxBatPool(logId int64) {
	n.rxBatPool.Del(logId)
}
