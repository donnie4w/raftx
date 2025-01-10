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
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
)

func (n *Node) LogStoreBatchDel(fromId, toId int64) error {
	if fromId > 0 && toId > fromId {
		ids := make([]int64, 0)
		for i := fromId; i <= toId; i++ {
			ids = append(ids, i)
		}
		return n.logStorage.BatchDel(ids)
	} else {
		return rafterrors.ErrParameter
	}
}

func (n *Node) LogStoreDel(logId int64) error {
	return n.logStorage.Del(logId)
}

func (n *Node) LogStoreMaxId() int64 {
	return n.bat.LogMax.Load()
}

func (n *Node) LogStoreGet(logId int64) *stub.LogEntryBat {
	return n.getLogEntryBat(logId)
}
