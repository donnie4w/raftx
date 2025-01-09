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
)

type logs struct {
	n *Node
}

func newLogs(n *Node) stub.LogStorage {
	return &logs{n: n}
}

func (logs *logs) Append(logIndex int64, LogEntryBatBinary []byte) error {
	return logs.n.persistent.Put(KEY.LogId(logIndex), LogEntryBatBinary)
}

func (logs *logs) Get(logIndex int64) ([]byte, error) {
	return logs.n.persistent.Get(KEY.LogId(logIndex))
}

func (logs *logs) Del(logIndex int64) error {
	return logs.n.persistent.Del(KEY.LogId(logIndex))
}

func (logs *logs) BatchDel(logIds []int64) error {
	bs := make([][]byte, len(logIds))
	for i, logId := range logIds {
		bs[i] = util.Int64ToBytes(logId)
	}
	return logs.n.persistent.BatchDel(bs)
}
