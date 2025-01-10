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
	"github.com/donnie4w/raftx/stub"
	"sort"
)

func (n *Node) TakeSnapshot(fromTransactionId, toTransactionId int64) (r []byte, err error) {
	if m, e := n.applymsg.Snapshot(fromTransactionId, toTransactionId); e == nil {
		r = stub.Marshal(m)
	} else {
		return nil, e
	}
	return
}

func (n *Node) RestoreSnapshot(snapshotData []byte) (err error) {
	mb := &stub.MvccBean{}
	if err = stub.Unmarshal(snapshotData, mb); err == nil {
		if len(mb.Beans) > 0 {
			sort.Slice(mb.Beans, func(i, j int) bool { return mb.Beans[i].GetRxId() < mb.Beans[j].GetRxId() })
			for _, bean := range mb.Beans {
				if n.saveRxBat(bean.GetRxId(), &stub.RxBat{Term: n.term, Mc: bean}) == nil {
					go n.bat.setRxMax(bean.GetRxId())
				}
			}
		}
	}
	return
}
