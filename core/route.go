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
	"fmt"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
)

func router(bs []byte, cn csNet) (err error) {
	defer sys.Recoverable(&err)
	switch bs[0] {
	case Ping:
		if len(bs) <= 9 {
			return cn.Ping(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Ping{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Ping(util.BytesToInt64(bs[1:9]), p)
		}
	case Pong:
		if len(bs) <= 9 {
			return cn.Pong(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Pong{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Pong(util.BytesToInt64(bs[1:9]), p)
		}
	case Chap:
		if len(bs) <= 9 {
			return cn.Chap(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Chap{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Chap(util.BytesToInt64(bs[1:9]), p)
		}
	case ChapAck:
		if len(bs) <= 9 {
			return cn.ChapAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Chap{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ChapAck(util.BytesToInt64(bs[1:9]), p)
		}
	case Auth:
		if len(bs) <= 9 {
			return cn.Auth(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Auth{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Auth(util.BytesToInt64(bs[1:9]), p)
		}
	case AuthAck:
		if len(bs) <= 9 {
			return cn.AuthAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Auth{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.AuthAck(util.BytesToInt64(bs[1:9]), p)
		}
	case Vote:
		if len(bs) <= 9 {
			return cn.Vote(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Vote{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Vote(util.BytesToInt64(bs[1:9]), p)
		}
	case VoteAck:
		if len(bs) <= 9 {
			return cn.VoteAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.VoteAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.VoteAck(util.BytesToInt64(bs[1:9]), p)
		}
	case Commit:
		if len(bs) <= 9 {
			return cn.Commit(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Commit{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Commit(util.BytesToInt64(bs[1:9]), p)
		}
	case CommitAck:
		if len(bs) <= 9 {
			return cn.CommitAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.CommitAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.CommitAck(util.BytesToInt64(bs[1:9]), p)
		}
	case AppendEntries:
		if len(bs) <= 9 {
			return cn.AppendEntries(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.AppendEntries{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.AppendEntries(util.BytesToInt64(bs[1:9]), p)
		}
	case AppendEntriesAck:
		if len(bs) <= 9 {
			return cn.AppendEntriesAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.AppendEntriesAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.AppendEntriesAck(util.BytesToInt64(bs[1:9]), p)
		}
	case ReqLogEntries:
		if len(bs) <= 9 {
			return cn.ReqLogEntries(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReqLogEntries{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ReqLogEntries(util.BytesToInt64(bs[1:9]), p)
		}
	case ReqLogEntriesAck:
		if len(bs) <= 9 {
			return cn.ReqLogEntriesAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReqLogEntriesAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ReqLogEntriesAck(util.BytesToInt64(bs[1:9]), p)
		}
	case HeartBeat:
		if len(bs) <= 9 {
			return cn.HeartBeat(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.HeartBeat{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.HeartBeat(util.BytesToInt64(bs[1:9]), p)
		}
	case HeartBeatAck:
		if len(bs) <= 9 {
			return cn.HeartBeatAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.HeartBeatAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.HeartBeatAck(util.BytesToInt64(bs[1:9]), p)
		}
	case Proxy:
		if len(bs) <= 9 {
			return cn.Proxy(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.Proxy{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.Proxy(util.BytesToInt64(bs[1:9]), p)
		}
	case ProxyAck:
		if len(bs) <= 9 {
			return cn.ProxyAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ProxyAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ProxyAck(util.BytesToInt64(bs[1:9]), p)
		}
	case NodeInfo:
		if len(bs) <= 9 {
			return cn.NodeInfo(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.NodeInfo{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.NodeInfo(util.BytesToInt64(bs[1:9]), p)
		}
	case NodeInfoAck:
		if len(bs) <= 9 {
			return cn.NodeInfoAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.NodeInfo{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.NodeInfoAck(util.BytesToInt64(bs[1:9]), p)
		}
	case ProxyRead:
		if len(bs) <= 9 {
			return cn.ProxyRead(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReadBean{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ProxyRead(util.BytesToInt64(bs[1:9]), p)
		}
	case ProxyReadAck:
		if len(bs) <= 9 {
			return cn.ProxyReadAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReadBean{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.ProxyReadAck(util.BytesToInt64(bs[1:9]), p)
		}
	case RollBack:
		if len(bs) <= 9 {
			return cn.RollBack(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.RollBack{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.RollBack(util.BytesToInt64(bs[1:9]), p)
		}
	case RollBackAck:
		if len(bs) <= 9 {
			return cn.RollBackAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.RollBackAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.RollBackAck(util.BytesToInt64(bs[1:9]), p)
		}
	case RxSync:
		if len(bs) <= 9 {
			return cn.RxSync(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.RxEntries{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.RxSync(util.BytesToInt64(bs[1:9]), p)
		}
	case RxSyncAck:
		if len(bs) <= 9 {
			return cn.RxSyncAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.RxEntriesAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.RxSyncAck(util.BytesToInt64(bs[1:9]), p)
		}
	case MemApply:
		if len(bs) <= 9 {
			return cn.MemApply(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemApply{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemApply(util.BytesToInt64(bs[1:9]), p)
		}
	case MemApplyAck:
		if len(bs) <= 9 {
			return cn.MemApplyAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemApplyAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemApplyAck(util.BytesToInt64(bs[1:9]), p)
		}
	case MemSync:
		if len(bs) <= 9 {
			return cn.MemSync(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemSync{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemSync(util.BytesToInt64(bs[1:9]), p)
		}
	case MemSyncAck:
		if len(bs) <= 9 {
			return cn.MemSyncAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemSyncAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemSyncAck(util.BytesToInt64(bs[1:9]), p)
		}
	case MemProxy:
		if len(bs) <= 9 {
			return cn.MemProxy(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemProxy{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemProxy(util.BytesToInt64(bs[1:9]), p)
		}
	case MemProxyAck:
		if len(bs) <= 9 {
			return cn.MemProxyAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.MemProxyAck{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemProxyAck(util.BytesToInt64(bs[1:9]), p)
		}
	case MemProxyRead:
		if len(bs) <= 9 {
			return cn.MemProxyRead(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReadBean{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemProxyRead(util.BytesToInt64(bs[1:9]), p)
		}
	case MemProxyReadAck:
		if len(bs) <= 9 {
			return cn.MemProxyReadAck(util.BytesToInt64(bs[1:9]), nil)
		}
		p := &stub.ReadBean{}
		if err = stub.Unmarshal(bs[9:], p); err == nil {
			return cn.MemProxyReadAck(util.BytesToInt64(bs[1:9]), p)
		}
	default:
		err = fmt.Errorf("unknow command")
	}
	return
}
