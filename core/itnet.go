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
)

type csNet interface {
	Id() int64
	IsValid() bool
	Close() (err error)
	Chap(syncId int64, chap *stub.Chap) (err error)
	ChapAck(syncId int64, chap *stub.Chap) (err error)
	Auth(syncId int64, auth *stub.Auth) (err error)
	AuthAck(syncId int64, auth *stub.Auth) (err error)
	Vote(syncId int64, vote *stub.Vote) (err error)
	VoteAck(syncId int64, voteAck *stub.VoteAck) (err error)
	Commit(syncId int64, commit *stub.Commit) (err error)
	CommitAck(syncId int64, cmmitAck *stub.CommitAck) (err error)
	RollBack(syncId int64, commit *stub.RollBack) (err error)
	RollBackAck(syncId int64, commit *stub.RollBackAck) (err error)
	AppendEntries(syncId int64, appendEntries *stub.AppendEntries) (err error)
	AppendEntriesAck(syncId int64, appendEntriesAck *stub.AppendEntriesAck) (err error)
	ReqLogEntries(syncId int64, reqLogEntries *stub.ReqLogEntries) (err error)
	ReqLogEntriesAck(syncId int64, reqLogEntriesAck *stub.ReqLogEntriesAck) (err error)
	HeartBeat(syncId int64, heartBeat *stub.HeartBeat) (err error)
	HeartBeatAck(syncId int64, heartBeatAck *stub.HeartBeatAck) (err error)
	Ping(syncId int64, ping *stub.Ping) (err error)
	Pong(syncId int64, pong *stub.Pong) (err error)
	Proxy(syncId int64, proxy *stub.Proxy) (err error)
	ProxyAck(syncId int64, ack *stub.ProxyAck) (err error)
	ProxyRead(syncId int64, rbean *stub.ReadBean) (err error)
	ProxyReadAck(syncId int64, rbean *stub.ReadBean) (err error)
	NodeInfo(syncId int64, ni *stub.NodeInfo) (err error)
	NodeInfoAck(syncId int64, ni *stub.NodeInfo) (err error)
	addNoAck() int32
	RxSync(syncId int64, rxEntries *stub.RxEntries) (err error)
	RxSyncAck(syncId int64, rxEntriesAck *stub.RxEntriesAck) (err error)
	MemApply(syncId int64, memApply *stub.MemApply) (err error)
	MemApplyAck(syncId int64, memApplyAck *stub.MemApplyAck) (err error)
	MemSync(syncId int64, memSync *stub.MemSync) (err error)
	MemSyncAck(syncId int64, memSyncAck *stub.MemSyncAck) (err error)
	MemProxy(syncId int64, memProxy *stub.MemProxy) (err error)
	MemProxyAck(syncId int64, memProxyAck *stub.MemProxyAck) (err error)
	MemProxyRead(syncId int64, rbean *stub.ReadBean) (err error)
	MemProxyReadAck(syncId int64, rbean *stub.ReadBean) (err error)
}
