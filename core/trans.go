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
	"github.com/donnie4w/gofer/buffer"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"github.com/donnie4w/tsf"
	"google.golang.org/protobuf/proto"
	"sync"
	"sync/atomic"
)

type trans struct {
	ts      tsf.TsfSocket
	mux     sync.RWMutex
	noAck   atomic.Int32
	isValid bool
}

func newTrans(ts tsf.TsfSocket) csNet {
	return &trans{ts: ts, isValid: true}
}

func (t *trans) Id() int64 {
	return t.ts.ID()
}

func (t *trans) addNoAck() int32 {
	return t.noAck.Add(1)
}

func (t *trans) IsValid() bool { return t.isValid }

func (t *trans) Close() (err error) {
	defer sys.Recoverable(&err)
	if t != nil {
		t.mux.Lock()
		defer t.mux.Unlock()
		if t.isValid {
			t.isValid = false
			return t.ts.Close()
		}
	}
	return nil
}

func (t *trans) write(bs []byte) (err error) {
	if !t.isValid {
		return rafterrors.ErrConnectionFailed
	}
	t.mux.RLock()
	defer t.mux.RUnlock()
	_, err = t.ts.WriteWithMerge(bs)
	return
}

func (t *trans) writeMessage(tp byte, syncId int64, m proto.Message) (err error) {
	var bs []byte
	if m != nil {
		bs = stub.Marshal(m)
	}
	buf := buffer.NewBufferWithCapacity(9 + len(bs))
	buf.WriteByte(tp)
	buf.Write(util.Int64ToBytes(syncId))
	if len(bs) > 0 {
		buf.Write(bs)
	}
	return t.write(buf.Bytes())
}

func (t *trans) writeBytes(tp byte, syncId int64, body []byte) (err error) {
	if syncId != 0 {
		buf := buffer.NewBufferWithCapacity(9 + len(body))
		buf.WriteByte(tp)
		buf.Write(util.Int64ToBytes(syncId))
		buf.Write(body)
		return t.write(buf.Bytes())
	} else {
		buf := buffer.NewBufferWithCapacity(1 + len(body))
		buf.WriteByte(tp)
		buf.Write(body)
		return t.write(buf.Bytes())
	}
}

func (t *trans) Chap(syncId int64, chap *stub.Chap) (err error) {
	return t.writeMessage(Chap, syncId, chap)
}

func (t *trans) ChapAck(syncId int64, chap *stub.Chap) (err error) {
	return t.writeMessage(ChapAck, syncId, chap)
}

func (t *trans) Auth(syncId int64, auth *stub.Auth) (err error) {
	return t.writeMessage(Auth, syncId, auth)
}

func (t *trans) AuthAck(syncId int64, auth *stub.Auth) (err error) {
	return t.writeMessage(AuthAck, syncId, auth)
}

func (t *trans) Vote(syncId int64, vote *stub.Vote) (err error) {
	return t.writeMessage(Vote, syncId, vote)
}

func (t *trans) VoteAck(syncId int64, voteAck *stub.VoteAck) (err error) {
	return t.writeMessage(VoteAck, syncId, voteAck)
}

func (t *trans) Commit(syncId int64, commit *stub.Commit) (err error) {
	return t.writeMessage(Commit, syncId, commit)
}

func (t *trans) CommitAck(syncId int64, cmmitAck *stub.CommitAck) (err error) {
	return t.writeMessage(CommitAck, syncId, cmmitAck)
}

func (t *trans) RollBack(syncId int64, rollback *stub.RollBack) (err error) {
	return t.writeMessage(RollBack, syncId, rollback)
}

func (t *trans) RollBackAck(syncId int64, rollbackAck *stub.RollBackAck) (err error) {
	return t.writeMessage(RollBackAck, syncId, rollbackAck)
}

func (t *trans) AppendEntries(syncId int64, appendEntries *stub.AppendEntries) (err error) {
	return t.writeMessage(AppendEntries, syncId, appendEntries)
}

func (t *trans) AppendEntriesAck(syncId int64, appendEntriesAck *stub.AppendEntriesAck) (err error) {
	return t.writeMessage(AppendEntriesAck, syncId, appendEntriesAck)
}

func (t *trans) ReqLogEntries(syncId int64, reqLogEntries *stub.ReqLogEntries) (err error) {
	return t.writeMessage(ReqLogEntries, syncId, reqLogEntries)
}

func (t *trans) ReqLogEntriesAck(syncId int64, reqLogEntriesAck *stub.ReqLogEntriesAck) (err error) {
	return t.writeMessage(ReqLogEntriesAck, syncId, reqLogEntriesAck)
}

func (t *trans) HeartBeat(syncId int64, heartBeat *stub.HeartBeat) (err error) {
	return t.writeMessage(HeartBeat, syncId, heartBeat)
}

func (t *trans) HeartBeatAck(syncId int64, heartBeatAck *stub.HeartBeatAck) (err error) {
	return t.writeMessage(HeartBeatAck, syncId, heartBeatAck)
}

func (t *trans) Ping(syncId int64, ping *stub.Ping) (err error) {
	return t.writeMessage(Ping, syncId, ping)
}

func (t *trans) Pong(syncId int64, pong *stub.Pong) (err error) {
	return t.writeMessage(Pong, syncId, pong)
}

func (t *trans) Proxy(syncId int64, proxy *stub.Proxy) (err error) {
	return t.writeMessage(Proxy, syncId, proxy)
}

func (t *trans) ProxyAck(syncId int64, ack *stub.ProxyAck) (err error) {
	return t.writeMessage(ProxyAck, syncId, ack)
}

func (t *trans) NodeInfo(syncId int64, ni *stub.NodeInfo) (err error) {
	return t.writeMessage(NodeInfo, syncId, ni)
}

func (t *trans) NodeInfoAck(syncId int64, ni *stub.NodeInfo) (err error) {
	return t.writeMessage(NodeInfoAck, syncId, ni)
}

func (t *trans) ProxyRead(syncId int64, rbean *stub.ReadBean) (err error) {
	return t.writeMessage(ProxyRead, syncId, rbean)
}

func (t *trans) ProxyReadAck(syncId int64, rbean *stub.ReadBean) (err error) {
	return t.writeMessage(ProxyReadAck, syncId, rbean)
}

func (t *trans) RxSync(syncId int64, rxEntries *stub.RxEntries) (err error) {
	return t.writeMessage(RxSync, syncId, rxEntries)
}

func (t *trans) RxSyncAck(syncId int64, rxEntriesAck *stub.RxEntriesAck) (err error) {
	return t.writeMessage(RxSyncAck, syncId, rxEntriesAck)
}

func (t *trans) MemApply(syncId int64, memApply *stub.MemApply) (err error) {
	return t.writeMessage(MemApply, syncId, memApply)
}

func (t *trans) MemApplyAck(syncId int64, memApplyAck *stub.MemApplyAck) (err error) {
	return t.writeMessage(MemApplyAck, syncId, memApplyAck)
}

func (t *trans) MemSync(syncId int64, memSync *stub.MemSync) (err error) {
	return t.writeMessage(MemSync, syncId, memSync)
}

func (t *trans) MemSyncAck(syncId int64, memSyncAck *stub.MemSyncAck) (err error) {
	return t.writeMessage(MemSyncAck, syncId, memSyncAck)
}

func (t *trans) MemProxy(syncId int64, memProxy *stub.MemProxy) (err error) {
	return t.writeMessage(MemProxy, syncId, memProxy)
}

func (t *trans) MemProxyAck(syncId int64, memProxyAck *stub.MemProxyAck) (err error) {
	return t.writeMessage(MemProxyAck, syncId, memProxyAck)
}

func (t *trans) MemProxyRead(syncId int64, readBean *stub.ReadBean) (err error) {
	return t.writeMessage(MemProxyRead, syncId, readBean)
}

func (t *trans) MemProxyReadAck(syncId int64, readBean *stub.ReadBean) (err error) {
	return t.writeMessage(MemProxyReadAck, syncId, readBean)
}
