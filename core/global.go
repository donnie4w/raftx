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
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/stub"
	"sync"
	"time"
)

const (
	GRANTED_OK           int32 = 1
	GRANTED_TERM_OLD     int32 = 2
	GRANTED_ID_LITTLE    int32 = 3
	GRANTED_DATA_OLD     int32 = 4
	GRANTED_LEADER_ALIVE int32 = 5
	GRANTED_VOTEOTHER    int32 = 6
)

const (
	HEARTBEAT_OK        int32 = 1
	HEARTBEAT_TERM_OLD  int32 = 2
	HEARTBEAT_PARTITION int32 = 3
)

const (
	NODEINFO_SYNCID  int32 = 1
	NODEINFO_ADDPEER int32 = 2
	NODEINFO_DELPEER int32 = 3
	NODEINFO_NOTIFY  int32 = 4
)

const (
	SYNC_OK         int32 = 1
	SYNC_NOTFOUND   int32 = 2
	SYNC_NOT_LEADER int32 = 3
	SYNC_ERROR      int32 = 4
	SYNC_PREPARE    int32 = 5
)

const (
	SYNCTYPE_MISS  int32 = 1
	SYNCTYPE_EMPTY int32 = 2
	SYNCTYPE_EXIST int32 = 3
)

const (
	defaultElectionInterval    = 300 * time.Millisecond
	defaultHeartbeatInterval   = 300 * time.Millisecond
	defaultWaitTimeout         = 3 * time.Second
	defaultClientTimeout       = 15 * time.Second
	defaultRestartInterval     = defaultWaitTimeout / 5
	defaultHeartbeatRetryTimes = 3
	defaultMemExpiredTime      = 30 * 60
	defaultMemLogEntryLimit    = 1 << 20
)

const (
	SYSVAR     byte = 1
	RX         byte = 2
	LOG        byte = 3
	ROLLBACK   byte = 4
	TOROLLBACK byte = 5
)

const (
	_ byte = iota
	Ping
	Pong
	Chap
	ChapAck
	Auth
	AuthAck
	Vote
	VoteAck
	Commit
	CommitAck
	RollBack
	RollBackAck
	AppendEntries
	AppendEntriesAck
	ReqLogEntries
	ReqLogEntriesAck
	HeartBeat
	HeartBeatAck
	Proxy
	ProxyAck
	NodeInfo
	NodeInfoAck
	ProxyRead
	ProxyReadAck
	RxSync
	RxSyncAck
	MemApply
	MemApplyAck
	MemSync
	MemSyncAck
	MemProxy
	MemProxyAck
	MemProxyRead
	MemProxyReadAck
)

var (
	TERM                 = []byte("TERM")
	NODEID               = []byte("NODE_ID")
	RX_EXEC_CURSOR       = []byte("RX_EXEC_CURSOR")
	RX_MAX_ID            = []byte("RX_MAX_ID")
	RX_TOTAL             = []byte("RX_TOTAL")
	LOG_MAX_ID           = []byte("LOG_MAX_ID")
	ROLLBACK_INCREMENT   = []byte("ROLLBACK_INCREMENT")
	ROLLBACK_CURSOR      = []byte("ROLLBACK_CURSOR")
	TOROLLBACK_INCREMENT = []byte("TOROLLBACK_INCREMENT")
	TOROLLBACK_CURSOR    = []byte("TOROLLBACK_CURSOR")
	TOROLLBACK_MAX       = []byte("TOROLLBACK_MAX")
)

func bytes(prefix byte, flag []byte) []byte {
	return append([]byte{prefix}, flag...)
}

type keySys byte

func (k keySys) Term() []byte {
	return bytes(SYSVAR, TERM)
}

func (k keySys) NodeID() []byte {
	return bytes(SYSVAR, NODEID)
}

func (k keySys) RxExecCursor() []byte {
	return bytes(SYSVAR, RX_EXEC_CURSOR)
}

func (k keySys) RxMaxId() []byte {
	return bytes(SYSVAR, RX_MAX_ID)
}

func (k keySys) RxTotal() []byte {
	return bytes(SYSVAR, RX_TOTAL)
}

func (k keySys) LogMaxId() []byte {
	return bytes(SYSVAR, LOG_MAX_ID)
}

func (k keySys) RollBackIncrement() []byte {
	return bytes(SYSVAR, ROLLBACK_INCREMENT)
}

func (k keySys) RollbackCursor() []byte {
	return bytes(SYSVAR, ROLLBACK_CURSOR)
}

func (k keySys) ToRollBackIncrement() []byte {
	return bytes(SYSVAR, TOROLLBACK_INCREMENT)
}

func (k keySys) ToRollbackCursor() []byte {
	return bytes(SYSVAR, TOROLLBACK_CURSOR)
}

func (k keySys) ToRollbackMax() []byte {
	return bytes(SYSVAR, TOROLLBACK_MAX)
}

type key byte

func (k key) RxId(id int64) []byte {
	return bytes(RX, util.Int64ToBytes(id))
}

func (k key) LogId(id int64) []byte {
	return bytes(LOG, util.Int64ToBytes(id))
}

func (k key) RollbackId(id int64) []byte {
	return bytes(ROLLBACK, util.Int64ToBytes(id))
}

func (k key) ToRollbackId(id int64) []byte {
	return bytes(TOROLLBACK, util.Int64ToBytes(id))
}

const (
	KEYSYS keySys = 0
	KEY    key    = 0
)

type Lock struct {
	FollowerTl    sync.Mutex
	CandidateTl   sync.Mutex
	LeaderTl      sync.Mutex
	VoteTl        sync.Mutex
	SyncDataTl    sync.Mutex
	RollBackTl    sync.Mutex
	ToRollBackTl  sync.Mutex
	syncRxTl      sync.Mutex
	syncRxMux     sync.Mutex
	PeercsMux     sync.Mutex
	HeartBeatMux  sync.Mutex
	VoteSyncMux   sync.Mutex
	RollBackMux   sync.Mutex
	ToRollBackMux sync.Mutex
	NodeInfoMux   sync.Mutex
	MemMux        sync.RWMutex
	MemSyncMux    sync.Mutex
	RaftxSyncMux  sync.Mutex
	RxTotalTL     sync.Mutex
	RxCustorTL    sync.Mutex
}

type votedFor struct {
	VoteId int64
	Vote   *stub.Vote
}

func newVoteFor() votedFor {
	return votedFor{VoteId: -1}
}

type watchBean struct {
	key  []byte
	fn   func(key, value []byte, wt raft.WatchType)
	sync bool
}
