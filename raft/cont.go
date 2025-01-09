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

package raft

import (
	"crypto/tls"
	"fmt"
	"github.com/donnie4w/raftx/stub"
	"time"
)

// STATE represents the current state of a node in the Raft consensus algorithm.
type STATE byte

const (
	_ STATE = iota // Skip zero value for better readability

	// FOLLOWER indicates that the node is currently acting as a follower in the Raft cluster.
	// Followers do not participate in elections unless they do not hear from a leader for a certain period.
	FOLLOWER

	// CANDIDATE indicates that the node has transitioned into a candidate state.
	// A node becomes a candidate when it does not receive communication from a leader within the ElectionInterval.
	CANDIDATE

	// PREPARE indicates that the node is transitioning to become a leader or is in a pre-election phase.
	// In this state, the node performs data synchronization to ensure its data integrity before becoming a leader.
	PREPARE

	// LEADER indicates that the node is currently acting as the leader in the Raft cluster.
	// Leaders are responsible for accepting log entries from clients, replicating log entries to all followers, and issuing heartbeats.
	LEADER
)

// SCALE defines the concurrency scale size, which should be set according to machine resources.
// The SCALE type helps in tuning the system for different levels of concurrent request handling.
type SCALE int

const (
	// SMALL indicates a small concurrency scale, suitable for machines with limited resources.
	// This setting is appropriate when the expected number of concurrent requests is up to 1000.
	SMALL SCALE = 1

	// MEDIUM indicates a medium concurrency scale, suitable for machines with moderate resources.
	// This setting is appropriate for environments expecting between 1000 and 10000 concurrent requests.
	MEDIUM SCALE = 2

	// LARGE indicates a large concurrency scale, suitable for machines with substantial resources.
	// This setting is designed for environments that anticipate more than 10000 concurrent requests.
	LARGE SCALE = 3
)

// LIMITMODE defines the behavior of the system when the RequestRate limit is reached.
type LIMITMODE byte

const (
	// LimitBlock indicates that the system will queue new requests until there's capacity to handle them.
	// In this mode, the system waits until existing requests are processed, allowing new ones to proceed.
	LimitBlock LIMITMODE = 0

	// LimitNonBlock indicates that the system will immediately return an error if the concurrency limit is exceeded.
	// In this mode, the system rejects new requests without waiting, providing faster feedback but potentially losing some requests.
	LimitNonBlock LIMITMODE = 1
)

type MTYPE byte

const (
	MEM_PUT    MTYPE = 1
	MEM_DEL    MTYPE = 2
	MEM_APPEND MTYPE = 3
	MEM_DELKV  MTYPE = 4
)

const (
	DEFAULTLIMITER = 4 << 10
)

// Config contains the configuration parameters for a Raft-based distributed system.
type Config struct {
	// TlsConfig is the TLS configuration used for securing communications between nodes.
	// This is essential for ensuring data integrity and confidentiality in transit.
	TlsConfig *tls.Config

	// LogStorage is an interface that handles the persistence of log entries for the Raft algorithm.
	// It ensures that all committed log entries are durably stored for recovery purposes.
	LogStorage stub.LogStorage

	// SnapshotMgr manages snapshots of the system state, allowing for faster recovery and state transfer.
	// Snapshots help reduce the amount of log data that needs to be replayed during startup or leader changes.
	SnapshotMgr stub.SnapshotManager

	// Persistent provides a persistent storage interface that supports idempotent operations.
	// This component is responsible for storing key-value pairs that represent the application's state.
	Persistent stub.Persistent

	// StateMachine is the application-specific state machine that applies committed log entries.
	// It defines how commands are executed and affects the overall state of the system.
	StateMachine stub.StateMachine

	// ElectionInterval specifies the time duration after which a follower will become a candidate if it does not receive any communications from the leader.
	// This timeout is crucial for detecting leader failures and initiating new elections.
	ElectionInterval time.Duration

	// HeartbeatInterval is the interval at which leaders send heartbeats to followers to maintain leadership.
	// Regular heartbeats prevent unnecessary elections and ensure cluster stability.
	HeartbeatInterval time.Duration

	// WaitTimeout specifies how long a node should wait before timing out during certain operations.
	// This includes waiting for a quorum of votes during elections or acknowledgments during command replication.
	WaitTimeout time.Duration

	// ClientTimeout defines the maximum time to wait for a client request to complete.
	// This prevents clients from waiting indefinitely when the system is under heavy load or experiencing issues.
	ClientTimeout time.Duration

	// PeerAddr contains the addresses of all cluster nodes for inter-node communication.
	// These addresses are used for establishing connections between peers and for exchanging messages necessary for the consensus protocol.
	PeerAddr []string

	// ListenAddr is the address on which this node listens for incoming connections from other nodes in the Raft cluster.
	// This setting is crucial for peer-to-peer communication within the cluster.
	ListenAddr string

	// Scale represents the concurrency scale size based on machine resources.
	//
	// The SCALE type helps in tuning the system for different levels of concurrent request handling:
	//
	// - SMALL: up to 1000 concurrent requests
	// - MEDIUM: between 1000 and 10000 concurrent requests
	// - LARGE: more than 10000 concurrent requests
	Scale SCALE

	// LimitRate is the upper limit on the number of concurrent requests this node can handle.
	// Once this limit is reached, the behavior depends on the setting of LimitMode.
	LimitRate int

	// LimitMode determines the behavior when the LimitRate limit is reached:
	// - LIMITMODE_BLOCKING: waits until there's capacity to handle new requests.
	// - LIMITMODE_NONBLOCKING: immediately returns an error if the limit is exceeded.
	LimitMode LIMITMODE

	// BlockVoteTime prevent the time of election as leader
	// the unit is nanosecond
	BlockVoteTime int64

	// MemExpiredTime Mem data expiration time
	// the unit is second
	MemExpiredTime int64

	// MemLogEntryLimit capacity upper limit of the log container
	MemLogEntryLimit int64

	// StandAlone Flag indicating if the node operates in standalone mode.
	StandAlone bool

	//COMMITMODE defines the type for modes in which Raft algorithm commits log entries to the state machine.
	CommitMode COMMITMODE

	MEMSYNC bool

	RAFTXSYNC bool
}

// Metrics contains monitoring information for a Raft-based distributed system.
// 表示当前领导者节点自成为领导者以来的持续时间（以毫秒为单位）。这个度量可以帮助我们了解领导者的稳定性以及集群中领导权切换的频率。
type Metrics struct {
	// LeaderUptime is the duration in milliseconds that the current leader has been active.
	// This metric can be used to monitor the stability of leadership within the cluster.
	// 表示当前领导者节点自成为领导者以来的持续时间（以毫秒为单位）。这个度量可以帮助我们了解领导者的稳定性以及集群中领导权切换的频率。
	LeaderUptime int64

	// ProposalCount is the total number of proposals (log entries) that have been submitted to the Raft cluster.
	// This includes both successful and failed proposals.
	// 表表示提交给 Raft 集群的提案总数（即日志条目）。这包括了成功和失败的提案。通过监控这个度量，可以了解系统的活跃程度和处理能力
	ProposalCount int64

	// ProposalFailed is the count of proposals that failed to commit due to various reasons such as network issues or conflicts.
	// Monitoring this metric can help identify potential problems with proposal processing.
	// 表示未能提交成功的提案数量。这可能是由于网络问题、冲突或其他原因导致的。监控此度量有助于识别提案处理过程中的潜在问题。
	ProposalFailed int64

	// MemProposalCount is the total number of proposals in mem that have been submitted to the Raft cluster.
	// This includes both successful and failed proposals.
	// 表表示提交给 Raft集群的易失性提案总数。这包括了成功和失败的提案。通过监控这个度量，可以了解系统的活跃程度和处理能力
	MemProposalCount int64

	// MemProposalFailed is the count of proposals that failed to commit due to various reasons such as network issues or conflicts.
	// Monitoring this metric can help identify potential problems with proposal processing.
	// 表示未能提交成功的提案数量。这可能是由于网络问题、冲突或其他原因导致的。监控此度量有助于识别提案处理过程中的潜在问题。
	MemProposalFailed int64

	// FollowerLatency is a map containing the latency of each follower in the cluster.
	// The key is the follower's ID (or another unique identifier), and the value is the latency in nanoseconds.
	// High latencies may indicate network issues or performance bottlenecks on specific nodes.
	// 记录了每个跟随者节点的延迟情况。键是跟随者的唯一标识符（例如节点ID），值是延迟时间（以纳秒为单位）。高延迟可能指示网络问题或特定节点上的性能瓶颈
	FollowerLatency map[int64]int64

	// LostNodes the node cannot communicate with the leader node
	// leader 无法通讯的其它集群节点
	LostNodes map[int64]string

	//metricsBat
	MetricsBat *MetricsBat

	//
	MetricsMem *MetricsMem
}

// WatchType defines the types of changes that can be watched on a key.
// 监听类型定义了可以监听的键的变化类型。
type WatchType byte

// Constants representing different types of watch events.
// 表示不同监听事件类型的常量。
const (
	// ADD indicates that a new key-value pair has been added.
	// 新增表示一个新的键值对被添加。
	ADD WatchType = 1

	// DELETE indicates that a key-value pair has been deleted.
	// 删除表示一个键值对被删除。
	DELETE WatchType = 2

	// UPDATE indicates that an existing key's value has been modified.
	// 更新表示一个已存在的键的值被修改。
	UPDATE WatchType = 3
)

// COMMITMODE defines the type for modes in which Raft algorithm commits log entries to the state machine.
type COMMITMODE byte

const (
	// ORDEREDPOLL represents the ordered polling mode. In this mode,
	// log entries are committed to the state machine in the exact order they were appended to the Raft log.
	// This ensures that all state machines on every node process operations in the same sequence,
	// thereby maintaining strong consistency across the distributed system.
	ORDEREDPOLL COMMITMODE = 1

	// UNORDEREDMVCC represents the unordered multiversion concurrency control (MVCC) mode.
	// In this mode, log entries may be committed out of the order they appear in the Raft log,
	// instead relying on MVCC mechanisms to manage different versions of data, allowing for concurrent read and write operations.
	// This increases system throughput and response speed but may sacrifice some ordering guarantees.
	// Applications must be designed to handle concurrency and data versioning correctly when using this mode.
	UNORDEREDMVCC COMMITMODE = 2
)

type MetricsBat struct {
	RxExecCustor        int64
	RxMax               int64
	RxTotal             int64
	LogMax              int64
	RollbackIncrement   int64
	RollbacCustor       int64
	ToRollbackIncrement int64
	ToRollbacCustor     int64
	ToRollbacMax        int64
}

func (b *MetricsBat) String() string {
	return fmt.Sprintf("RxExecCustor:%d,RxMax:%d,RxTotal:%d,LogMax:%d,RollbackIncrement:%d,RollbacCustor:%d,ToRollbackIncrement:%d,ToRollbacCustor:%d,ToRollbacMax:%d",
		b.RxExecCustor, b.RxMax, b.RxTotal, b.LogMax, b.RollbackIncrement, b.RollbacCustor, b.ToRollbackIncrement, b.ToRollbacCustor, b.ToRollbacMax)
}

type MetricsMem struct {
	Cursor    int64
	MaxId     int64
	FsmLength int64
}

func (ms *MetricsMem) String() string {
	return fmt.Sprintf("cursor:%d,maxId:%d,fsm-len:%d", ms.Cursor, ms.MaxId, ms.FsmLength)
}
