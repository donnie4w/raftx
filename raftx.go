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

package raftx

import (
	"github.com/donnie4w/raftx/raft"
)

// Raftx is an interface that defines the behavior of a node participating in a distributed consensus algorithm based on the Raftx protocol.
// It encapsulates operations for managing cluster membership, log replication, leader election, snapshotting, and state transitions.
type Raftx interface {
	// LastCommitTxId returns the ID of the last transaction that has been committed to the state machine.
	LastCommitTxId() int64

	// LastTransactionId returns the ID of the latest transaction, whether it has been committed or not.
	LastTransactionId() int64

	// GetMetrics retrieves monitoring metrics related to the current state and performance of the Raftx node.
	GetMetrics() *raft.Metrics

	// Command submits a new proposal for consensus to be applied to the state machine.
	Command(cmd []byte) error

	// AddNode adds a new member node with the specified address to the Raftx cluster.
	AddNode(address string) error

	// RemoveNode removes a member node with the specified address from the Raftx cluster.
	// Returns true if the removal was successful; otherwise, false.
	RemoveNode(address string) bool

	// GetNodes returns a map containing information about all nodes in the cluster, including their addresses and terms.
	GetNodes() map[string]int64

	// TakeSnapshot captures a snapshot of the state machine within a given range of transactions, which can be used to truncate the log.
	TakeSnapshot(fromTransactionId, toTransactionId int64) ([]byte, error)

	// GetTerm returns the current term of the Raftx node.
	GetTerm() int64

	// GetLeaderID returns the ID and term of the current leader in the cluster.
	GetLeaderID() (string, int64)

	// GetState returns the current state of the Raftx node (Follower, Candidate, Leader).
	GetState() raft.STATE

	// ReSetNodeId Reset the node ID number and return the previous ID number
	ReSetNodeId(id int64) int64

	// RestoreSnapshot restores the state machine from a provided snapshot, allowing recovery without replaying all logs.
	RestoreSnapshot(data []byte) error

	// Stop gracefully halts the Raftx node service, ensuring all operations are completed before shutdown.
	Stop() error

	// Open starts the Raftx node service, initializing any necessary resources and configurations.
	Open() error

	// GetNodeTime returns two int64 values representing the node's start time and the current service time.
	// The start time is the Unix timestamp indicating when the node was started.
	// The service time is the duration in seconds that the node has been running since its start time.
	//
	// Returns:
	//   - startTime: An int64 value representing the Unix timestamp of when the node was started.
	//   - serviceTime: An int64 value representing the number of seconds the node has been running.
	GetNodeTime() (startTime int64, serviceTime int64)

	// GetValue retrieves a value from the state machine using the provided key.
	// If the key does not exist, an empty byte slice and an error are returned.
	GetValue(key []byte) (value []byte, err error)

	// GetLocalValue retrieves a value locally from the state machine using the provided key.
	// This bypasses consensus and is intended for read-only operations.
	GetLocalValue(key []byte) (value []byte, err error)

	// GetValueList retrieves a list of values corresponding to the provided keys from the state machine.
	// If a key does not exist, it will not appear in the result.
	GetValueList(key [][]byte) (result [][2][]byte, err error)

	// GetLocalValueList retrieves a list of values corresponding to the provided keys locally from the state machine.
	// This bypasses consensus and is intended for read-only operations.
	GetLocalValueList(key [][]byte) (result [][2][]byte, err error)

	// GetMemValue retrieves a value from the volatile memory storage using the provided key.
	// This method accesses data that may not yet have been committed to the state machine.
	GetMemValue(key []byte) (value []byte, err error)

	// GetLocalMemValue retrieves a value locally from the volatile memory storage using the provided key.
	// This bypasses consensus and is intended for read-only operations on volatile data.
	GetLocalMemValue(key []byte) (value []byte)

	// GetMemValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
	// This method accesses data that may not yet have been committed to the state machine.
	GetMemValueList(key [][]byte) (result [][2][]byte, err error)

	// GetMemMultiValue retrieves a value from the volatile memory storage using the provided key.
	// This method accesses data that may not yet have been committed to the state machine.
	GetMemMultiValue(key []byte) (value [][]byte, err error)

	// GetMultiValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
	// This method accesses data that may not yet have been committed to the state machine.
	GetMultiValueList(keys [][]byte) (result [][2][]byte, err error)

	// GetLocalMemValueList retrieves a list of values corresponding to the provided keys locally from the volatile memory storage.
	// This bypasses consensus and is intended for read-only operations on volatile data.
	GetLocalMemValueList(key [][]byte) (result [][2][]byte)

	// GetLocalMemMultiValue retrieves a list of values corresponding to the provided keys from the volatile memory storage.
	GetLocalMemMultiValue(key []byte) (value [][]byte)

	//GetLocalMemMultiValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
	GetLocalMemMultiValueList(keys [][]byte) (result [][2][]byte)

	// MemCommand applies a command to the volatile memory storage.
	// This method does not go through the consensus process but directly modifies the in-memory state.
	MemCommand(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error)

	// MemLen returns the number of active MemBean items currently stored in fsm
	MemLen() int64

	// MemWatch listens for changes to the volatile data associated with a specified key.
	//
	// Parameters:
	//   - key: The key to watch, provided as a byte slice.
	//   - watchFunc: A callback function that gets invoked when changes are detected.
	//       - key: The changed key, passed as a byte slice to the callback function.
	//       - value: The latest value of the key, passed as a byte slice to the callback function.
	//       - watchType: Indicates the type of change (created, deleted, modified).
	//   - isFnSync: Specifies whether the callback function should be executed synchronously.
	//               If true, the callback is executed immediately upon an event; if false, it may execute asynchronously.
	//   - watchTypes: An optional variadic parameter list specifying particular types of changes to watch (e.g., only creation and deletion).
	//                 If not provided, defaults to watching all types of changes.
	//
	// 注意：当使用 MemWatch 时，请确保在不再需要监听时调用相应的取消监听方法 (如 MemUnWatch 或 MemUnWatchWithType) 以避免潜在的内存泄漏或其他性能问题。
	MemWatch(key []byte, watchFunc func(key, value []byte, watchType raft.WatchType), isFnSync bool, watchTypes ...raft.WatchType)

	// MemUnWatch removes all listeners for the specified key.
	//
	// Parameters:
	//   - key: The key to stop watching, provided as a byte slice.
	//
	// 注意：调用此方法后，对于该键的所有变化将不再触发任何回调函数。
	MemUnWatch(key []byte)

	// MemUnWatchWithType removes listeners for specific types of changes on the specified key.
	//
	// Parameters:
	//   - key: The key to stop watching, provided as a byte slice.
	//   - wt: The specific type of change to stop listening for.
	//
	// 注意：与 MemUnWatch 不同，此方法仅取消对指定类型变化的监听。如果要取消对所有类型变化的监听，
	//      应使用 MemUnWatch 或多次调用本方法针对每种变化类型。
	MemUnWatchWithType(key []byte, wt raft.WatchType)

	//Running returns whether the raftx service is running properly
	Running() bool

	//WaitRun wait for the raftx service until it is ready to run
	WaitRun() error
}

// NewRaftx is a factory function that initializes a new Raftx instance using the provided configuration.
func NewRaftx(config *raft.Config) Raftx {
	return newSimpleRaftx(config)
}
