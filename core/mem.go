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
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/stub"
)

// mem is an interface that defines operations for managing and interacting with volatile data.
// It provides a set of methods to handle in-memory data, including string representation, state machine length queries,
// command execution, applying changes, ID management, key-value operations, log entry retrieval, watching keys, and obtaining metrics.
type mem interface {
	// String returns the string representation of the memory object.
	// This method is typically used for debugging or logging purposes.
	String() string

	// FsmLen returns the number of elements in the finite state machine (FSM).
	// This count can be used to evaluate the current size or complexity of the FSM.
	FsmLen() int64

	// Command executes a command on the memory object. It takes a key, value, TTL (time-to-live), and command type as parameters,
	// and returns any error that occurred during execution.
	// This method is used to add or modify data within the memory.
	Command(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error)

	// Apply applies a memory change to the current state machine.
	// mb is a MemBean object containing the change information.
	Apply(mb *stub.MemBean)

	// SetMaxId sets the maximum ID in the state machine.
	// This method is used to maintain the highest identifier allocated within the state machine.
	SetMaxId(id int64)

	// GetMaxId retrieves the maximum ID from the state machine.
	// Returns the currently allocated highest ID within the state machine.
	GetMaxId() int64

	// GetValue retrieves the value associated with the given key from memory.
	// If the key is found, it returns the corresponding value and true; otherwise, it returns nil and false.
	GetValue(key []byte) (value []byte, ok bool)

	// GetMultiValue retrieves the value associated with the given key from memory.
	// If the key is found, it returns the corresponding value and true; otherwise, it returns nil and false.
	GetMultiValue(key []byte) (r [][]byte, ok bool)

	// GetValueList retrieves a list of values for a batch of keys.
	// Each result is a two-dimensional slice containing the key and its corresponding value, even if some keys do not exist.
	GetValueList(keys [][]byte) (result [][2][]byte)

	// GetMultiValueList retrieves a list of values for a batch of keys.
	// Each result is a two-dimensional slice containing the key and its corresponding value, even if some keys do not exist.
	GetMultiValueList(keys [][]byte) (result [][2][]byte)

	// GetLogEntry retrieves the log entry associated with the given memory ID.
	// The log entry contains detailed information related to a specific memory change.
	GetLogEntry(memId int64) *stub.MemBean

	// Watch monitors a specified key and invokes the provided callback function when conditions are met.
	// watchTypes specifies the types of events to monitor, isSync determines whether monitoring is synchronous, and watchFunc is the function called when a monitored condition is met.
	Watch(key []byte, watchTypes []raft.WatchType, isSync bool, watchFunc func(key, value []byte, watchType raft.WatchType))

	// UnWatch stops monitoring a specified key.
	// Stops all watch operations associated with the given key.
	UnWatch(key []byte)

	// UnWatchWithType stops monitoring a specified key for a specific type of event.
	// Only stops watch operations associated with the given key and the specified event type.
	UnWatchWithType(key []byte, wt raft.WatchType)

	// GetMetricsMem retrieves memory-related metrics.
	// Returns an object containing information about memory usage and other performance indicators.
	GetMetricsMem() *raft.MetricsMem
}
