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

package stub

// StateMachine is the interface for Raft's state machine, responsible for applying log entries.
type StateMachine interface {
	// Apply applies a log entry to the state machine and returns any error that occurs.
	Apply(rxId int64, command []byte) error

	// ApplyMc applies an MvccCell to the state machine (likely for Multi-Version Concurrency Control support).
	ApplyMc(mc *MvccCell) error

	// Get retrieves the value associated with the specified key from the state machine.
	Get(key []byte) ([]byte, error)

	// GetList retrieves a list of values for multiple keys, each result containing a key-value pair.
	GetList(keys [][]byte) ([][2][]byte, error)

	// Rollback rolls back the operation identified by rxId.
	Rollback(rxId int64) error

	// HasRx has rxId exist
	HasRx(rxId int64) bool

	// Snapshot creates a snapshot of the state machine for a range of log entries, from fromRxId to toRxId.
	Snapshot(fromRxId, toRxId int64) (*MvccBean, error)

	// GetByRx retrieves an MvccCell associated with the given rxId from the state machine.
	GetByRx(rxId int64) (*MvccCell, error)
}

// LogStorage is the interface for Raft's log storage, handling the persistence of log entries.
type LogStorage interface {
	// Append saves a batch of log entries to the log storage. The log entries are serialized into a byte slice.
	Append(logId int64, LogEntryBatBinary []byte) error

	// Get retrieves a log entry at the specified index.
	Get(logId int64) ([]byte, error)

	// Del removes a log entry at the specified index.
	Del(logId int64) error

	// BatchDel removes multiple log entries specified by their indices.
	BatchDel(logIds []int64) error
}

// SnapshotManager is the interface for managing snapshots of the system state.
type SnapshotManager interface {
	// SaveSnapshot saves a snapshot as a file and returns the file index.
	SaveSnapshot(snapshotData []byte) (string, error)

	// CreateSnapshotAndSave generates snapshot data and saves it using a stream writer function.
	CreateSnapshotAndSave(logIndex int, fileIndex string, writeStream func([]byte) error) error

	// RestoreSnapshot retrieves snapshot data and restores the system state from the snapshot.
	RestoreSnapshot(fileIndex string) ([]byte, error)
}

// Persistent is the interface for persistent storage, which natively supports idempotent operations.
type Persistent interface {
	// Put stores a key-value pair in the persistent storage.
	Put(key, value []byte) error

	// Get retrieves the value associated with the specified key from the persistent storage.
	Get(key []byte) ([]byte, error)

	// Del removes the key and its associated value from the persistent storage.
	Del(key []byte) error

	// Has checks if the persistent storage contains the specified key.
	Has(key []byte) bool

	// BatchPut stores multiple key-value pairs in the persistent storage in a single operation.
	BatchPut(kv [][2][]byte) error //[][2][]byte{ks,vs}

	// BatchDel removes multiple keys and their associated values from the persistent storage in a single operation.
	BatchDel(kv [][]byte) error //[][]byte{ks}
}
