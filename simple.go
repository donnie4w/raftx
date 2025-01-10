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
	"context"
	"github.com/donnie4w/raftx/core"
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/rafterrors"
	"golang.org/x/time/rate"
)

// SimpleRaftx implements the Raftx interface and serves as a concrete implementation of a Raftx node.
// It manages the core logic for consensus, including log replication, leader election, and snapshot management.
type SimpleRaftx struct {
	node    *core.Node    // The underlying Node that handles the core Raftx protocol logic.
	config  *raft.Config  // Configuration settings for the Raftx node.
	limiter *rate.Limiter // Rate limiter to control the frequency of operations.
}

// newSimpleRaftx initializes a new instance of SimpleRaftx with the provided configuration.
// It sets up the core node and configures rate limiting based on the configuration parameters.
func newSimpleRaftx(config *raft.Config) (r *SimpleRaftx) {
	r = &SimpleRaftx{
		node:   core.NewNode(config),
		config: config,
	}
	if r.config == nil {
		r.config = &raft.Config{}
	}
	lmt := raft.DEFAULTLIMITER
	if config.LimitRate > 0 {
		lmt = config.LimitRate
	}
	r.limiter = rate.NewLimiter(rate.Limit(lmt), lmt)
	return
}

// LastCommitTxId returns the ID of the last transaction that has been committed to the state machine.
func (r *SimpleRaftx) LastCommitTxId() int64 {
	return r.node.LastCommitTxId()
}

// LastTransactionId returns the ID of the latest transaction, whether it has been committed or not.
func (r *SimpleRaftx) LastTransactionId() int64 {
	return r.node.LastTransactionId()
}

// ReSetNodeId Reset the node Id number and return the previous ID number
func (r *SimpleRaftx) ReSetNodeId(id int64) (prev int64) {
	return r.node.SetId(id)
}

// GetMetrics retrieves monitoring metrics related to the current state and performance of the Raftx node.
// Note: This method currently returns nil as no specific metrics are implemented.
func (r *SimpleRaftx) GetMetrics() *raft.Metrics {
	return r.node.GetMetrics()
}

// limitCheck checks if an operation should be allowed based on the configured rate limiter.
// It supports different modes of rate limiting as defined in the configuration.
func (r *SimpleRaftx) limitCheck() error {
	switch r.config.LimitMode {
	case raft.LimitNonBlock:
		if !r.limiter.Allow() {
			return rafterrors.ErrTooManyRequests
		}
	default:
		if err := r.limiter.Wait(context.TODO()); err != nil {
			return rafterrors.ErrTooManyRequests
		}
	}
	return nil
}

// Command submits a new proposal for consensus to be applied to the state machine.
// It first checks the rate limiter before submitting the command.
func (r *SimpleRaftx) Command(cmd []byte) error {
	if err := r.limitCheck(); err != nil {
		return err
	}
	return r.node.Command(cmd)
}

// AddNode adds a new member node with the specified address to the Raftx cluster.
func (r *SimpleRaftx) AddNode(address string) error {
	return r.node.AddNode(address)
}

// RemoveNode removes a member node with the specified address from the Raftx cluster.
// Returns true if the removal was successful; otherwise, false.
func (r *SimpleRaftx) RemoveNode(address string) bool {
	return r.node.RemoveNode(address)
}

// GetNodes returns a map containing information about all nodes in the cluster, including their addresses and terms.
func (r *SimpleRaftx) GetNodes() map[string]int64 {
	return r.node.GetPeerInfo()
}

// TakeSnapshot captures a snapshot of the state machine within a given range of transactions, which can be used to truncate the log.
func (r *SimpleRaftx) TakeSnapshot(fromTransactionId, toTransactionId int64) ([]byte, error) {
	return r.node.TakeSnapshot(fromTransactionId, toTransactionId)
}

// GetTerm returns the current term of the Raftx node.
func (r *SimpleRaftx) GetTerm() int64 {
	return r.node.GetTerm()
}

// GetLeaderID returns the ID and term of the current leader in the cluster.
func (r *SimpleRaftx) GetLeaderID() (string, int64) {
	return r.node.GetLeaderId()
}

// GetState returns the current state of the Raftx node (Follower, Candidate, Leader).
func (r *SimpleRaftx) GetState() raft.STATE {
	return r.node.GetState()
}

// GetNodeTime returns two int64 values representing the node's start time and the current service time
func (r *SimpleRaftx) GetNodeTime() (startTime int64, serviceTime int64) {
	return r.node.GetNodeTime()
}

// RestoreSnapshot restores the state machine from a provided snapshot, allowing recovery without replaying all logs.
func (r *SimpleRaftx) RestoreSnapshot(data []byte) error {
	return r.node.RestoreSnapshot(data)
}

// Stop gracefully halts the Raftx node service, ensuring all operations are completed before shutdown.
func (r *SimpleRaftx) Stop() error {
	return r.node.Close()
}

// Open starts the Raftx node service, initializing any necessary resources and configurations.
func (r *SimpleRaftx) Open() error {
	return r.node.Open()
}

// GetValue retrieves a value from the state machine using the provided key.
// If the key does not exist, an empty byte slice and an error are returned.
func (r *SimpleRaftx) GetValue(key []byte) (value []byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetValue(key)
}

// GetLocalValue retrieves a value locally from the state machine using the provided key.
// This bypasses consensus and is intended for read-only operations.
func (r *SimpleRaftx) GetLocalValue(key []byte) (value []byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalValue(key)
}

// GetValueList retrieves a list of values corresponding to the provided keys from the state machine.
// If a key does not exist, it will not appear in the result.
func (r *SimpleRaftx) GetValueList(keys [][]byte) (result [][2][]byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetValueList(keys)
}

// GetLocalValueList retrieves a list of values corresponding to the provided keys locally from the state machine.
// This bypasses consensus and is intended for read-only operations.
func (r *SimpleRaftx) GetLocalValueList(keys [][]byte) (result [][2][]byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalValueList(keys)
}

// GetMemValue retrieves a value from the volatile memory storage using the provided key.
// This method accesses data that may not yet have been committed to the state machine.
func (r *SimpleRaftx) GetMemValue(key []byte) (value []byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetMemValue(key)
}

// GetLocalMemValue retrieves a value locally from the volatile memory storage using the provided key.
// This bypasses consensus and is intended for read-only operations on volatile data.
func (r *SimpleRaftx) GetLocalMemValue(key []byte) (value []byte) {
	if err := r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalMemValue(key)
}

// GetMemValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
// This method accesses data that may not yet have been committed to the state machine.
func (r *SimpleRaftx) GetMemValueList(keys [][]byte) (result [][2][]byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetMemValueList(keys)
}

// GetMemMultiValue retrieves a value from the volatile memory storage using the provided key.
// This method accesses data that may not yet have been committed to the state machine.
func (r *SimpleRaftx) GetMemMultiValue(key []byte) (value [][]byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetMemMultiValue(key)
}

// GetMultiValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
// This method accesses data that may not yet have been committed to the state machine.
func (r *SimpleRaftx) GetMultiValueList(keys [][]byte) (result [][2][]byte, err error) {
	if err = r.limitCheck(); err != nil {
		return
	}
	return r.node.GetMultiValueList(keys)
}

// GetLocalMemValueList retrieves a list of values corresponding to the provided keys locally from the volatile memory storage.
// This bypasses consensus and is intended for read-only operations on volatile data.
func (r *SimpleRaftx) GetLocalMemValueList(keys [][]byte) (result [][2][]byte) {
	if err := r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalMemValueList(keys)
}

// GetLocalMemMultiValue retrieves a list of values corresponding to the provided keys from the volatile memory storage.
func (r *SimpleRaftx) GetLocalMemMultiValue(key []byte) (value [][]byte) {
	if err := r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalMemMultiValue(key)
}

// GetLocalMemMultiValueList retrieves a list of values corresponding to the provided keys from the volatile memory storage.
func (r *SimpleRaftx) GetLocalMemMultiValueList(keys [][]byte) (result [][2][]byte) {
	if err := r.limitCheck(); err != nil {
		return
	}
	return r.node.GetLocalMemMultiValueList(keys)
}

// MemCommand applies a command to the volatile memory storage.
// This method does not go through the consensus process but directly modifies the in-memory state.
func (r *SimpleRaftx) MemCommand(key, value []byte, ttl uint64, ptype raft.MTYPE) (err error) {
	if err := r.limitCheck(); err != nil {
		return err
	}
	return r.node.MemCommand(key, value, ttl, ptype)
}

func (r *SimpleRaftx) MemLen() int64 {
	return r.node.MemLen()
}

func (r *SimpleRaftx) MemWatch(key []byte, watchFunc func(key, value []byte, watchType raft.WatchType), isSync bool, watchTypes ...raft.WatchType) {
	r.node.Watch(key, watchTypes, isSync, watchFunc)
}

func (r *SimpleRaftx) MemUnWatch(key []byte) {
	r.node.UnWatch(key)
}

func (r *SimpleRaftx) MemUnWatchWithType(key []byte, wt raft.WatchType) {
	r.node.UnWatchWithType(key, wt)
}

func (r *SimpleRaftx) Running() bool {
	return r.node.Running()
}

func (r *SimpleRaftx) WaitRun() error {
	return r.node.WaitRun()
}
