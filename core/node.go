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
	"crypto/tls"
	"fmt"
	"github.com/donnie4w/gofer/hashmap"
	"github.com/donnie4w/gofer/lock"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/raft"
	"github.com/donnie4w/raftx/stub"
	"github.com/donnie4w/raftx/sys"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Node represents a single node in a Raft cluster, encapsulating the core logic for consensus.
type Node struct {
	lock                 Lock                                            // Synchronization primitive for state transitions.
	mux                  sync.Mutex                                      // Mutex to protect concurrent access to certain fields.
	tlsConfig            *tls.Config                                     // TLS configuration for secure communication.
	metrics              raft.Metrics                                    // Contains monitoring information
	bat                  *Bat                                            // Batch processor for handling log entries and network messages.
	tServer              *tServer                                        // Transport server for handling incoming connections.
	dataWait             *lock.FastAwait[any]                            // Await structure for waiting on asynchronous operations.
	numLock              *lock.Numlock                                   // Numeric lock for managing concurrent access to numeric counters.
	listenAddr           string                                          // Address on which this node listens for incoming connections.
	votedFor             votedFor                                        // Information about the candidate voted for in the current term.
	logStorage           stub.LogStorage                                 // Storage system for client command log entries.
	snapshotMgr          stub.SnapshotManager                            // Manager for handling state snapshots.
	persistent           stub.Persistent                                 // Persistent storage for Raft's persistent state.
	applymsg             stub.StateMachine                               // Channel for applying committed logs to the state machine.
	peers                *peers                                          // Manages a collection of Raft cluster members
	hbcs                 *hashmap.MapL[int64, csNet]                     // Map of heartbeat channels for addrPeers.
	peersCn              *hashmap.MapL[int64, csNet]                     // Map of connection objects for addrPeers.
	conns                *hashmap.Map[int64, csNet]                      // Map of connection objects for addrPeers.
	leases               *hashmap.MapL[int64, int64]                     // Map of lease durations for addrPeers.
	logEntryBatPool      *hashmap.LimitHashMap[int64, *stub.LogEntryBat] // pool of LogEntryBat object.
	rxBatPool            *hashmap.LimitHashMap[int64, *stub.RxBat]       // pool of RxBat object.
	missPeer             *hashmap.MapL[string, int64]                    // Map of missing node
	electionInterval     time.Duration                                   // Duration between elections.
	heartbeatInterval    time.Duration                                   // Interval at which heartbeats are sent.
	waitTimeout          time.Duration                                   // Timeout for waiting on certain operations.
	clientTimeout        time.Duration                                   // Timeout for client operations.
	becomeId             atomic.Int64                                    // Atomic counter for state transitions.
	mem                  mem                                             // mem is a structure for volatile data
	memExpiredTime       int64                                           // Expiration time of volatile data
	memLogEntryLimit     int64                                           // volatile data log capacity
	memLogEntrySyncLimit int64                                           // volatile data log capacity
	heartbeatTime        int64                                           // Timestamp of the last heartbeat received.
	heartbeatLease       int64                                           // Lease duration for heartbeats.
	term                 int64                                           // Current term of the node.
	id                   int64                                           // Unique identifier for the node.
	leaderId             int64                                           // ID of the current leader.
	leaderTime           int64                                           // time for become leader
	leaderCn             csNet                                           // Connection object for the current leader.
	state                raft.STATE                                      // Current state of the node (Follower, Candidate, PrePare,  Leader).
	nodeClose            bool                                            // Flag indicating if the node is closed.
	standAlone           bool                                            // Flag indicating if the node operates in standalone mode.
	startTime            int64                                           // time of service start
	blockVoteTime        int64                                           // time for
	commitMode           raft.COMMITMODE
	cpurate              int
	chanWait             chan struct{}
	memSync              bool
	raftxSync            bool
}

// NewNode initializes a new instance of Node with the provided configuration.
func NewNode(config *raft.Config) *Node {
	n := &Node{tlsConfig: config.TlsConfig, logStorage: config.LogStorage, snapshotMgr: config.SnapshotMgr, persistent: config.Persistent, applymsg: config.StateMachine, electionInterval: config.ElectionInterval, heartbeatInterval: config.HeartbeatInterval, waitTimeout: config.WaitTimeout, clientTimeout: config.ClientTimeout, listenAddr: config.ListenAddr}
	n.peers = newPeers()
	n.hbcs = hashmap.NewMapL[int64, csNet]()
	n.peersCn = hashmap.NewMapL[int64, csNet]()
	n.leases = hashmap.NewMapL[int64, int64]()
	n.conns = hashmap.NewMap[int64, csNet]()
	n.missPeer = hashmap.NewMapL[string, int64]()
	n.votedFor = newVoteFor()
	n.standAlone = config.StandAlone
	n.commitMode = config.CommitMode
	n.memSync = config.MEMSYNC
	n.raftxSync = config.RAFTXSYNC
	n.dataWait = lock.NewFastAwait[any]()
	switch config.Scale {
	case raft.LARGE:
		n.rxBatPool = hashmap.NewLimitHashMap[int64, *stub.RxBat](1 << 17)
		n.logEntryBatPool = hashmap.NewLimitHashMap[int64, *stub.LogEntryBat](1 << 17)
		n.numLock = lock.NewNumLock(1 << 8)
	case raft.SMALL:
		n.rxBatPool = hashmap.NewLimitHashMap[int64, *stub.RxBat](1 << 13)
		n.logEntryBatPool = hashmap.NewLimitHashMap[int64, *stub.LogEntryBat](1 << 13)
		n.numLock = lock.NewNumLock(1 << 6)
	default:
		n.rxBatPool = hashmap.NewLimitHashMap[int64, *stub.RxBat](1 << 15)
		n.logEntryBatPool = hashmap.NewLimitHashMap[int64, *stub.LogEntryBat](1 << 15)
		n.numLock = lock.NewNumLock(1 << 7)
	}
	if n.persistent == nil {
		n.persistent = newMemoryPersistent()
	}
	if n.logStorage == nil {
		n.logStorage = newLogs(n)
	}
	if n.applymsg == nil {
		n.applymsg = newMemoryStateMachine()
	}
	if len(config.PeerAddr) > 0 {
		n.peers.init(config.PeerAddr)
	}
	if config.MemExpiredTime > 0 {
		n.memExpiredTime = config.MemExpiredTime
	} else {
		n.memExpiredTime = defaultMemExpiredTime
	}
	if config.MemLogEntryLimit > 0 {
		n.memLogEntryLimit = config.MemLogEntryLimit
	} else {
		n.memLogEntryLimit = defaultMemLogEntryLimit
	}
	if config.MemLogEntrySyncLimit > 0 {
		n.memLogEntrySyncLimit = config.MemLogEntrySyncLimit
	} else {
		n.memLogEntrySyncLimit = n.memLogEntryLimit / 5
	}

	n.blockVoteTime = config.BlockVoteTime
	n.startTime = time.Now().UnixNano()
	n.mem = newMem(n)
	return n
}

// server starts the transport server for handling incoming connections.
func (n *Node) server() (err error) {
	ch := make(chan error)
	n.tServer = newTServer(n)
	go n.tServer.serve(ch, n.listenAddr)
	err = <-ch
	return
}

func (n *Node) isClose() bool {
	return n.nodeClose
}

func (n *Node) SetId(id int64) (prev int64) {
	if n.id != 0 {
		prev = n.id
	}
	if id == 0 {
		return
	}
	if n.persistent.Put(KEYSYS.NodeID(), util.Int64ToBytes(id)) == nil {
		n.id = id
	}
	return
}

func (n *Node) isLeader() bool {
	return n.state == raft.LEADER || n.state == raft.PREPARE
}

func (n *Node) isPrepare() bool {
	return n.state == raft.PREPARE
}

func (n *Node) isFollower() bool {
	return n.state == raft.FOLLOWER
}

func (n *Node) isCandidate() bool {
	return n.state == raft.CANDIDATE
}

// init initializes the node's internal state and starts necessary goroutines.
func (n *Node) init() (err error) {
	defer sys.Recoverable(&err)
	if bs, err := n.persistent.Get(KEYSYS.NodeID()); err == nil && len(bs) > 0 {
		n.id = util.BytesToInt64(bs)
	} else {
		n.id = getUUID()
		n.persistent.Put(KEYSYS.NodeID(), util.Int64ToBytes(n.id))
	}

	if bs, err := n.persistent.Get(KEYSYS.Term()); err == nil && len(bs) > 0 {
		n.term = util.BytesToInt64(bs)
	}

	if n.waitTimeout <= 0 {
		n.waitTimeout = defaultWaitTimeout
	}

	if n.clientTimeout <= 0 {
		n.clientTimeout = defaultClientTimeout
	}

	if n.heartbeatInterval <= 0 {
		n.heartbeatInterval = defaultHeartbeatInterval
	}

	if n.electionInterval <= 0 {
		n.electionInterval = defaultElectionInterval
	}

	if n.commitMode != raft.ORDEREDPOLL && n.commitMode != raft.UNORDEREDMVCC {
		n.commitMode = raft.ORDEREDPOLL
	}

	n.bat = newBat(n)
	err = n.bat.init()
	if !n.standAlone {
		go n.becomeFollower(n.becomeId.Load())
		go n.initNet(true)
		go n.hbTicker()
		go n.checkTicker()
		go n.eleTicker()
	} else {
		n.becomeLeader(n.becomeId.Load(), n.term)
	}
	return
}

// startElection initiates an election if the node is a follower and not already closed.
func (n *Node) startElection() {
	if !n.nodeClose && n.isFollower() {
		n.becomeCandidate(n.becomeId.Load())
	}
}

// initNet initializes network connections to addrPeers, retrying if necessary.
func (n *Node) initNet(redo bool) {
START:
	for k, v := range n.peers.addrMapClone() {
		if v == 0 {
			n.nodeInfoAction(k)
		}
	}
	if redo && n.peers.idLen() < n.peers.len()/2 {
		<-time.After(defaultRestartInterval)
		goto START
	}
}

// becomeFollower transitions the node to the follower state.
func (n *Node) becomeFollower(bid int64) {
	if !n.lock.FollowerTl.TryLock() {
		return
	}
	defer n.lock.FollowerTl.Unlock()
	if n.becomeId.Load() > bid {
		return
	}
	n.becomeId.Add(1)
	log.Debug("becomeFollower:", n.id, ",term:", n.term)
	n.state = raft.FOLLOWER
	n.clearCsnet()
}

// becomeLeader transitions the node to the leader state.
func (n *Node) becomeLeader(bid, oterm int64) {
	if !n.lock.LeaderTl.TryLock() {
		return
	}
	defer n.lock.LeaderTl.Unlock()
	if n.becomeId.Load() > bid {
		return
	}
	n.becomeId.Add(1)

	if n.leaderId != 0 {
		return
	}
	n.setTerm(oterm + 1)
	log.Debug("becomeLeader:", n.listenAddr, ",id:", n.id, ",term:", n.term)
	n.waitDone()
	if !n.standAlone {
		n.leaderId = n.id
		n.leaderTime = time.Now().UnixMilli()
		n.state = raft.PREPARE
		go n.heartbeatsAction(true)
		n.rxSyncAction(100)
	}
	n.state = raft.LEADER
}

// becomeCandidate transitions the node to the candidate state.
func (n *Node) becomeCandidate(bid int64) {
	if !n.lock.CandidateTl.TryLock() {
		return
	}
	defer n.lock.CandidateTl.Unlock()
	if n.becomeId.Load() > bid {
		return
	}
	n.becomeId.Add(1)
	log.Debug("becomeCandidate:", n.id, ",term:", n.term)
	if n.leaderId != 0 && n.leaderId != n.id {
		if n.detectLeader() {
			return
		}
	}
	if n.peers.idLen() <= n.peers.len()/2 {
		return
	}
	if n.leases.Len() > 0 {
		n.leases.Range(func(k int64, v int64) bool {
			n.leases.Del(k)
			return true
		})
	}

	if n.votedFor.Vote != nil && n.term > n.votedFor.Vote.Term {
		n.votedFor = newVoteFor()
	}
	n.leaderId = 0
	n.heartbeatLease = 0
	n.state = raft.CANDIDATE

	if !n.standAlone {
		if n.isBlock() {
			n.becomeFollower(n.becomeId.Load())
			return
		}
		n.voteAction()
	} else {
		n.becomeLeader(n.becomeId.Load(), n.term)
	}
}

func (n *Node) isBlock() bool {
	return time.Now().UnixNano()-n.startTime <= n.blockVoteTime
}

func (n *Node) clearCsnet() {
	n.lock.PeercsMux.Lock()
	defer n.lock.PeercsMux.Unlock()
	if n.leaderCn != nil {
		n.leaderCn.Close()
		n.leaderCn = nil
	}
	n.hbcs.Range(func(k int64, c csNet) bool {
		c.Close()
		n.hbcs.Del(k)
		return true
	})
	n.peersCn.Range(func(k int64, c csNet) bool {
		c.Close()
		n.peersCn.Del(k)
		return true
	})

	n.conns.Range(func(k int64, v csNet) bool {
		v.Close()
		n.conns.Del(k)
		return true
	})
}

func (n *Node) removeCsNet(id int64) {
	if n.leaderCn != nil && n.leaderCn.Id() == id {
		n.leaderCn = nil
	}

	n.hbcs.Range(func(k int64, v csNet) bool {
		if id == v.Id() {
			n.hbcs.Del(k)
			return false
		}
		return true
	})

	n.peersCn.Range(func(k int64, v csNet) bool {
		if id == v.Id() {
			n.peersCn.Del(k)
			return false
		}
		return true
	})

	n.conns.Del(id)
}

// Open initializes the node and starts its operation.
func (n *Node) Open() (err error) {
	if n.standAlone {
		err = n.init()
	} else if err = n.server(); err == nil {
		err = n.init()
	}
	if err != nil {
		n.Close()
	}
	return err
}

func (n *Node) getRxIdMax() int64 {
	return n.bat.RxMax.Load()
}

func (n *Node) setTerm(term int64) {
	n.mux.Lock()
	defer n.mux.Unlock()
	if n.term < term {
		n.persistent.Put(KEYSYS.Term(), util.Int64ToBytes(term))
		n.term = term
	}
}

func (n *Node) detectLeader() bool {
	if n.isLeader() {
		return true
	}
	log.Debug("detectLeader:", n.leaderId, ",listen:", n.listenAddr)
	if cn, err := n.getAndSetLeaderCsNet(); err == nil {
		syncId := util.UUID64()
		if cn.Ping(syncId, &stub.Ping{Detect: 1, Term: n.term}) == nil {
			if ack, err := n.dataWait.Wait(syncId, n.electionInterval); err == nil {
				pong := ack.(*stub.Pong)
				if pong.Result == 1 {
					return true
				}
			}
		}
	}
	return false
}

func (n *Node) RemoveById(nodeId int64) (r bool) {
	if nodeId == n.id {
		n.Close()
		r = true
	} else {
		n.peers.delId(nodeId)
	}
	return
}

func (n *Node) RemoveByAddr(addr string) (r bool) {
	if addr == n.listenAddr {
		n.Close()
		r = true
	} else {
		n.peers.delAddr(addr)
	}
	return
}

// Close shuts down the node gracefully.
func (n *Node) Close() (err error) {
	defer sys.Recoverable(&err)
	n.mux.Lock()
	defer n.mux.Unlock()
	if !n.nodeClose {
		n.nodeClose = true
		err = n.tServer.close()
		n.clearCsnet()
	}
	return err
}

// hbTicker handles periodic heartbeat actions for the leader.
func (n *Node) hbTicker() {
	tk := time.NewTicker(n.heartbeatInterval)
	var count int64
	for !n.nodeClose {
		select {
		case <-tk.C:
			switch n.state {
			case raft.LEADER, raft.PREPARE:
				n.heartbeatsAction(false)
				if count%10 == 0 {
					go func() {
						<-time.After(n.heartbeatInterval / 2)
						n.heartbeatsAction(true)
					}()
				}
			}
		}
		count++
	}
}

// eleTicker handles periodic election checks for followers.
func (n *Node) eleTicker() {
	interval := n.heartbeatInterval + n.electionInterval
	tk := time.NewTicker(interval)
	for !n.nodeClose {
		go func() {
			if n.cpurate = sys.GetCPURate(interval); n.cpurate > 80 {
				log.Info("high cpu:", n.cpurate)
			}
		}()
		select {
		case <-tk.C:
			switch n.state {
			case raft.FOLLOWER:
				if n.heartbeatLease == 0 {
					n.heartbeatLease = rand.Int63n(100) * int64(time.Millisecond.Nanoseconds())
				}
				if time.Now().UnixNano()-n.heartbeatTime > interval.Nanoseconds()+n.heartbeatLease {
					n.startElection()
				}
			}
		}
	}
}

// checkTicker performs periodic checks and maintenance tasks.
func (n *Node) checkTicker() {
	tk := time.NewTicker(n.clientTimeout)
	<-time.After(10 * n.clientTimeout)
	for !n.nodeClose {
		select {
		case <-tk.C:
			n.rollback()
			n.toRollback()
			n.initNet(false)
		}
	}
}

func (n *Node) String() string {
	return fmt.Sprintf(`Node{
  ID: %d,
  State: %v,
  LeaderID: %d,
  Term: %d,
  ListenAddr: %s,
  VotedFor: %+v,
  ElectionInterval: %v,
  HeartbeatInterval: %v,
  WaitTimeout: %v,
  ClientTimeout: %v,
  BecomeId: %d,
  MemExpiredTime: %d,
  MemLogEntryLimit: %d,
  HeartbeatTime: %d,
  HeartbeatLease: %d,
  StartTime: %d,
  BlockVoteTime: %d,
  LeaderTime: %d,
  NodeClose: %v,
  StandAlone: %v,
}`,
		n.id, n.state, n.leaderId, n.term, n.listenAddr,
		n.votedFor,
		n.electionInterval, n.heartbeatInterval, n.waitTimeout, n.clientTimeout,
		n.becomeId.Load(), n.memExpiredTime, n.memLogEntryLimit,
		n.heartbeatTime, n.heartbeatLease, n.startTime, n.blockVoteTime, n.leaderTime,
		n.nodeClose, n.standAlone,
	)
}

func (n *Node) isRunStat() bool {
	if n.isLeader() {
		return n.peersCn.Len() > int64(n.peers.len()/2)
	}
	interval := n.heartbeatInterval.Nanoseconds() + n.electionInterval.Nanoseconds() + n.heartbeatLease
	return n.isFollower() && n.leaderId != 0 && time.Now().UnixNano()-n.heartbeatTime < interval
}

func (n *Node) updateHeartbeatTime() {
	n.heartbeatTime = time.Now().UnixNano()
	n.waitDone()
}

func (n *Node) getWaitTime() time.Duration {
	if n.cpurate > 90 {
		return 3 * time.Minute
	}
	if n.cpurate > 80 {
		return time.Minute
	}
	return n.waitTimeout
}

func (n *Node) waitRun() (err error) {
	defer sys.Recoverable(&err)
	if n.isRunStat() {
		return
	}
	n.mux.Lock()
	if n.chanWait == nil {
		n.chanWait = make(chan struct{})
	}
	n.mux.Unlock()
	<-n.chanWait
	return
}

func (n *Node) waitDone() {
	if n.chanWait == nil {
		return
	}
	defer sys.Recoverable(nil)
	if n.chanWait != nil {
		n.mux.Lock()
		defer n.mux.Unlock()
		close(n.chanWait)
		n.chanWait = nil
	}
}
