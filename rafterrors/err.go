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

package rafterrors

import (
	"errors"
	"fmt"
)

// RaftError represents a custom error type for the Raft consensus algorithm.
type RaftError struct {
	Code    int    // Error code
	Message string // Error message description
}

func (e *RaftError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// Communication errors (01XXXX)
// 通信错误 (01XXXX)
var (
	// ErrNetworkTimeout indicates a network timeout occurred during communication,
	// such as when sending requests or receiving responses.
	// 网络超时，例如请求发送或接收响应超时
	ErrNetworkTimeout = &RaftError{Code: 10100, Message: "Network timeout occurred during communication"}

	// ErrConnectionFailed indicates failure to establish a connection with the target node,
	// possibly due to the node being offline or other network issues.
	// 无法连接到目标节点，例如节点掉线
	ErrConnectionFailed = &RaftError{Code: 10101, Message: "Failed to establish connection to target node"}

	// ErrInvalidRequestFormat indicates that an invalid request format was received,
	// such as missing required fields or unexpected format.
	// 收到非法的请求格式，例如缺少必要字段
	ErrInvalidRequestFormat = &RaftError{Code: 10102, Message: "Invalid request format received"}

	// ErrResponseParsingError indicates an error occurred while parsing the response from a peer,
	// possibly due to mismatched data formats or corrupted data.
	// 响应解析失败，例如数据格式不匹配或损坏
	ErrResponseParsingError = &RaftError{Code: 10103, Message: "Error occurred while parsing response from peer"}

	// ErrNodeNotInCluster indicates that the node is no longer part of the cluster and thus cannot participate in communication.
	// 节点已被移出集群，无法参与通信
	ErrNodeNotInCluster = &RaftError{Code: 10104, Message: "Node is no longer part of the cluster, communication failed"}

	// ErrNodeListenerFailed indicates that the node failed to start its listener service,
	// preventing it from accepting incoming connections. This could be due to port conflicts or insufficient permissions.
	// 节点启动监听服务失败，例如端口被占用或权限不足
	ErrNodeListenerFailed = &RaftError{Code: 10105, Message: "Node failed to start listener, unable to accept incoming connections"}

	// ErrNodeStandAlone  Node is in single-node mode. Cluster related operations cannot be performed
	ErrNodeStandAlone = &RaftError{Code: 10106, Message: "Node is in single-node mode. Cluster related operations cannot be performed"}
)

// Log errors (02XXXX)
// 日志错误 (02XXXX)
var (
	// ErrLogIndexMismatch indicates a log index mismatch detected between the Leader and Follower,
	// which can lead to log synchronization failures.
	// 日志索引不匹配，例如 Leader 和 Follower 不一致
	ErrLogIndexMismatch = &RaftError{Code: 20100, Message: "Log index mismatch detected between Leader and Follower"}

	// ErrLogEntryMissing indicates that a required log entry is missing or outdated,
	// which can prevent log replication and application.
	//  日志条目丢失，例如指定的日志不存在
	ErrLogEntryMissing = &RaftError{Code: 20101, Message: "Required log entry is missing or outdated"}

	// ErrLogWriteFailure indicates a failure to write a log entry to persistent storage,
	// possibly due to disk issues or other storage-related problems.
	// 日志写入失败，例如磁盘问题
	ErrLogWriteFailure = &RaftError{Code: 20102, Message: "Failed to write log entry to persistent storage"}

	// ErrLogEntryNotFound indicates that a log entry was not found at the specified index,
	// typically occurring when attempting to access a non-existent log entry.
	// 未找到指定的日志条目
	ErrLogEntryNotFound = &RaftError{Code: 20103, Message: "Log entry not found at the specified index"}

	// ErrLogChecksumError indicates a log checksum validation failure,
	// suggesting potential data integrity issues.
	// 日志校验和验证失败
	ErrLogChecksumError = &RaftError{Code: 20104, Message: "Log checksum validation failed, data integrity issue"}
)

// Voting and election errors (03XXXX)
// 投票与选举错误 (03XXXX)
var (
	// ErrTermTooLow indicates that the candidate's term is lower than the current term,
	// so the vote is rejected.
	// 选人任期较低，无法获得投票
	ErrTermTooLow = &RaftError{Code: 30100, Message: "Candidate's term is lower than current term, vote rejected"}

	// ErrAlreadyVoted indicates that a vote has already been granted to another candidate in the same term.
	// 已经投票给其他候选人
	ErrAlreadyVoted = &RaftError{Code: 30101, Message: "Vote already granted to another candidate in this term"}

	// ErrTermOutOfDate indicates that an outdated term was received in a vote request.
	// 投票请求任期低于当前任期
	ErrTermOutOfDate = &RaftError{Code: 30102, Message: "Received outdated term in vote request"}

	// ErrNodeNotEligible indicates that the node is not eligible to participate in voting.
	// 节点不符合投票资格
	ErrNodeNotEligible = &RaftError{Code: 30103, Message: "Node is not eligible to participate in voting"}
)

// Cluster operation errors (04XXXX)
// 集群操作错误 (04XXXX)
var (
	// ErrLeaderNotFound indicates that the leader was not found,
	// possibly because the cluster is in a transitional state like during an election.
	// 未能找到 Leader，例如选举尚未完
	ErrLeaderNotFound = &RaftError{Code: 40100, Message: "Leader not found, cluster is in transitional state"}

	// ErrProposalSyncFailed indicates a failure to replicate a proposal to a majority of nodes.
	// 提案同步集群失败，例如未能将日志条目复制到大多数节点
	ErrProposalSyncFailed = &RaftError{Code: 40101, Message: "Failed to replicate proposal to a majority of nodes"}

	// ErrClusterSyncTimeout indicates that the cluster synchronization timed out during data replication.
	// 集群同步数据超时，例如在数据复制或状态同步过程中出现超时
	ErrClusterSyncTimeout = &RaftError{Code: 40102, Message: "Cluster synchronization timed out during data replication"}

	// ErrProxyFailed indicates that the proxy failed to forward data.
	// 代理转发数据失败
	ErrProxyFailed = &RaftError{Code: 40103, Message: "The proxy failed to forward data"}

	// ErrProxyTimeOutFailed indicates that the proxy failed to forward data within the expected time frame.
	//  代理转发反馈超时
	ErrProxyTimeOutFailed = &RaftError{Code: 40104, Message: "The proxy failed to forward data within the expected time frame"}
)

// State machine errors (05XXXX)
// 状态机错误 (05XXXX)
var (
	// ErrStateMachineUpdateFailed indicates a failure to update the state machine due to a conflict.
	// 状态机更新失败，例如业务逻辑冲突
	ErrStateMachineUpdateFailed = &RaftError{Code: 50100, Message: "Failed to update state machine due to conflict"}

	// ErrApplyLogConflict indicates a conflict detected while applying a log entry.
	// 应用日志时发生数据冲突
	ErrApplyLogConflict = &RaftError{Code: 50101, Message: "Conflict detected while applying log entry"}

	// ErrSnapshotLoadFailed indicates a failure to load a snapshot into the state machine.
	// 状态机快照加载失败
	ErrSnapshotLoadFailed = &RaftError{Code: 50102, Message: "Failed to load snapshot into state machine"}

	// ErrRollbackFailed indicates a failure to rollback the state machine to a previous state.
	// 状态回滚失败
	ErrRollbackFailed = &RaftError{Code: 50103, Message: "Failed to rollback state machine to previous state"}

	// ErrPersistenceUpdateFailed indicates a failure to update the persistent state in storage,
	// possibly due to disk write errors or unavailable storage devices.
	// 状态机持久化更新失败，例如磁盘写入错误或存储设备不可用
	ErrPersistenceUpdateFailed = &RaftError{Code: 50104, Message: "Failed to update persistent state in storage"}

	// ErrRepeatedRxId  repeatedly transaction Id
	// 重复提交事务Id
	ErrRepeatedRxId = &RaftError{Code: 50104, Message: "The transaction ID was submitted repeatedly"}
)

// Configuration change errors (06XXXX)
// 配置变更错误 (06XXXX)
var (
	// ErrConfigChangeFailed indicates that a configuration change did not reach consensus.
	// 配置变更未能达成共识
	ErrConfigChangeFailed = &RaftError{Code: 60100, Message: "Configuration change did not reach consensus"}

	// ErrNodeNotFound indicates that the target node was not found for a configuration change.
	// 找不到指定的节点
	ErrNodeNotFound = &RaftError{Code: 60101, Message: "Target node not found for configuration change"}

	// ErrConfigChangeNotLeader indicates that a non-leader node attempted to initiate a configuration change.
	// 配置变更由非 Leader 节点发起
	ErrConfigChangeNotLeader = &RaftError{Code: 60102, Message: "Non-Leader node attempted to initiate configuration change"}

	// ErrConfigChangeConflict indicates that concurrent configuration changes caused a conflict.
	// 配置变更冲突
	ErrConfigChangeConflict = &RaftError{Code: 60103, Message: "Concurrent configuration changes caused a conflict"}
)

// Term and role errors (07XXXX)
// 任期与角色错误 (07XXXX)
var (
	// ErrTermTooSmall indicates that a request with a smaller term was received and rejected.
	// 请求任期小于当前任期
	ErrTermTooSmall = &RaftError{Code: 70100, Message: "Received request with a smaller term, rejected"}

	// ErrRoleNotLeader indicates that an operation requiring the Leader role was handled by a non-Leader node.
	// 非 Leader 节点处理了需要 Leader 的请求
	ErrRoleNotLeader = &RaftError{Code: 70101, Message: "Operation requires Leader role, but node is not Leader"}

	// ErrHeartbeatTimeout indicates that a heartbeat timeout occurred, triggering a re-election.
	// 心跳超时，触发选举
	ErrHeartbeatTimeout = &RaftError{Code: 70102, Message: "Heartbeat timeout occurred, triggering re-election"}

	// ErrFollowerDemotionFailed indicates that the node failed to demote itself to the Follower role.
	// 节点无法降级为 Follower
	ErrFollowerDemotionFailed = &RaftError{Code: 70103, Message: "Failed to demote node to Follower role"}
)

// Request errors (08XXXX)
// 请求 (08XXXX)
var (
	// ErrParameter indicates an error in the request parameters.
	// 参数错误
	ErrParameter = &RaftError{Code: 80100, Message: "Request parameter error"}

	// ErrTooManyRequests indicates that too many requests have been made, possibly exceeding rate limits.
	// 并发请求数过多
	ErrTooManyRequests = &RaftError{Code: 80101, Message: "Too many requests"}

	ErrAddExistAddress = &RaftError{Code: 80102, Message: "failed to add an existing address"}
)

// Parse extracts the error code and message from an error.
// If the error is nil, it returns 0 for the code and an empty string for the message.
// If the error is of type *RaftError, it extracts the code and message from it.
// Otherwise, it returns 0 for the code and the error's string representation as the message.
func Parse(err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	if re, ok := err.(*RaftError); ok {
		return re.Code, re.Message
	}
	return 0, err.Error()
}

// Eq compares two errors to determine if they are equal based on their underlying codes.
// If both errors are of type *RaftError, it compares their codes for equality.
// Otherwise, it uses the standard errors.Is function to check if err1 is equivalent to err2.
func Eq(err1, err2 error) bool {
	if re1, ok := err1.(*RaftError); ok {
		if re2, ok2 := err2.(*RaftError); ok2 {
			return re1.Code == re2.Code
		}
	}
	return errors.Is(err1, err2)
}
