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
	"context"
	"github.com/donnie4w/gofer/util"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/sys"
	"time"
)

// proxyCs is a synchronization proxy function for nodes, enabling the leader node to send
// log entries or states to follower nodes and wait for their responses to verify replication success.
//
// Parameters:
//   - req: A function that performs the request operation to each node, taking a syncId and a
//     node connection (csNet). It returns an error if the request fails.
//   - ack: A callback function that processes the node's response, accepting the response data
//     and returning a boolean to indicate if the response is valid.
//
// Returns:
//   - `true` if replication is successful (more than half the nodes respond successfully),
//     `false` otherwise (e.g., due to timeout or insufficient responses).
func (n *Node) proxyCs(req func(int64, csNet) error, ack func(any) bool) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responses := make(chan bool, n.peers.len()-1)
	defer func() {
		close(responses)
		responses = nil
	}()
	acklist := make([]any, 0, n.peers.len())
	for _, id := range n.peers.ids() {
		if id == n.id {
			continue
		}
		go func(id int64) {
			defer sys.Recoverable(nil)
			if conn, err := n.getAnSetPeersCnCsNet(id); err == nil {
				syncID := util.UUID64()
				if err = req(syncID, conn); err == nil {
					var a any
					if a, _, err = n.dataWait.WaitWithCancel(ctx, syncID, n.getWaitTime()); err == nil && responses != nil {
						acklist = append(acklist, a)
						to := time.After(10 * time.Millisecond)
						select {
						case responses <- ack(a):
						case <-to:
						}
					}
				}
			}
		}(id)
	}

	//n.peersCn.Range(func(k int64, c csNet) bool {
	//	if k == n.id {
	//		return true
	//	}
	//	go func(nodeId int64, conn csNet) {
	//		count++
	//		defer sys.Recoverable(nil)
	//		waitTimeout := time.After(n.waitTimeout)
	//	START:
	//		select {
	//		case <-ctx.Done():
	//			return
	//		case <-waitTimeout:
	//			return
	//		default:
	//			syncID := util.UUID64()
	//			if err := req(syncID, conn); err != nil {
	//				log.Error(err)
	//				if n.peersCn.Has(nodeId) {
	//					<-time.After(defaultRestartInterval)
	//					goto START
	//				}
	//			} else {
	//				if a, _, err := n.dataWait.WaitWithCancel(ctx, syncID, n.waitTimeout); err == nil && responses != nil {
	//					responses <- ack(a)
	//				}
	//			}
	//		}
	//	}(k, c)
	//	return true
	//})

	successCount := 0
	waitTimeout := time.After(n.waitTimeout)
	for {
		select {
		case res, ok := <-responses:
			if !ok {
				return successCount >= n.peers.len()/2, nil
			}
			if res {
				successCount++
			}
			if successCount >= n.peers.len()/2 {
				return true, nil
			}
		case <-waitTimeout:
			if len(acklist) >= n.peers.len()/2 {
				success := 0
				for _, a := range acklist {
					if ack(a) {
						success++
					}
				}
				if success >= n.peers.len()/2 {
					return true, nil
				}
			}
			log.Error("ErrProxyTimeOutFailed:", successCount, ",", n.peers.len())
			return false, rafterrors.ErrProxyTimeOutFailed
		}
	}
}

//func (n *Node) proxyPeers(req func(int64, csNet) error, ack func(any) bool) bool {
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	responses := make(chan bool, n.addrPeers.len()-1)
//	defer close(responses)
//
//	n.addrPeers.Range(func(k string, v int64) bool {
//		if v == n.id {
//			return true
//		}
//		go func(addr string) {
//			defer sys.Recoverable(nil)
//			shortCycletime := 500 * time.Millisecond
//			waitTimeout := time.After(n.waitTimeout)
//		START:
//			select {
//			case <-ctx.Done():
//				return
//			case <-waitTimeout:
//				return
//			default:
//				cn, err := n.getCsNet(addr)
//				if err != nil {
//					<-time.After(shortCycletime)
//					goto START
//				}
//				defer cn.Close()
//				syncID := util.UUID64()
//				if err := req(syncID, cn); err != nil {
//					if n.addrPeers.Has(addr) {
//						<-time.After(shortCycletime)
//						goto START
//					}
//				} else {
//					if a, err := n.dataWait.Wait(syncID, n.waitTimeout); err == nil {
//						responses <- ack(a)
//					}
//				}
//			}
//		}(k)
//		return true
//	})
//
//	successCount := 0
//	waitTimeout := time.After(n.waitTimeout)
//	for {
//		select {
//		case res, ok := <-responses:
//			if !ok {
//				return successCount > int(n.addrPeers.len()/2)
//			}
//			if res {
//				successCount++
//			}
//			if successCount > int(n.addrPeers.len()/2) {
//				return true
//			}
//		case <-waitTimeout:
//			return false
//		}
//	}
//
//	//wg.Wait()
//	return false
//}
