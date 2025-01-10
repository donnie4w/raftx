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
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/rafterrors"
	"github.com/donnie4w/raftx/sys"

	"github.com/donnie4w/tsf"
	"time"
)

type tServer struct {
	isClose   bool
	server    *tsf.Tsf
	localNode *Node
}

func newTServer(node *Node) *tServer {
	return &tServer{localNode: node}
}

func (t *tServer) serve(ch chan error, listenAddr string) (err error) {
	defer sys.Recoverable(&err)
	cfg := &tsf.TsfConfig{ListenAddr: listenAddr, TConfiguration: &tsf.TConfiguration{TLSConfig: t.localNode.tlsConfig, ProcessMerge: true}}
	tc := &tsf.TContext{}
	tc.OnClose = func(ts tsf.TsfSocket) error {
		defer sys.Recoverable(nil)
		log.Info("OnClose:", ts.ID())
		t.localNode.removeCsNet(ts.ID())
		return nil
	}
	tc.OnOpenSync = func(socket tsf.TsfSocket) error {
		cn := newTrans(socket)
		socket.SetContext(context.WithValue(context.Background(), true, newProcessor(t.localNode, cn, true)))
		t.localNode.conns.Put(socket.ID(), cn)
		return nil
	}
	tc.Handler = func(socket tsf.TsfSocket, packet *tsf.Packet) error {
		if t.localNode.nodeClose {
			socket.Close()
			return nil
		}
		return router(packet.ToBytes(), socket.GetContext().Value(true).(csNet))
	}
	chClose := false
	defer func() {
		if !chClose {
			ch <- err
		}
	}()
	if t.server, err = tsf.NewTsf(cfg, tc); err == nil {
		if err = t.server.Listen(); err == nil {
			log.Info("raftx service listen ", listenAddr)
			close(ch)
			chClose = true
			err = t.server.AcceptLoop()
		}
	}
	if err != nil {
		log.Error("failed to start listener:", err)
		err = rafterrors.ErrNodeListenerFailed
	}
	return
}

func (t *tServer) close() error {
	t.isClose = true
	return t.server.Close()
}

type connect struct {
	csNet
	localNode *Node
}

func newConnect(node *Node) *connect {
	return &connect{localNode: node}
}

func (c *connect) open(addr string) (err error) {
	defer sys.Recoverable(&err)
	if c.localNode.nodeClose {
		log.Debugf(`node service is close "%s"`, c.localNode.listenAddr)
		return rafterrors.ErrNodeNotInCluster
	}
	tx := &tsf.TContext{}
	wait := make(chan struct{})
	tx.OnOpenSync = func(socket tsf.TsfSocket) error {
		log.Debug("connect open:", c.localNode.listenAddr, "->", addr)
		defer close(wait)
		c.csNet = newTrans(socket)
		socket.SetContext(context.WithValue(context.Background(), true, newProcessor(c.localNode, c.csNet, false)))
		c.localNode.conns.Put(socket.ID(), c.csNet)
		return nil
	}
	tx.OnClose = func(ts tsf.TsfSocket) error {
		log.Info("OnClose:", ts.ID())
		defer sys.Recoverable(nil)
		defer c.Close()
		c.localNode.removeCsNet(ts.ID())
		return nil
	}
	tx.Handler = func(socket tsf.TsfSocket, packet *tsf.Packet) error {
		if c.localNode.nodeClose {
			socket.Close()
			return nil
		}
		return router(packet.ToBytes(), socket.GetContext().Value(true).(csNet))
	}
	conn := tsf.NewTsfSocketConf(addr, &tsf.TConfiguration{ProcessMerge: true, TLSConfig: c.localNode.tlsConfig, ConnectTimeout: c.localNode.waitTimeout})
	if err = conn.Open(); err == nil {
		go conn.On(tx)
	} else {
		err = rafterrors.ErrConnectionFailed
		return
	}
	to := time.After(c.localNode.heartbeatInterval + c.localNode.electionInterval)
	select {
	case <-wait:
	case <-to:
		conn.Close()
		log.Errorf("connect timeout: %s -> %s ", c.localNode.listenAddr, addr)
		err = rafterrors.ErrNetworkTimeout
	}
	return
}
