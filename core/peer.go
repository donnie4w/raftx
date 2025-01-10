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
	"sync"
)

// peers manages a collection of Raft cluster members, associating their network addresses with unique IDs.
type peers struct {
	// addrPeers maps peer network addresses to their unique identifiers.
	addrPeers map[string]int64
	// idPeers maps peer unique identifiers back to their network addresses.
	idPeers map[int64]string
	// idshot is a slice holding all peer IDs for fast iteration over all peers.
	idshot []int64
	// mux is a read-write mutex ensuring thread-safe access to the peers structure.
	mux      sync.RWMutex
	addrMuxs map[string]*sync.Mutex
}

// newPeers creates and returns a new instance of peers with initialized maps.
func newPeers() *peers {
	return &peers{addrPeers: make(map[string]int64), idPeers: make(map[int64]string), addrMuxs: make(map[string]*sync.Mutex)}
}

// init initializes the peers with a list of addresses, setting their associated IDs to 0.
func (p *peers) init(addrs []string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	for _, addr := range addrs {
		p.addrPeers[addr] = 0 // Initialize with ID 0 or any default value as appropriate.
		p.addrMuxs[addr] = &sync.Mutex{}
	}
}

// len returns the number of peers currently managed.
func (p *peers) len() int {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return len(p.addrPeers)
}

// hasAddr checks if a peer with the specified address exists in the peers set.
func (p *peers) hasAddr(addr string) bool {
	p.mux.RLock()
	defer p.mux.RUnlock()
	_, ok := p.addrPeers[addr]
	return ok
}

// add adds a new peer address to the peers set. Returns false if the address already exists.
func (p *peers) add(addr string) bool {
	p.mux.Lock()
	defer p.mux.Unlock()
	if _, ok := p.addrPeers[addr]; ok {
		return false
	}
	p.addrPeers[addr] = 0 // Assign a default ID or handle ID assignment accordingly.
	p.addrMuxs[addr] = &sync.Mutex{}
	return true
}

// idLen returns the number of peers indexed by ID.
func (p *peers) idLen() int {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return len(p.idPeers)
}

// getPeerInfo returns a copy of the mapping between peer addresses and their IDs.
func (p *peers) getPeerInfo() map[string]int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	m := make(map[string]int64)
	for k, v := range p.addrPeers {
		m[k] = v
	}
	return m
}

// getAddr retrieves the network address for a given peer ID.
func (p *peers) getAddr(id int64) (addr string, ok bool) {
	p.mux.RLock()
	defer p.mux.RUnlock()
	addr, ok = p.idPeers[id]
	return
}

// getId retrieves the ID for a given peer address.
func (p *peers) getId(addr string) (id int64, ok bool) {
	p.mux.RLock()
	defer p.mux.RUnlock()
	id, ok = p.addrPeers[addr]
	return
}

// setId sets or updates the ID for a given peer address. If the ID already exists for another address, it removes the old association.
func (p *peers) setId(addr string, id int64) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if prev, ok := p.idPeers[id]; ok && prev != addr {
		delete(p.addrPeers, prev)
	}
	p.addrPeers[addr] = id
	p.idPeers[id] = addr
	p.resetIdshot()
}

// delAddr removes the peer record for the given address from both mappings.
func (p *peers) delAddr(addr string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	id := p.addrPeers[addr]
	delete(p.idPeers, id)
	delete(p.addrPeers, addr)
	delete(p.addrMuxs, addr)
	p.resetIdshot()
}

// resetIdshot rebuilds the idshot slice containing all current peer IDs.
func (p *peers) resetIdshot() {
	p.idshot = make([]int64, 0, len(p.idPeers))
	for k := range p.idPeers {
		p.idshot = append(p.idshot, k)
	}
}

// delId removes the peer record for the given ID from both mappings.
func (p *peers) delId(id int64) {
	p.mux.Lock()
	defer p.mux.Unlock()
	addr := p.idPeers[id]
	delete(p.idPeers, id)
	delete(p.addrPeers, addr)
	p.resetIdshot()
}

// addrMapClone returns a shallow copy of the addrPeers map.
func (p *peers) addrMapClone() map[string]int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	m := make(map[string]int64)
	for k, v := range p.addrPeers {
		m[k] = v
	}
	return m
}

// ids returns a copy of the slice containing all peer IDs.
func (p *peers) ids() []int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	ids := make([]int64, len(p.idshot))
	copy(ids, p.idshot)
	return ids
}

// addrs returns a copy of the slice containing all peer addresses.
func (p *peers) addrs() []string {
	p.mux.RLock()
	defer p.mux.RUnlock()
	as := make([]string, 0, len(p.addrPeers))
	for k := range p.addrPeers {
		as = append(as, k)
	}
	return as
}

func (p *peers) getAddrMux(addr string) *sync.Mutex {
	p.mux.RLock()
	defer p.mux.RUnlock()
	mux, _ := p.addrMuxs[addr]
	return mux
}
