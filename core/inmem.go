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
	"errors"
	"github.com/donnie4w/gofer/hashmap"
	"github.com/donnie4w/raftx/log"
	"github.com/donnie4w/raftx/stub"
)

type memoryStateMachine struct {
	state *hashmap.Map[int64, []byte]
}

func (sm *memoryStateMachine) GetByRx(rxId int64) (*stub.MvccCell, error) {
	return nil, nil
}

func newMemoryStateMachine() stub.StateMachine {
	return &memoryStateMachine{state: hashmap.NewMap[int64, []byte]()}
}

func (sm *memoryStateMachine) ApplyMc(mc *stub.MvccCell) error {
	sm.state.Put(mc.GetRxId(), stub.Marshal(mc))
	return nil
}

func (sm *memoryStateMachine) Apply(rxId int64, command []byte) error {
	sm.state.Put(rxId, command)
	return nil
}

func (sm *memoryStateMachine) HasRx(rxId int64) bool {
	return sm.state.Has(rxId)
}

func (sm *memoryStateMachine) Rollback(rxId int64) error {
	log.Debug("rollBack:", rxId)
	sm.state.Del(rxId)
	return nil
}

func (sm *memoryStateMachine) Get(key []byte) ([]byte, error) {
	return key, nil
}

func (sm *memoryStateMachine) GetList(keys [][]byte) ([][2][]byte, error) {
	r := make([][2][]byte, len(keys))
	for _, key := range keys {
		r = append(r, [2][]byte{key, key})
	}
	return r, nil
}

func (sm *memoryStateMachine) Snapshot(fromRxId, toRxId int64) (*stub.MvccBean, error) {
	mb := &stub.MvccBean{Beans: make([]*stub.MvccCell, 0)}
	for i := fromRxId; i <= toRxId; i++ {
		v, _ := sm.state.Get(i)
		mb.Beans = append(mb.Beans, &stub.MvccCell{RxId: i, Key: v, Value: v, PType: 0})
	}
	return mb, nil
}

type memoryPersistent struct {
	data *hashmap.Map[string, []byte]
}

func newMemoryPersistent() stub.Persistent {
	return &memoryPersistent{data: hashmap.NewMap[string, []byte]()}
}

func (p *memoryPersistent) Put(key, value []byte) error {
	p.data.Put(string(key), value)
	return nil
}

func (p *memoryPersistent) Get(key []byte) ([]byte, error) {
	if v, ok := p.data.Get(string(key)); ok {
		return v, nil
	} else {
		return nil, errors.New("key not found")
	}
}

func (p *memoryPersistent) Del(key []byte) error {
	p.data.Del(string(key))
	return nil
}

func (p *memoryPersistent) Has(key []byte) bool {
	return p.data.Has(string(key))
}

func (p *memoryPersistent) BatchPut(kv [][2][]byte) error {
	for _, pair := range kv {
		p.data.Put(string(pair[0]), pair[1])
	}
	return nil
}

func (p *memoryPersistent) BatchDel(kv [][]byte) error {
	for _, v := range kv {
		p.Del(v)
	}
	return nil
}
