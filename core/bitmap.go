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
	"math"
)

type bitmap struct {
	data []uint64
	size int64
	cf   [4]int64
}

func newBitmap(n int64) *bitmap {
	numWords := int(math.Ceil(float64(n) / 64))
	return &bitmap{
		data: make([]uint64, numWords),
		size: n,
	}
}

func (b *bitmap) Set(id int64) {
	b.cleanData(id)
	id = id % b.size
	if id < 0 || id >= b.size {
		return
	}
	wordIndex := id / 64
	bitIndex := id % 64
	b.data[wordIndex] |= (1 << bitIndex)
}

func (b *bitmap) Clear(id int64) {
	if id < 0 {
		return
	}
	id = id % b.size

	wordIndex := id / 64
	bitIndex := id % 64
	b.data[wordIndex] &= ^(1 << bitIndex)
}

func (b *bitmap) Has(id int64) bool {
	id = id % b.size
	if id < 0 {
		return false
	}
	wordIndex := id / 64
	bitIndex := id % 64
	return (b.data[wordIndex] & (1 << bitIndex)) != 0
}

func (b *bitmap) cleanData(id int64) {
	flag := id/b.size + 1
	if v := id % b.size; v <= b.size/4 {
		if b.cf[0] != flag {
			b.cf[0] = flag
			b.cleanOldData(3)
		}
	} else if v <= (b.size / 2) {
		if b.cf[1] != flag {
			b.cf[1] = flag
			b.cleanOldData(4)
		}
	} else if v <= (3 * b.size / 4) {
		if b.cf[2] != flag {
			b.cf[2] = flag
			b.cleanOldData(1)
		}
	} else {
		if b.cf[3] != flag {
			b.cf[3] = flag
			b.cleanOldData(2)
		}
	}
}
func (b *bitmap) cleanOldData(f byte) {
	switch f {
	case 1:
		for i := int64(0); i <= b.size/4; i++ {
			b.Clear(i)
		}
	case 2:
		for i := b.size / 4; i <= b.size/2; i++ {
			b.Clear(i)
		}
	case 3:
		for i := b.size / 2; i <= 3*b.size/4; i++ {
			b.Clear(i)
		}
	case 4:
		for i := 3 * b.size / 4; i < b.size; i++ {
			b.Clear(i)
		}
	}
}
