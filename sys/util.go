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

package sys

import (
	"fmt"
	"github.com/shirou/gopsutil/v4/cpu"
	"math"
	"time"
)

func Recoverable(err *error) {
	if r := recover(); r != nil {
		//log.Debug(string(debug.Stack()))
		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}

func GetCPURate(interval time.Duration) int {
	if percent, err := cpu.Percent(interval, false); err == nil {
		return int(math.Max(0, math.Min(100, math.Round(percent[0]))))
	}
	return 0
}
