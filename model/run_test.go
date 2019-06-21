/* Copyright 2019 Vox Media, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

package model

import (
	"testing"
	"time"
)

func Test_nextRunTicks(t *testing.T) {

	// Happy path
	freqs := []*Freq{&Freq{
		Period: 10 * time.Second,
		Offset: 0,
		Active: true,
	}}
	now, _ := time.Parse(time.RFC3339, "2010-06-11T11:30:05Z")
	step := 10 * time.Second

	ticks := nextRunTicks(now, step, freqs)
	if len(ticks) == 0 {
		t.Errorf("len(ticks) == 0")
	}

	// Not happening
	freqs[0].Period = time.Hour
	ticks = nextRunTicks(now, step, freqs)
	if len(ticks) > 0 {
		t.Errorf("len(ticks) > 0")
	}

	// Slightly late
	freqs[0].Period = 30 * time.Minute
	ticks = nextRunTicks(now, step, freqs)
	if len(ticks) > 0 {
		t.Errorf("len(ticks) > 0")
	}

	// Exact match
	now, _ = time.Parse(time.RFC3339, "2010-06-11T11:29:50Z")
	ticks = nextRunTicks(now, step, freqs)
	if len(ticks) != 1 {
		t.Errorf("len(ticks) != 1: %v", len(ticks))
	}

	// Exact match 2
	now, _ = time.Parse(time.RFC3339, "2010-06-11T11:30:00Z")
	ticks = nextRunTicks(now, step, freqs)
	if len(ticks) != 0 {
		t.Errorf("len(ticks) != 0: %v", len(ticks))
	}

	// Very close
	now, _ = time.Parse(time.RFC3339, "2010-06-11T11:29:59.9999Z")
	ticks = nextRunTicks(now, step, freqs)
	if len(ticks) != 1 {
		t.Errorf("len(ticks) != 1: %v", len(ticks))
	}
}
