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

// Package scheduler contains all the logic for periodic execution of
// tables, including construction of a dependency graph,
// prioritization, etc.
package scheduler

import (
	"time"
)

// Schedulable describes something that can return a list of
// Conditions.
type Schedulable interface {
	Conditions() []*Condition
}

// A Run is a collection of tasks which is to be selectively
// executed. The run has a notion of "now" which is when it presumably
// is started.
type Run struct {
	Now time.Time
}

// Given a list of tasks, return those whose Condition is satisfied by
// this Run's "Now".
func (r *Run) SelectTasks(tasks []Schedulable) []Schedulable {
	ready := make([]Schedulable, 0, len(tasks))
	for _, t := range tasks {
		for _, c := range t.Conditions() {
			if c.Satisfied(r.Now) {
				ready = append(ready, t)
				break
			}
		}
	}
	return ready
}
