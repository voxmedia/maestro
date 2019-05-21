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

package scheduler

import (
	"testing"
	"time"
)

type Task struct {
	cond []*Condition
}

func (t *Task) Conditions() []*Condition {
	return t.cond
}

func TestSelectTasksForRun(t *testing.T) {
	run := &Run{
		Now: time.Date(2017, 8, 28, 9, 0, 0, 0, time.UTC),
	}

	tasks := []Schedulable{
		&Task{[]*Condition{&Condition{Weekdays: Weekdays{0: true}, Hours: Hours{9: true}}}},
		&Task{[]*Condition{&Condition{Weekdays: Weekdays{1: true}, Hours: Hours{9: true}}}},
		&Task{[]*Condition{&Condition{Weekdays: Weekdays{1: true, 3: true, 5: true}, Hours: Hours{9: true, 10: true}}}},
		&Task{ // Multiple conditions are OR-ed
			[]*Condition{
				&Condition{Hours: Hours{9: true}},
				&Condition{Months: Months{1: true}, Hours: Hours{8: true}},
			},
		},
	}
	selected := run.SelectTasks(tasks)
	if len(selected) != 3 {
		t.Errorf("Three tasks should be selected")
	}
}
