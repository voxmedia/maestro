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
	"encoding/json"
	"testing"
	"time"
)

func Test_MondaysAt9(t *testing.T) {
	cond := &Condition{
		Weekdays: map[time.Weekday]bool{1: true},
		Hours:    map[int]bool{9: true},
	}
	if cond.Satisfied(time.Date(2017, 8, 28, 9, 0, 0, 0, time.UTC)) == false {
		t.Errorf("Should run task if it's scheduled on Mondays at 9am UTC with current time of 2017-08-28 09:00:00")
	}
	if cond.Satisfied(time.Date(2017, 8, 28, 10, 0, 0, 0, time.UTC)) == true {
		t.Errorf("Should not run task if it's scheduled on Mondays at 9am UTC with current time of 2017-08-28 10:00:00")
	}
	if cond.Satisfied(time.Date(2017, 8, 21, 9, 0, 0, 0, time.UTC)) == false {
		t.Errorf("Should not run task if it's scheduled on Mondays at 9am UTC when current date is not a Monday.")
	}
}

func TestCondition_UnmarshalJSON(t *testing.T) {
	var cond Condition
	err := json.Unmarshal([]byte(`{"weekdays": [1,3], "months": [1,2], "hours": [0,12]}`), &cond)
	if err != nil {
		t.Errorf("Should not have errored out when unmarshaling JSON")
	}
	if cond.Satisfied(time.Date(2017, 1, 30, 12, 0, 0, 0, time.UTC)) == false {
		t.Errorf("Should run task if it's scheduled on Mon/Wed for the months of Jan & Feb at 0 or 12pm UTC with time of 2017-01-30 12:00:00")
	}
	if cond.Satisfied(time.Date(2017, 2, 15, 0, 0, 0, 0, time.UTC)) == false {
		t.Errorf("Should run task if it's scheduled on Mon/Wed for the months of Jan & Feb at 0 or 12pm UTC with time of 2017-02-15 00:00:00")
	}
}

func TestCondition_MarshalJSON(t *testing.T) {
	cond := &Condition{
		Weekdays: map[time.Weekday]bool{1: true},
		Hours:    map[int]bool{9: true},
	}
	json, err := json.Marshal(cond)
	if err != nil {
		t.Errorf("Should not have errored out when marshaling JSON")
	}
	if string(json) != `{"hours":[9],"weekdays":[1]}` {
		t.Errorf("Wrong JSON generated after marshaling: %v", string(json))
	}
}
