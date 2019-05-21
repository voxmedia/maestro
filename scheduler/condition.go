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
	"sort"
	"time"
)

type Months map[time.Month]bool
type Weekdays map[time.Weekday]bool
type Days map[int]bool
type Hours map[int]bool

// A Condition is a list of various time attributes such as months,
// weekdays, days or month, etc. This can be expanded in the
// future.
type Condition struct {
	Months   Months   `json:"months"`
	Weekdays Weekdays `json:"weekdays"`
	Days     Days     `json:"days"`
	Hours    Hours    `json:"hours"`
}

func (c *Condition) UnmarshalJSON(data []byte) error {
	c.Months = make(map[time.Month]bool)
	c.Weekdays = make(map[time.Weekday]bool)
	c.Days = make(map[int]bool)
	c.Hours = make(map[int]bool)

	aux := &struct {
		Months   []time.Month
		Weekdays []time.Weekday
		Days     []int
		Hours    []int
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	for _, m := range aux.Months {
		c.Months[m] = true
	}

	for _, w := range aux.Weekdays {
		c.Weekdays[w] = true
	}

	for _, d := range aux.Days {
		c.Days[d] = true
	}

	for _, h := range aux.Hours {
		c.Hours[h] = true
	}
	return nil
}

func (c *Condition) MarshalJSON() ([]byte, error) {
	var data map[string][]int = make(map[string][]int)
	if c.Months != nil {
		data["months"] = make([]int, 0, len(c.Months))
		for k := range c.Months {
			data["months"] = append(data["months"], int(k))
		}
		sort.Ints(data["months"])
	}
	if c.Weekdays != nil {
		data["weekdays"] = make([]int, 0, len(c.Weekdays))
		for k := range c.Weekdays {
			data["weekdays"] = append(data["weekdays"], int(k))
		}
		sort.Ints(data["weekdays"])
	}
	if c.Days != nil {
		data["days"] = make([]int, 0, len(c.Days))
		for k := range c.Days {
			data["days"] = append(data["days"], int(k))
		}
		sort.Ints(data["days"])
	}
	if c.Hours != nil {
		data["hours"] = make([]int, 0, len(c.Hours))
		for k := range c.Hours {
			data["hours"] = append(data["hours"], int(k))
		}
		sort.Ints(data["hours"])
	}
	return json.Marshal(data)
}

// For each of the time attributes specified in the condition, at
// least one must match the passed-in time value.
// E.g. given an argument which is Monday, 9am
// Condition [Monday,Tuesday], [9am, 10am] : satisfied
// Condition [Tuesday,Wednesday], [9am] : not satisfied
func (c *Condition) Satisfied(now time.Time) bool {
	if len(c.Months) > 0 && !c.Months[now.Month()] {
		return false
	}
	if len(c.Weekdays) > 0 && !c.Weekdays[now.Weekday()] {
		return false
	}
	if len(c.Days) > 0 && !c.Days[now.Day()] {
		return false
	}
	if len(c.Hours) > 0 && !c.Hours[now.Hour()] {
		return false
	}
	return true
}
