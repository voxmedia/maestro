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
	"relative/scheduler"
	"testing"
	"time"
)

func Test_NewRun(t *testing.T) {

	u := int64(100)
	r := NewRun(200, &u)
	if r.FreqId != 200 {
		t.Errorf("r.FreqId != 200")
	}
	if *r.UserId != 100 {
		t.Errorf("r.UserId != 100")
	}

	r = NewRun(200, nil)
	if r.UserId != nil {
		t.Errorf("r.UserId != nil")
	}
}

func Test_Run_Assemble(t *testing.T)                  {} // TODO
func Test_Run_Start(t *testing.T)                     {} // TODO
func Test_Run_Resume(t *testing.T)                    {} // TODO
func Test_Run_Graph(t *testing.T)                     {} // TODO
func Test_Run_processCycle(t *testing.T)              {} // TODO
func Test_Run_countErrors(t *testing.T)               {} // TODO
func Test_Run_loadRunUnfinishedJobGraph(t *testing.T) {} // TODO

func Test_monitorRun(t *testing.T)        {} // TODO
func Test_graphFromJobsById(t *testing.T) {} // TODO

func Test_loadRunJobGraph(t *testing.T) {

	save := loadRunJobGraph
	defer func() { loadRunJobGraph = save }()

	count := 0
	loadRunJobGraph = func(m *Model, runId int64) (scheduler.Graph, error) {
		count++
		return nil, nil
	}

	loadRunJobGraph(nil, 0)
	if count != 1 {
		t.Errorf("count != 1")
	}
}

func Test_monitorUnfinishedRuns(t *testing.T) {} // TODO

func Test_nextRunTime(t *testing.T) {

	for _, pn := range []struct {
		period time.Duration
		now    string
		offset time.Duration
		expect string
	}{
		{10 * time.Second, "2010-06-11T11:30:05Z", 0, "2010-06-11T11:30:10Z"},                // Happy path
		{60 * time.Minute, "2010-06-11T11:30:05Z", 0, "2010-06-11T12:00:00Z"},                // Not happening
		{30 * time.Minute, "2010-06-11T11:30:05Z", 0, "2010-06-11T12:00:00Z"},                // Slightly late
		{30 * time.Minute, "2010-06-11T11:29:50Z", 0, "2010-06-11T11:30:00Z"},                // Exact match
		{30 * time.Minute, "2010-06-11T11:30:00Z", 0, "2010-06-11T11:30:00Z"},                // Exact match 2
		{30 * time.Minute, "2010-06-11T11:29:59.9999Z", 0, "2010-06-11T11:30:00Z"},           // Very close
		{30 * time.Minute, "2010-06-11T11:30:00Z", time.Minute, "2010-06-11T11:31:00Z"},      // Offset
		{30 * time.Minute, "2010-06-11T11:30:00Z", 30 * time.Minute, "2010-06-11T12:00:00Z"}, // Large ffset
		{30 * time.Minute, "2010-06-11T11:30:00Z", -time.Minute, "2010-06-11T11:59:00Z"},     // Neg offset
	} {

		now, _ := time.Parse(time.RFC3339, pn.now)
		expect, _ := time.Parse(time.RFC3339, pn.expect)

		result := nextRunTime(now, pn.period, pn.offset)

		if !result.Equal(expect) {
			t.Errorf("!result.Equal(expect). now: %v period: %v offset: %v result: %v expect: %v",
				now, pn.period, pn.offset, result, expect)
		}
	}
}

func Test_nextRunTick(t *testing.T) {

	step := 10 * time.Second

	for _, pn := range []struct {
		period time.Duration
		now    string
		notnil bool // expect tick != nil
	}{
		{10 * time.Second, "2010-06-11T11:30:05Z", true},      // Happy path
		{60 * time.Minute, "2010-06-11T11:30:05Z", false},     // Not happening
		{30 * time.Minute, "2010-06-11T11:30:05Z", false},     // Slightly late
		{30 * time.Minute, "2010-06-11T11:29:50Z", true},      // Exact match
		{30 * time.Minute, "2010-06-11T11:30:00Z", false},     // Exact match 2
		{30 * time.Minute, "2010-06-11T11:29:59.9999Z", true}, // Very close
	} {

		freq := &Freq{
			Period: pn.period,
			Offset: 0,
		}
		now, _ := time.Parse(time.RFC3339, pn.now)

		tick := nextRunTick(now, step, freq)
		if pn.notnil != (tick != nil) {
			t.Errorf("tick: %v null: %v period: %v now: %v", tick, pn.notnil, pn.period, pn.now)
		}

	}
}

func Test_nextRunTicks(t *testing.T) {
	// Mainly jusr count the number of times nextRunTick is called

	save := nextRunTick
	defer func() { nextRunTick = save }()

	count := 0
	nextRunTick = func(now time.Time, step time.Duration, freq *Freq) *runTick {
		count++
		return save(now, step, freq)
	}

	freqs := []*Freq{&Freq{Active: true}, &Freq{Active: false},
		&Freq{Period: 10 * time.Second, Active: true}}

	now, _ := time.Parse(time.RFC3339, "2010-06-11T11:30:05Z")
	step := 10 * time.Second
	result := nextRunTicks(now, step, freqs)
	if count != len(freqs) {
		t.Errorf("count != len(freqs)")
	}
	if len(result) != 1 {
		t.Errorf("len(result) != 1")
	}
}

func Test_startTicker(t *testing.T) {
	m := &Model{
		db: &tDb{
			selectFreqs: func() ([]*Freq, error) {
				return nil, nil
			},
		},
	}

	ticks, stop := startTicker(m, 0)
	if ticks == nil {
		t.Errorf("ticks == nil")
	}
	if cap(ticks) != 8 {
		t.Errorf("cap(ticks) != 8")
	}
	if stop == nil {
		t.Errorf("stop == nil")
	}
	stop <- true
}

func Test_runTicker(t *testing.T) {
	m := &Model{
		db: &tDb{
			selectFreqs: func() ([]*Freq, error) {
				return nil, nil
			},
		},
	}

	ch := make(chan runTick)
	stop := make(chan bool)

	go runTicker(m, time.Millisecond, ch, stop)

	// Stop the ticker
	stop <- false

	// Drain the (already empty) channel
	for _ = range ch {
	}

	select {
	case _, ok := <-ch:
		if !ok {
			// This is good, no news.
		} else {
			t.Errorf("Channel still open after we sent something to stop.")
		}
	default:
		t.Errorf("Channel still open (and empty) after we sent something to stop.")
	}
}

func Test_logNextRuns(t *testing.T) {} // TODO
func Test_triggerRuns(t *testing.T) {} // TODO
