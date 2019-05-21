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
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"relative/scheduler"
)

// Run is a series of tables triggered by Maestro and associated with
// a Frequency.
//
// A Run is created with a NewRun() (which merely inits the
// datastructure), followed by Assemble(). Assemble selects all the
// tables associated with the Run's frequency and constructs BQJob
// configurations for each table and inserts those into the
// database. This collection of BQJobs is the execution plan for this
// Run and is self-sufficient, reflecting the tables as they were at
// the time of Assemble. This means that the tables can change
// afterwards, but it will not affect an already assembled Run.
//
// The Run execution is commenced via Start(), which repeatedly calls
// processCycle, which, in turn, constructs a DAG of the jobs,
// traverses and submits BQ jobs that can execute. At this point a
// BQJob will have a BQJobId which is the indicator that it has been
// submitted. If a job encounters errors, processCycle fails and with
// it fails the entire Run, requiring manual intervention afterwards.
//
// A failed Run can be resumed with Resume(). All that Resume does is
// continue the cycling process, but this time counting errors. If the
// count of errors exceeds the count at the beginning of the Run, the
// Run fails again.
type Run struct {
	Id         int64
	UserId     *int64     `db:"user_id"`     // Non nil only if this is a UI-triggered un
	CreatedAt  time.Time  `db:"created_at"`  // Time the run was created
	StartTime  *time.Time `db:"start_time"`  // Time the run started (as opposed to created)
	EndTime    *time.Time `db:"end_time"`    // Time the run ended (even if in failure)
	FreqId     int64      `db:"freq_id"`     // The frequency associated with the run
	FreqName   string     `db:"freq_name"`   // This comes from freqs, so don't count on it being there
	TotalBytes int64      `db:"total_bytes"` // This comes from SUM() on bq_jobs, same warning as ^
	Error      *string    `db:"error"`       // Text of the error that failed the run (redundant - same error would exist in the table)

	ignoreErrors bool `db:"-"` // when a run is resumed
	maxErrCnt    int  `db:"-"` // when a run is resumed
}

// Return a new Run. (Does not write to the database).
func NewRun(freqId int64, userId *int64) *Run {
	return &Run{
		UserId: userId,
		FreqId: freqId,
	}
}

// Write the run to the database and create and save all the jobs that
// belong to it, as well as figure out the parent/child relationship
// in the process.
func (r *Run) Assemble(m *Model, now time.Time) error {
	if r.Id != 0 {
		return fmt.Errorf("Run already has an Id, cannot assemble twice.")
	}

	// Create a run record in the db
	nr, err := m.InsertRun(r.UserId, r.FreqId)
	if err != nil {
		return err
	}
	r.Id = nr.Id

	// List tables in this run
	byFreq, err := m.TablesByFrequency(r.FreqId)
	if err != nil {
		return err
	}

	// Make sure conditions are satisfied
	var tables []*Table
	for _, t := range byFreq {
		if t.conditionsSatisfied(now) {
			tables = append(tables, t)
		}
	}

	// Compute parents
	parents, err := m.TablesParents(tables)
	if err != nil {
		return err
	}

	// Create BQ jobs and store parent table ids in a slice in the job
	for _, t := range tables {

		pidslice := make([]int64, 0, len(parents[t.Name]))
		for _, p := range parents[t.Name] {
			pidslice = append(pidslice, p.Id)
		}

		var job *BQJob
		if t.IsImport() { // Import
			ds, err := t.ImportDataset(m)
			if err != nil {
				return err
			}
			name := t.importFileName(&r.Id, now)
			gcsUrl := m.gcs.UrlForName(name)
			job = t.newBQLoadJob(m.bq, r.UserId, &r.Id, []string{gcsUrl}, ds, now)
		} else if t.IsExternal() { // External
			// NB: Autodetect is set true by newBQLoadJob
			// We do not know the URLs to GCS, so we pass in nil for now
			// It will be set by ui.tableLoadExternalHandler()
			job = t.newBQLoadJob(m.bq, r.UserId, &r.Id, nil, t.Dataset, now)
		} else { // Summary
			job = t.newBQQueryJob(m.bq, r.UserId, &r.Id, pidslice)
		}

		if job, err = m.InsertBQJob(job); err != nil {
			return err
		}
	}

	return nil
}

func (r *Run) Start(m *Model) error {
	log.Printf("Starting run.")
	now := time.Now()
	r.StartTime = &now
	if err := m.UpdateRun(r); err != nil {
		return err
	}
	go monitorRun(r, m)
	return nil
}

// Resume a failed run. The run will continue *after* the failed
// table, presumably that table would be fixed and ran manually.
func (r *Run) Resume(m *Model) error {
	log.Printf("Resuming run (%d) freq_id: %d.", r.Id, r.FreqId)
	r.ignoreErrors = true
	r.EndTime = nil
	r.Error = nil
	if err := m.UpdateRun(r); err != nil {
		return err
	}
	go monitorRun(r, m)
	return nil
}

// Construct and return the graph (DAG) of this run (for UI).
func (r *Run) Graph(m *Model) (scheduler.Graph, error) {
	return loadRunJobGraph(m, r.Id)
}

func monitorRun(r *Run, m *Model) {
	const pause = 5 * time.Second

	if r.ignoreErrors {
		// We are resuming, count the number of errors and make that
		// maximum allowable for this run. Any addidional errors will
		// stop the run.
		cnt, err := r.countErrors(m)
		if err != nil {
			log.Printf("monitorRun error: %v", err)
		}
		r.maxErrCnt = cnt
	}

	for {

		if err := r.processCycle(m); err != nil {
			m.SlackAlert(fmt.Sprintf("Run (%d) failed (freq_id: %v): %v", r.Id, r.FreqId, err))
			log.Printf("Run (%d) failed: %v", r.Id, err)
			now := time.Now()
			r.EndTime = &now
			es := err.Error()
			r.Error = &es
			if err = m.UpdateRun(r); err != nil {
				log.Printf("monitorRun error: %v", err)
			}
		}

		if r.EndTime != nil {
			log.Printf("Run (%v) completed (freq_id: %v), exiting monitorRun().", r.Id, r.FreqId)
			return
		}

		time.Sleep(pause)
	}
}

func (r *Run) processCycle(m *Model) error {
	g, err := r.loadRunUnfinishedJobGraph(m)
	if err != nil {
		return err
	}

	if len(g) == 0 {
		// Mark this run as completed
		now := time.Now()
		r.EndTime = &now
		return m.UpdateRun(r)
	}

	ready, err := g.ReadyItems()
	if err != nil {
		return err
	}

	if len(ready) > 0 {
		for _, item := range ready {

			job := item.(*BQJob)
			if job.error != "" {
				// fail this run
				return fmt.Errorf("Error in table %v: %v", job.TableId, job.error)
			}

			if job.BQJobId == "" { // not yet submitted

				if job.Type == "load" {
					status := m.GetImportStatus(job.TableId)
					if status == ImpError {
						// fail this run
						return fmt.Errorf("Errors encountered during import of table %v.", job.TableId)
					}
					if status == ImpNone {

						t, err := m.SelectTable(job.TableId)
						if err != nil {
							return err
						}

						if t.IsImport() {
							log.Printf("Running %#v (import)", item.GetName())
							if err := t.setRunning(m, true); err != nil {
								return err
							}
							// userid doesn't matter since the job already exists
							m.queueImport(t, nil, job)
						} else if t.IsExternal() && !t.hasExternalWait(m) {
							log.Printf("Running %#v (external wait start)", item.GetName())
							t.startExternalWait(m, job) // sets running
						}
					}

				} else { // a normal query BQ job

					log.Printf("Running %#v (summary)", item.GetName())

					// Mark table as running
					t, err := m.SelectTable(job.TableId)
					if err != nil {
						return err
					}

					// Clear the error (Issue #117)
					t.Error = ""

					if err := t.setRunning(m, true); err != nil {
						return err
					}

					if err = submitJob(job, m); err != nil {
						t.setError(m, err)
						return err
					}

					go monitorJob(job, m)
				}
			}
		}
	}
	return nil
}

func graphFromJobsById(byId map[int64]*BQJob) (scheduler.Graph, error) {
	g := scheduler.NewGraph()
	for _, job := range byId {
		var parents []int64
		if job.Parents != nil && *job.Parents != "" {
			if err := json.Unmarshal([]byte(*job.Parents), &parents); err != nil {
				return nil, err
			}
		}

		n := 0
		for _, pid := range parents {
			if parent, ok := byId[int64(pid)]; ok {
				g.Relate(parent, job)
				n++
			}
		}
		if n == 0 {
			// if we had parents, they were all done
			g.Relate(nil, job)
		}
	}
	return g, nil
}

func (r *Run) countErrors(m *Model) (int, error) {
	jobs, err := m.SelectBQJobsByRunId(r.Id)
	if err != nil {
		return 0, err
	}

	// convert to a map
	errCnt := 0
	for _, j := range jobs {
		if err = j.cacheStatus(); err != nil {
			return 0, err
		}
		if strings.Contains(j.error, bqSchemaErr) {
			// ignore schema mismatch errors
			continue
		}
		if j.error != "" {
			errCnt++
		}
	}
	return errCnt, nil
}

func (r *Run) loadRunUnfinishedJobGraph(m *Model) (scheduler.Graph, error) {

	jobs, err := m.SelectBQJobsByRunId(r.Id)
	if err != nil {
		return nil, err
	}

	// convert to a map
	byId := make(map[int64]*BQJob)
	errCnt := 0
	for _, j := range jobs {
		if err = j.cacheStatus(); err != nil {
			return nil, err
		}
		if strings.Contains(j.error, bqSchemaErr) {
			// ignore schema mismatch errors
			continue
		}

		if j.error != "" {
			errCnt++
		}

		if j.state != "DONE" || (j.error != "" && (r.maxErrCnt == 0 || r.maxErrCnt > 0 && errCnt > r.maxErrCnt)) {
			if j.error != "" && r.maxErrCnt > 0 {
				log.Printf("Maximum allowed errors for resumed run (%d) of %d is exceeded (errCnt: %d).",
					r.Id, r.maxErrCnt, errCnt)
			}
			byId[j.TableId] = j
		}
	}

	return graphFromJobsById(byId)
}

func loadRunJobGraph(m *Model, runId int64) (scheduler.Graph, error) {

	jobs, err := m.SelectBQJobsByRunId(runId)
	if err != nil {
		return nil, err
	}

	// convert to a map
	byId := make(map[int64]*BQJob)
	for _, j := range jobs {
		if j.Type == "extract" { // we only want the corresponding query job
			continue
		}
		if err = j.cacheStatus(); err != nil {
			return nil, err
		}
		byId[j.TableId] = j
	}

	return graphFromJobsById(byId)
}

// This is meant to be called once on startup to check the status of
// an unifnished run and continue running it if needed.
func monitorUnfinishedRuns(m *Model) {

	log.Printf("Checking for unfinished runs...")

	runs, err := m.db.UnfinishedRuns()
	if err != nil {
		log.Printf("monitorUnfinishedRuns(): error %v", err)
	}

	if len(runs) == 0 {
		log.Printf("No unfinished runs.")
		return
	}

	for _, r := range runs {
		go monitorRun(r, m)
		log.Printf("Started monitor for run id %d (freq_id: %d)", r.Id, r.FreqId)
	}
}

type runTick struct {
	freq    *Freq
	tooLate time.Time
}

func runTicker(freq *Freq, ch chan runTick) {
	var lastNext time.Time
	for {
		now := time.Now()
		var next time.Time
		if now.Truncate(freq.Period).Add(freq.Offset).After(now) {
			// it's possible we are in "today" and still before offset
			next = now.Truncate(freq.Period).Add(freq.Offset)
		} else {
			next = now.Truncate(freq.Period).Add(freq.Period).Add(freq.Offset)
		}
		if !lastNext.Equal(next) {
			log.Printf("Next run for period %q is at: %v", freq.Name, next)
			lastNext = next
		}
		if next.Before(now) { // possible with a negative offset
			time.Sleep(time.Second)
		} else {
			time.Sleep(next.Sub(now))
			ch <- runTick{freq: freq, tooLate: next.Add(freq.Period)}
		}
	}
}

func triggerRuns(m *Model) {
	// TODO This needs to re-read the freqs after every run and be
	// able to create/cancel the ticks
	freqs, err := m.SelectFreqs()
	if err != nil {
		log.Printf("triggerRuns(): error %v", err)
	}

	ch := make(chan runTick, 8)
	for _, freq := range freqs {
		if freq.Active {
			log.Printf("Starting run ticker for period %q: %v +%v", freq.Name, freq.Period, freq.Offset)
			go runTicker(freq, ch)
		}
	}

	// This is what actually starts the runs
	go func(ch chan runTick) {
		for t := range ch {
			now := time.Now()
			if t.tooLate.After(now) {
				log.Printf("triggerRuns(): starting run %v", t)

				var err error
				run := NewRun(t.freq.Id, nil)
				if err = run.Assemble(m, now); err != nil {
					err = fmt.Errorf("error assembling: %v", err)
				} else {
					if err = run.Start(m); err != nil {
						err = fmt.Errorf("error starting: %v", err)
					}
				}

				if err != nil {
					log.Printf("triggerRuns(): %v", err)
					m.SlackAlert(fmt.Sprintf("Maestro run error: %v", err))
				}

			} else {
				log.Printf("triggerRuns(): dropping run tick %v because it is too late.", t)
			}
		}
	}(ch)

}
