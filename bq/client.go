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

package bq

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"

	"google.golang.org/api/bigquery/v2"
)

type bqClient struct {
	bq      *bigquery.Service
	proj    string
	jobsets map[string]*list.List
}

func newBQClient(client *http.Client, dsProj string) (*bqClient, error) {

	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	return &bqClient{
		bq:      service,
		proj:    dsProj,
		jobsets: make(map[string]*list.List),
	}, nil
}

func (c *bqClient) startJob(conf *bigquery.JobConfiguration) (*bigquery.Job, error) {
	bqJob := &bigquery.Job{Configuration: conf}
	call := c.bq.Jobs.Insert(c.proj, bqJob)
	return call.Do()
}

func rowToStringSlice(row *bigquery.TableRow) []string {
	result := make([]string, 0, len(row.F))
	for _, cell := range row.F {
		if cell.V == nil {
			result = append(result, "\\N")
		} else if val, ok := cell.V.(string); ok {
			result = append(result, val)
		} else {
			buf, _ := json.Marshal(cell.V)
			result = append(result, string(buf))
		}
	}
	return result
}

func rowToInterfaceSlice(row *bigquery.TableRow) []interface{} {
	// NB: All data will be strings
	result := make([]interface{}, 0, len(row.F))
	for _, cell := range row.F {
		result = append(result, cell.V)
	}
	return result
}

func (c *bqClient) getTable(dataset, table string) (*bigquery.Table, error) {
	return c.bq.Tables.Get(c.proj, dataset, table).Do()
}

func (c *bqClient) getTableColumnNames(dataset, table string) ([]string, error) {
	result := []string{}

	bqTable, err := c.getTable(dataset, table)
	if err != nil {
		return nil, err
	}

	for _, field := range bqTable.Schema.Fields {
		result = append(result, field.Name)
	}

	return result, nil
}

func (c *bqClient) getTableData(dataset, table string, f func([]string) error) error {

	const BATCH_SIZE = 1024 // TODO: Should be configurable?

	call := c.bq.Tabledata.List(c.proj, dataset, table)
	call.MaxResults(BATCH_SIZE)

	err := call.Pages(context.TODO(), func(list *bigquery.TableDataList) error {
		for _, row := range list.Rows {
			slice := rowToStringSlice(row)
			if err := f(slice); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (c *bqClient) getJob(id string) (*bigquery.Job, error) {
	return c.bq.Jobs.Get(c.proj, id).Do()
}

func (c *bqClient) monitor(jobset string) {

	const (
		BaseBackoff         = 250
		BackoffGrowthFactor = 1.8
		BackoffGrowthDamper = 0.25
		MaxBackoff          = 30000
		JobStatusDone       = "DONE"
	)

	jobq, ok := c.jobsets[jobset]
	if !ok {
		return
	}

	var backoff float64 = BaseBackoff
	pause := func(grow bool) {
		if grow {
			backoff *= BackoffGrowthFactor
			backoff -= (backoff * rand.Float64() * BackoffGrowthDamper)
			backoff = math.Min(backoff, MaxBackoff)
			fmt.Fprintf(os.Stderr, "[%s] Checking remaining %d jobs...\n", jobset,
				1+jobq.Len())
		}
		time.Sleep(time.Duration(backoff) * time.Millisecond)
	}
	var stats jobStats

	// Track a 'head' pending job in queue for detecting cycling.
	head := ""
	// Loop until all jobs are done - with either success or error.
	for jobq.Len() > 0 {
		jel := jobq.Front()
		job := jel.Value.(*bigquery.Job)
		jobq.Remove(jel)
		jid := job.JobReference.JobId
		loop := false

		// Check and possibly pick a new head job id.
		if len(head) == 0 {
			head = jid
		} else {
			if jid == head {
				loop = true
			}
		}

		// Retrieve the job's current status.
		pause(loop)
		j, err := c.getJob(jid)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			// In this case of a transient API error, we want keep the job.
			if j == nil {
				jobq.PushBack(job)
			} else {
				// Must reset head tracker if job is discarded.
				if loop {
					head = ""
					backoff = BaseBackoff
				}
			}
			continue
		}

		// Reassign with the updated job data (from Get).
		// We don't use j here as Get might return nil for this value.
		job = j

		if job.Status.State != JobStatusDone {
			jobq.PushBack(job)
			continue
		}

		if res := job.Status.ErrorResult; res != nil {
			fmt.Fprintln(os.Stderr, res.Message)
		} else {
			stat := job.Statistics
			if stat.Load != nil {
				lstat := stat.Load
				stats.files += 1
				stats.bytesIn += lstat.InputFileBytes
				stats.bytesOut += lstat.OutputBytes
				stats.rows += lstat.OutputRows
			} else if stat.Query != nil {
				qstat := stat.Query
				stats.query = true
				stats.bytesIn += qstat.TotalBytesProcessed
				stats.bytesOut += qstat.TotalBytesBilled
				stats.files += int64(len(qstat.QueryPlan))
				for _, stage := range qstat.QueryPlan {
					stats.rows += stage.RecordsWritten
				}
			}
			stats.elapsed +=
				time.Duration(stat.EndTime-stat.StartTime) * time.Millisecond

			if stats.start.IsZero() {
				stats.start = time.Unix(stat.StartTime/1000, 0)
			} else {
				t := time.Unix(stat.StartTime/1000, 0)
				if stats.start.Sub(t) > 0 {
					stats.start = t
				}
			}

			if stats.finish.IsZero() {
				stats.finish = time.Unix(stat.EndTime/1000, 0)
			} else {
				t := time.Unix(stat.EndTime/1000, 0)
				if t.Sub(stats.finish) > 0 {
					stats.finish = t
				}
			}
		}
		// When the head job is processed reset the backoff since the loads
		// run in BQ in parallel.
		if loop {
			head = ""
			backoff = BaseBackoff
		}
	}

	fmt.Fprintf(os.Stderr, "%#v\n", stats)
}

type jobStats struct {
	// Number of files (sources) loaded.
	files int64
	// Bytes read from source (possibly compressed).
	bytesIn int64
	// Bytes loaded into BigQuery (uncompressed).
	bytesOut int64
	// Rows loaded into BigQuery.
	rows int64
	// Time taken to load source into table.
	elapsed time.Duration
	// Start time of the job.
	start time.Time
	// End time of the job.
	finish time.Time
	//
	query bool
}

func (s jobStats) GoString() string {
	const GB = 1 << 30

	if s.query {
		return fmt.Sprintf("\n%d stages processed in %v (%v). Size: %.2fGB Rows: %d\n",
			s.files, s.finish.Sub(s.start), s.elapsed, float64(s.bytesOut)/GB,
			s.rows)
	} else {
		return fmt.Sprintf("\n%d files loaded in %v (%v). Size: %.2fGB Rows: %d\n",
			s.files, s.finish.Sub(s.start), s.elapsed, float64(s.bytesOut)/GB,
			s.rows)
	}
}
