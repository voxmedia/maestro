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
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"relative/gcs"

	bigquery "google.golang.org/api/bigquery/v2"
)

// Representation of a BigQuery job. BQJob may seem to replicate much
// of the data that exists in Table but this is intentional. The table
// may change after the job and it is important to preserve all the
// information in the table as it was at the moment of job creation.
//
// A BQJob is created before submitting the job to BigQuery. Between
// the time it is created and submitted to BQ (which can be a while
// during a run, where all jobs are created upfront) it has no
// BQJobId. In a rare but not uncommon case BQ responds with "Retrying
// may solve the problem" error, when this happens the submission will
// be retried thereby altering the BQJobId (the old one is forgotten,
// though you can see it in the logs).
//
// BQJobs are essential to Runs, because Runs are initially
// constructed as a collection of BQJobs, which are then submitted in
// the correct order based on dependencies and priorities.
type BQJob struct {
	Id                  int64      `db:"id"`
	CreatedAt           time.Time  `db:"created_at"`
	TableId             int64      `db:"table_id"`         // Corresponding table
	UserId              *int64     `db:"user_id"`          // User who triggered the job or nil (if it was system-triggered)
	RunId               *int64     `db:"run_id"`           // Run in which this job ran or nil
	Parents             *string    `db:"parents"`          // List of parents for this table
	BQJobId             string     `db:"bq_job_id"`        // Id returned by BigQuery
	Type                string     `db:"type"`             // Type of job: query, extract, load
	Configuration       *string    `db:"configuration"`    // BigQuery Job configuration (as a JSON string)
	Status              *string    `db:"status"`           // BigQuery Job status (as a JSON string)
	QueryStats          *string    `db:"query_stats"`      // Excerpt from Statistics returned by BQ (JSON)
	LoadStats           *string    `db:"load_stats"`       // Excerpt from Statistics returned by BQ (JSON)
	ExtractStats        *string    `db:"extract_stats"`    // Excerpt from Statistics returned by BQ (JSON)
	DestinationUrls     *string    `db:"destination_urls"` // For GCS extracts
	CreationTime        *time.Time `db:"creation_time"`    // BQ-specific
	StartTime           *time.Time `db:"start_time"`       // BQ-specific
	EndTime             *time.Time `db:"end_time"`         // BQ-specific
	TotalBytesProcessed int64      `db:"total_bytes_processed"`
	TotalBytesBilled    int64      `db:"total_bytes_billed"`
	ImportBegin         *time.Time `db:"import_begin"`
	ImportEnd           *time.Time `db:"import_end"`
	ImportBytes         int64      `db:"import_bytes"`
	ImportRows          int64      `db:"import_rows"`

	bqConfig  *bigquery.JobConfiguration
	state     string
	error     string
	nExtracts int
}

const (
	bqSchemaErr        = "Provided Schema does not match Table"
	bqRetryingErr      = "Retrying may solve the problem"
	apiErrorRetryDelay = 15 * time.Second
)

// Satisfies scheduler.Item.
func (j *BQJob) GetName() string {
	return fmt.Sprintf("%d", j.TableId)
}

// Return a BQ TableReference.
func (j *BQJob) TableReference() (*bigquery.TableReference, error) {
	cfg, err := j.getConfig()
	if err != nil {
		return nil, err
	}

	var tref *bigquery.TableReference
	if j.Type == "load" && cfg.Load != nil {
		tref = cfg.Load.DestinationTable
	} else if j.Type == "query" && cfg.Query != nil {
		tref = cfg.Query.DestinationTable
	} else if j.Type == "extract" && cfg.Extract != nil {
		tref = cfg.Extract.SourceTable
	} else {
		return nil, fmt.Errorf("Unsupported job type or nil config (id %d): %v", j.Id, j.Type)
	}

	return tref, nil
}

// Return the status of the table.
func (j *BQJob) GetStatus() (status, error string, err error) {
	if err = j.cacheStatus(); err != nil {
		return "", "", err
	}
	return j.state, j.error, nil
}

func (j *BQJob) cacheStatus() error {
	if j.Status != nil {
		var status bigquery.JobStatus
		if err := json.Unmarshal([]byte(*j.Status), &status); err != nil {
			return err
		}
		j.state = status.State
		if status.ErrorResult != nil {
			j.error = status.ErrorResult.Message
		}
	}
	return nil
}

func (j *BQJob) getConfig() (*bigquery.JobConfiguration, error) {
	if j.bqConfig != nil {
		return j.bqConfig, nil
	}
	if j.Configuration == nil {
		return nil, fmt.Errorf("Missing configuration in job %d", j.Id)
	}
	var conf bigquery.JobConfiguration
	if err := json.Unmarshal([]byte(*j.Configuration), &conf); err != nil {
		return nil, err
	}
	// This is such a hack, dear Google...
	if conf.Query != nil && !*conf.Query.UseLegacySql {
		conf.Query.ForceSendFields = []string{"UseLegacySql"}
	}
	j.bqConfig = &conf

	return &conf, nil
}

func (j *BQJob) setConfig(conf *bigquery.JobConfiguration) {
	b, _ := conf.MarshalJSON()
	s := string(b)
	j.Configuration = &s
	j.bqConfig = conf
}

func (j *BQJob) setLoadJobSchema(schema *bigquery.TableSchema) (err error) {
	cfg, err := j.getConfig()
	if err != nil {
		return err
	}
	cfg.Load.Schema = schema
	cfg.Load.Autodetect = false
	j.setConfig(cfg)
	return nil
}

func (j *BQJob) setLoadJobGcsUris(gcsUrls []string) (err error) {
	cfg, err := j.getConfig()
	if err != nil {
		return err
	}
	cfg.Load.SourceUris = gcsUrls
	j.setConfig(cfg)
	return nil
}

// Generate GCS URLs that are signed (i.e. require no authentication).
func (j *BQJob) SignedExtractURLs(m *Model) ([]string, error) {

	if j.DestinationUrls == nil {
		return nil, nil
	}

	var urls []string
	if err := json.Unmarshal([]byte(*j.DestinationUrls), &urls); err != nil {
		return nil, err
	}

	var signeds []string
	for _, url := range urls {
		_, filename := filepath.Split(url)
		signed, err := m.bq.SignedStorageUrl(filename, "GET")
		if err != nil {
			return nil, err
		}
		signeds = append(signeds, signed)
	}

	return signeds, nil
}

func (j *BQJob) importFileName() (string, error) {
	cfg, err := j.getConfig()
	if err != nil {
		return "", err
	}
	if cfg.Load == nil || len(cfg.Load.SourceUris) == 0 {
		return "", fmt.Errorf("Missing Load.SourceUris.")
	}
	_, fn, err := gcs.ParseGcsUri(cfg.Load.SourceUris[0])
	if err != nil {
		return "", err
	}
	return fn, nil
}

func dryRunJob(job *BQJob, m *Model) error {
	conf, err := job.getConfig()
	if err != nil {
		return err
	}

	conf.DryRun = true

	_, err = m.bq.StartJob(conf)

	return err
}

func submitJob(job *BQJob, m *Model) error {
	conf, err := job.getConfig()
	if err != nil {
		return err
	}

	bqJob, err := m.bq.StartJob(conf)
	if err != nil {
		if strings.Contains(err.Error(), "server encountered a temporary error") {
			log.Printf("submitJob(): 'server encountered a temporary error', retrying in 10s")
			time.Sleep(10 * time.Second)
			bqJob, err = m.bq.StartJob(conf)
			if err != nil {
				return err
			}
		}
		return err
	}

	// Most importantly this should write the bq_job_id
	job = updateJobWithBQData(job, bqJob)
	if err = m.db.UpdateBQJob(job); err != nil {
		log.Printf("submitJob() error, bailing: %#v\n", err)
		return err
	}
	log.Printf("Submitted BQ job %v (table %d).", job.BQJobId, job.TableId)
	return nil
}

func marshalStats(stats *bigquery.JobStatistics) (query, load, extract *string) {
	if stats.Query != nil {
		b, _ := stats.Query.MarshalJSON()
		s := string(b)
		query = &s
	}

	if stats.Load != nil {
		b, _ := stats.Load.MarshalJSON()
		s := string(b)
		load = &s
	}

	if stats.Extract != nil {
		b, _ := stats.Extract.MarshalJSON()
		s := string(b)
		extract = &s
	}

	return
}

func buildExtractDestinationUris(uris []string, nFiles []int64) []string {
	result := make([]string, 0, len(uris))
	for i, uri := range uris {
		if !strings.Contains(uri, "*") {
			result = append(result, uri)
		} else {
			for n := int64(0); n < nFiles[i]; n++ {
				result = append(result, strings.Replace(uri, "*", fmt.Sprintf("%012d", n), 1))
			}
		}
	}
	return result
}

// Update our BQJob stucture with data from bigquery.Job.
// If job is nil, it is created.
func updateJobWithBQData(job *BQJob, bqJob *bigquery.Job) *BQJob {
	if job == nil {
		job = new(BQJob)
	}

	// JobId - it might be in project:job_id format
	jid := bqJob.Id
	if strings.Contains(jid, ":") {
		parts := strings.Split(jid, ":")
		jid = parts[len(parts)-1]
	}
	if strings.Contains(jid, ".") {
		parts := strings.Split(jid, ".")
		jid = parts[len(parts)-1]
	}
	job.BQJobId = jid

	// QueryStats, LoadStats, ExtractStats
	if bqJob.Statistics != nil {
		query, load, extract := marshalStats(bqJob.Statistics)
		job.QueryStats = query
		job.LoadStats = load
		job.ExtractStats = extract
	}

	// Configuration
	if bqJob.Configuration != nil {
		b, _ := bqJob.Configuration.MarshalJSON()
		s := string(b)
		job.Configuration = &s

		// DestinationUrls (Extract only)
		if bqJob.Configuration.Extract != nil && len(bqJob.Configuration.Extract.DestinationUris) > 0 {
			if bqJob.Statistics.Extract != nil {
				uris := buildExtractDestinationUris(bqJob.Configuration.Extract.DestinationUris, bqJob.Statistics.Extract.DestinationUriFileCounts)
				b, _ := json.Marshal(uris)
				s := string(b)
				job.DestinationUrls = &s
			}
		}
	}

	// Status
	if bqJob.Status != nil {
		b, _ := bqJob.Status.MarshalJSON()
		s := string(b)
		job.Status = &s
	}
	job.cacheStatus()

	// TotalBytesProcessed
	job.TotalBytesProcessed = bqJob.Statistics.TotalBytesProcessed

	// CreationTime
	if bqJob.Statistics.CreationTime != 0 {
		job.CreationTime = timeFromMs(bqJob.Statistics.CreationTime)
	}

	// StartTime
	if bqJob.Statistics.StartTime != 0 {
		job.StartTime = timeFromMs(bqJob.Statistics.StartTime)
	}

	// EndTime BQ Docs: "This field will be present whenever a job is
	// in the DONE state."
	if bqJob.Statistics.EndTime != 0 {
		job.EndTime = timeFromMs(bqJob.Statistics.EndTime)
	}

	// TotalBytesBilled
	if bqJob.Statistics.Query != nil {
		job.TotalBytesBilled = bqJob.Statistics.Query.TotalBytesBilled
	}

	return job
}

// Exponential back-off with sleep.
func pause(b time.Duration) time.Duration {
	const (
		maxBackoff          = 15000
		baseBackoff         = 250
		backoffGrowthFactor = 1.8
		backoffGrowthDamper = 0.25
	)

	bo := float64(b / 1e6)
	bo *= backoffGrowthFactor
	bo -= (bo * rand.Float64() * backoffGrowthDamper)
	bo = math.Min(bo, maxBackoff)

	delay := time.Duration(bo) * time.Millisecond
	time.Sleep(delay)
	return delay
}

func resubmit(m *Model, job *BQJob, delay time.Duration) error {
	// Clear out the status or the run will fail.
	job.Status, job.EndTime = nil, nil
	if err := m.db.UpdateBQJob(job); err != nil {
		return err
	}
	time.Sleep(delay)
	job.Status, job.state, job.error, job.EndTime = nil, "", "", nil
	if err := submitJob(job, m); err != nil {
		return err
	}
	log.Printf("resubmit() on re-submit, new job_id: %s (%d).", job.BQJobId, job.Id)
	return nil
}

func reimport(m *Model, t *Table, job *BQJob) error {
	// Send a slack alert
	m.SlackAlert(fmt.Sprintf("Re-importing due to schema change in %s: %v",
		"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), job.error))
	m.DeleteImportStatus(t.Id)
	if err := m.ReimportTable(t, job.UserId, job.RunId); err != nil {
		return err
	}
	// Set the error anyway, as a deterrent from this thing re-spawning itself ad infinitum
	// This particular error will be cleared in processJobDone
	// TODO: There should be a better way to do this
	if err := t.setError(m, fmt.Errorf(job.error)); err != nil {
		return err
	}
	return nil
}

func extractToGCS(m *Model, t *Table, job *BQJob) (string, error) {
	extrJobId, err := m.ExtractTableToGCS(t, job.UserId, job.RunId)
	if err != nil {
		t.setError(m, err)
		m.SlackAlert(fmt.Sprintf("extractToGCS() GCS extract error in %s: %v.",
			"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), err))
	}
	return extrJobId, err
}

func extractToSheet(m *Model, t *Table) error {
	err := m.ExtractTableToSheet(t)
	if err != nil {
		t.setError(m, err)
		m.SlackAlert(fmt.Sprintf("Sheets extract error in %s: %v.",
			"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), err))
	}
	return err
}

func exportTable(m *Model, t *Table) error {
	err := m.ExportTable(t)
	if err != nil {
		t.setError(m, err)
		m.SlackAlert(fmt.Sprintf("Export to db error in %s: %v",
			"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), err))
	}
	return err
}

func loadCleanup(m *Model, job *BQJob, t *Table) error {
	cfg, err := job.getConfig()
	if err == nil {
		log.Printf("loadCleanup(): deleting %v", cfg.Load.SourceUris)
		if err = m.gcs.DeleteFiles(cfg.Load.SourceUris); err != nil {
			log.Printf("loadCleanup(): error deleting: %v", err)
			// We do not return this error, just keep going
		}
	}
	if t.IdColumn != "" && t.Disposition == "WRITE_TRUNCATE" {
		// A special case for reimports when there is an IdColumn, yet the disposition is truncate,
		// (can only happen if this is a reimport) make sure to set it to append in the table config.
		t.Disposition = "WRITE_APPEND"
	}
	t.setImportedAt(m, time.Now()) // This does a SaveTable
	return err
}

func postCompletionQuery(m *Model, t *Table, job *BQJob) {
	if t.Extract || t.ExportDbId != 0 { // Should we also start a GCS extract job?
		extrJobId, err := extractToGCS(m, t, job)
		if err != nil {
			log.Printf("postCompletion() error submitting GCS extract, bailing: %v.", err)
			return
		}
		log.Printf("Submitted BQ extract %q for table id: %v OK.", extrJobId, t.Id)
	}
	if t.SheetsExtract { // Should we copy this table to a sheet?
		if err := extractToSheet(m, t); err != nil {
			log.Printf("postCompletion() error submitting sheets extract for table %d, bailing: %v.", t.Id, err)
			return
		}
		log.Printf("Submitted a Sheets extract for table id: %v OK.", t.Id)
	}
}

func postCompletionExtract(m *Model, t *Table, job *BQJob) {
	if err := m.notifyCompletion(t, job); err != nil {
		// NB: This error may be nil even if notify failed, see model.go
		log.Printf("postCompletion() notify error (ignoring): %v.", err)
	}
	if t.ExportDbId != 0 { // Should also export this table to a database?
		if err := exportTable(m, t); err != nil {
			log.Printf("postCompletion() error exporting table %d to db: %v, bailing.", t.Id, err)
			return
		}
		log.Printf("Submitted export to db for table id: %v OK.", t.Id)
	}
}

func postCompletionLoad(m *Model, t *Table, job *BQJob) {
	// Delete the GCS files as a nice clean-up gesture
	if err := loadCleanup(m, job, t); err != nil {
		log.Printf("monitorJob(): cleanup error (ignoring): %v.", err)
	}
}

func processJobDone(m *Model, job *BQJob) {
	// If we got this far, the job is complete without API "Retrying"
	// API errors, but possibly with BQ errors.

	// Save the job in the database.
	if err := m.db.UpdateBQJob(job); err != nil {
		log.Printf("processJobDone() db error, bailing: %v", err)
		return
	}
	log.Printf("Job %s (%d) complete (table: %d) at %v.", job.BQJobId, job.Id, job.TableId, job.EndTime)

	// Load the table so that we can update its status
	t, err := m.db.SelectTable(job.TableId)
	if err != nil {
		log.Printf("processJobDone() db error, bailing: %v.", err)
		return
	}
	if t == nil { // This should never happen?
		log.Printf("processJobDone() warning: table_id %v not found, bailing.", job.TableId)
		return
	}

	// The import (if this was one) is done, delete the import status
	// (same as IMP_NONE)
	m.DeleteImportStatus(t.Id)

	if job.error != "" { // We have errors

		// If this is a load job and there is a schema error, re-import the table
		// TODO: This should be optional based on Table config
		if job.Type == "load" && strings.Contains(job.error, bqSchemaErr) {
			// NB: The t.Error == "" is critical so as to not reimport ad infinitum
			if t.Error == "" && job.RunId != nil && *job.RunId != 0 { // only do this as part of a run
				log.Printf("Schema change error in table %v (%d), reimporting.", t.Name, t.Id)
				if err = reimport(m, t, job); err != nil {
					log.Printf("processJobDone() reimport error: %v", err)
				}
				return
			}
		}

		if err := t.setError(m, fmt.Errorf(job.error)); err != nil {
			log.Printf("processJobDone() db error, bailing: %v.", err)
			return
		}

		// Report the error to log and slack
		m.SlackAlert(fmt.Sprintf("BigQuery error in %s: %v",
			"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), t.Error))
		log.Printf("Job %s (%d table: %d) BigQuery error %v.", job.BQJobId, job.Id, job.TableId, t.Error)

		return // No further processing because of errors
	}

	// No errors

	if strings.Contains(t.Error, bqSchemaErr) { // Table was reimported
		// NB: This error was in the table already from the previous job
		t.Error = "" // Clear the error
	}

	// Mark it as not running
	if err := t.setRunning(m, false); err != nil { // Calls SaveTable()
		log.Printf("processJobDone() db error, bailing: %v.", err)
		return
	}

	// If there are no errors and this does not require a GCS extract, set the last okay run
	if t.Error == "" && ((job.Type == "query" && !t.Extract) || job.Type != "query") {
		// NB: Sheet extract is not a BQ job, it is done by Maestro.
		// If there is a GCS extract, when it is dome, it will fall into the job.Type != "query"
		// at which point the lastRunOk will get set.
		if err := t.setLastOkRunEndAt(m, time.Now()); err != nil {
			log.Printf("processJobDone() db error, bailing: %v.", err)
			return
		}
	}

	// Post-completion steps
	switch job.Type {
	case "query":
		postCompletionQuery(m, t, job)
	case "extract":
		postCompletionExtract(m, t, job)
	case "load":
		postCompletionLoad(m, t, job)
	default:
		log.Printf("processJobDone(): Invalid job type: %v", job.Type)
	}
}

func waitForJob(m *Model, job *BQJob) *BQJob {
	backoff, errCnt := time.Second, 0
	for {
		backoff = pause(backoff)
		bqJob, err := m.bq.GetJob(job.BQJobId)
		if err != nil { // This must be a transient API error - try again later
			log.Printf("waitForJob() BQ API ERROR (try %d, will retry in ~ %v): %v", errCnt+1, backoff, err)
			errCnt++
			continue
		}
		job = updateJobWithBQData(job, bqJob)
		if job.EndTime != nil && !job.EndTime.IsZero() {
			// If the above is true, the job is done (possibly with errors).
			return job
		}
	}
	panic("unreachable")
}

// Starts monitoring a job with BQ. If we do not have a BQJob in the
// database, one is created, otherwise tableId and userId are
// ignored. Therefore if we know that a job definitely already exists,
// then we can just pass zeros for tableId and userId.
func monitorJob(job *BQJob, m *Model) {

	job = waitForJob(m, job)

	// NB: The job isn't saved in the database yet, this is because we
	// do not want to record a bqRetryingErr Status in the db because
	// it can fail a run (because runs check for job.error). We
	// resubmit on bqRetryingErr without ever saving the job.

	// If we get a "Retrying may solve the problem" error, re-submit
	// the job again after a little sleep
	if job.Status != nil && strings.Contains(*job.Status, bqRetryingErr) {
		log.Printf("monitorJob: Job %s (%d) backendError, retrying in %v.", job.BQJobId, job.Id, apiErrorRetryDelay)
		if err := resubmit(m, job, apiErrorRetryDelay); err != nil { // saves the job in db
			log.Printf("monitorJob() resubmit error, bailing: %v.", err)
		} else {
			log.Printf("monitorJob() on re-submit, new job_id: %s (%d).", job.BQJobId, job.Id)
		}
		return
	}

	processJobDone(m, job)
}

func checkForError(bqJob *bigquery.Job) error {
	if bqJob.Status != nil && bqJob.Status.ErrorResult != nil {
		return fmt.Errorf(bqJob.Status.ErrorResult.Message)
	}
	return nil
}

// This is meant to be called once on startup to check the status of
// all the jobs that were for whatever reason left unfinished from the
// last run.
func monitorUnfinishedJobs(m *Model) {

	log.Printf("Checking for unfinished jobs...")

	jobs, err := m.db.RunningBQJobs()
	if err != nil {
		log.Printf("monitorUnfinishedJobs(): error %v", err)
	}

	if len(jobs) == 0 {
		log.Printf("No unfinished jobs.")
		return
	}

	for _, j := range jobs {
		go monitorJob(j, m)
		log.Printf("Started monitor for BQ job %q (table: %d)", j.BQJobId, j.TableId)
	}
}
