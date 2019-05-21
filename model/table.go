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
	"path"
	"time"

	"relative/bq"
	"relative/scheduler"

	"google.golang.org/api/bigquery/v2"
	"gopkg.in/yaml.v2"
)

// All the information about a Table. Note the absence of "db" tags,
// this struct is stored in the database by way of an intermediate
// internal struct, see the db package.
type Table struct {
	Id               int64
	UserId           int64                  // This user created the table
	GroupId          int64                  // Group to which this table belongs
	Email            string                 // from joined users table
	Name             string                 // Table name (without dataset)
	Dataset          string                 // from joined datasets table
	DatasetId        int64                  // The dataset
	Query            string                 // The SQL
	Disposition      string                 // e.g. WRITE_TRUNCATE or WRITE_APPEND
	Partitioned      bool                   // BigQuery DAY partitioning
	LegacySQL        bool                   // Standard SQL if false
	Description      string                 // Some descriptive text. TODO: make it searchable?
	Error            string                 // If the last run ended in error
	Running          bool                   // True if the table is currently running
	Extract          bool                   // Make a GCS extract
	NotifyExtractUrl string                 // The URL to notify upon extract completion
	SheetsExtract    bool                   // Make a sheets extract
	SheetId          string                 // The sheet id
	ExportDbId       int64                  // Export this table to this database
	ExportTableName  string                 // Use this as table name when exporting
	FreqId           int64                  // Run this table periodically
	Conditions       []*scheduler.Condition // Run only when this condition is satisfied
	ExternalTmout    int64                  // External table only: timeout
	ExternalFormat   string                 // CSV or NEWLINE_DELIMITED_JSON
	ImportDbId       int64                  // Import from this database
	ImportedAt       time.Time              // Last import time
	IdColumn         string                 // Column for incremental imports
	ReimportCond     []*scheduler.Condition // Reimport if this condition satisfied and imported_at does not satisfy it
	LastId           string                 // Last value of IdColumn for incremental imports

	CreatedAt      time.Time
	DeletedAt      time.Time
	LastOkRunEndAt time.Time // Used by pythonlib

	importDs string // cached value
}

// Satisfy scheduler.Item, used when building the DAG.
func (t *Table) GetName() string { return t.Name }
func (t *Table) parentNames() []string {
	if t.IsImport() {
		return nil
	}
	return tablesInQuery(t.Query)
}

func (t *Table) IsImport() bool   { return t.ImportDbId != 0 }
func (t *Table) IsExternal() bool { return t.ExternalTmout != 0 }

// Return the dataset associated with the import database.
func (t *Table) ImportDataset(m *Model) (string, error) {
	if !t.IsImport() {
		return "", fmt.Errorf("Not an import table.")
	}
	if t.importDs != "" {
		return t.importDs, nil
	}
	idbs, err := m.importDbsAsMap()
	if err != nil {
		return "", err
	}
	t.importDs = idbs[t.ImportDbId].Dataset
	return t.importDs, nil
}

// Return the latest extract BQJob. This is where the GCS URLs come
// from.
func (t *Table) LastExtractJob(m *Model) (*BQJob, error) {
	jobs, err := m.SelectBQJobsByTableId(t.Id, 0, 10)
	if err != nil {
		return nil, err
	}
	var last *BQJob
	for _, job := range jobs {
		if job.Type == "extract" {
			last = job
			break
		}
	}
	return last, nil
}

// Get table information from the BigQuery API.
func (t *Table) GetBQInfo(m *Model) (*bigquery.Table, error) {
	return m.bq.GetTable(t.Dataset, t.Name)
}

func (t *Table) newBQQueryJob(bq *bq.BigQuery, userId, runId *int64, pids []int64) *BQJob {
	job := &BQJob{
		TableId: t.Id,
		UserId:  userId,
		RunId:   runId,
		Type:    "query",
	}
	job.setConfig(bq.NewQueryJobConfiguration(t.Query, t.Dataset, t.Name, t.Disposition, t.LegacySQL, t.Partitioned))
	if len(pids) > 0 {
		b, _ := json.Marshal(pids)
		s := string(b)
		job.Parents = &s
	}
	return job
}

func (t *Table) newBQExtractJob(bq *bq.BigQuery, userId, runId *int64) *BQJob {
	job := &BQJob{
		TableId: t.Id,
		UserId:  userId,
		RunId:   runId,
		Type:    "extract",
	}
	job.setConfig(bq.NewExtractJobConfiguration(t.Dataset, t.Name))
	return job
}

func (t *Table) newBQLoadJob(bq *bq.BigQuery, userId, runId *int64, gcsUrls []string, dataset string, now time.Time) *BQJob {
	job := &BQJob{
		TableId: t.Id,
		UserId:  userId,
		RunId:   runId,
		Type:    "load",
	}
	wdisp := t.Disposition
	if t.reimportCondSatisfied(now) {
		wdisp = "WRITE_TRUNCATE"
	}
	format := "CSV" // assume IsImport() true
	if t.IsExternal() {
		format = t.ExternalFormat
	}
	job.setConfig(bq.NewLoadJobConfiguration(t.Name, dataset, wdisp, gcsUrls, format))
	return job
}

// Queue up a git commit, which will eventually happen in a separate
// goroutine. The tables are stored in Git by Marshaling the Table
// struct as YAML.
func (t *Table) QueueGitCommit(m *Model, email string) error {
	content, err := yaml.Marshal(t)
	if err != nil {
		return err
	}

	comment := fmt.Sprintf("Table %s updated.", t.Name)
	name := "" // TODO we don't know the user's name

	m.git.QueueCommit(t.relpath(), content, comment, name, email, time.Now())
	return nil
}

// Construct a valid GitHub URL to this particular table
func (t *Table) GithubUrl(m *Model) string {
	u := m.git.Url()
	u.Path = path.Join(u.Path, "blob/master", t.relpath())
	return u.String()
}

func (t *Table) relpath() string {
	return path.Join("tables", idToPath(t.Id), fmt.Sprintf("%d.yaml", t.Id))
}

func (t *Table) conditionsSatisfied(now time.Time) bool {
	if len(t.Conditions) == 0 { // empty == satisfied
		return true
	}
	for _, c := range t.Conditions {
		if c.Satisfied(now) {
			return true
		}
	}
	return false
}

func (t *Table) reimportCondSatisfied(now time.Time) bool {
	// As a simple work-around, a reimport condition can only be
	// satisfied once per day. (TODO). The logic is: if the condition
	// is satisfied AND imported_at does NOT satisfy it, then it is OK
	// to reimport.
	//
	// NOTE: unlike run cond, this is NOT satisfied if empty
	ok := false
	for _, c := range t.ReimportCond {
		if c.Satisfied(now) {
			ok = true
			break
		}
	}
	if ok {
		for _, c := range t.ReimportCond {
			if c.Satisfied(t.ImportedAt) {
				return false
			}
		}
		return true
	}
	return false
}

func (t *Table) setRunning(m *Model, running bool) error {
	t.Running = running
	return m.SaveTable(t)
}

// Set an error, and mark the table as not running
func (t *Table) setError(m *Model, err error) error {
	if err != nil {
		t.Running, t.Error = false, err.Error()
		return m.SaveTable(t)
	}
	return nil
}

func (t *Table) setImportedAt(m *Model, imported time.Time) error {
	t.ImportedAt = imported
	return m.SaveTable(t)
}

func (t *Table) setLastId(m *Model, id string) error {
	t.LastId = id
	return m.SaveTable(t)
}

func (t *Table) setLastOkRunEndAt(m *Model, now time.Time) error {
	t.LastOkRunEndAt = now
	return m.SaveTable(t)
}

// Create a signed PUT-only GCS URL (for external table).
func (t *Table) SignedUploadURL(m *Model) string {
	filename := fmt.Sprintf("%s_%s_%d.json", t.Dataset, t.Name, time.Now().Unix())
	signed, _ := m.bq.SignedStorageUrl(filename, "PUT")
	return signed
}

// Use the first two digits as path
func idToPath(id int64) string {
	two := fmt.Sprintf("%02d", id)
	return path.Join(two[0:1], two[1:2])
}

func (t *Table) hasExternalWait(m *Model) bool {
	m.x.Lock()
	defer m.x.Unlock()
	_, ok := m.externalWaits[t.Id]
	return ok
}

func (t *Table) startExternalWait(m *Model, job *BQJob) error {
	if !t.IsExternal() {
		return fmt.Errorf("Not an external table")
	}

	if t.hasExternalWait(m) {
		return fmt.Errorf("ExternalWait for table id %d already exists.", t.Id)
	}

	if err := t.setRunning(m, true); err != nil {
		return err
	}

	m.x.Lock()
	defer m.x.Unlock()

	ew := &externalWait{make(chan bool), job}
	m.externalWaits[t.Id] = ew
	go func(ch chan bool, tmout time.Duration) {
		select {
		case <-ch:
		case <-time.After(tmout):
			t.setError(m, fmt.Errorf("External wait timed out after %v", tmout))
		}
	}(ew.ch, time.Duration(t.ExternalTmout)*time.Second)

	return nil
}

func (t *Table) cancelExternalWait(m *Model) *BQJob {
	m.x.Lock()
	defer m.x.Unlock()
	ew, ok := m.externalWaits[t.Id]
	if !ok {
		return nil // nothing to do
	}
	close(ew.ch)
	delete(m.externalWaits, t.Id)
	return ew.job
}

// Load table from external data. fname is the GCS file name (without
// bucket), userId is the user triggering this action, if any.
func (t *Table) ExternalLoad(m *Model, fname string, userId *int64) (err error) {

	job := t.cancelExternalWait(m)

	if job == nil {
		if t.ExternalFormat == "" {
			t.ExternalFormat = "CSV"
		}
		// NB: Autodetect is set true by newBQLoadJob
		job = t.newBQLoadJob(m.bq, userId, nil, nil, t.Dataset, time.Now())
		job, err = m.InsertBQJob(job)
		if err != nil {
			return err
		}
	}

	gcsUrl := m.gcs.UrlForName(fname)
	job.setLoadJobGcsUris([]string{gcsUrl})

	log.Printf("BigQuery external load for table %s from %s", t.Name, gcsUrl)
	if err := submitJob(job, m); err != nil {
		return err
	}

	go monitorJob(job, m)
	log.Printf("ExternalLoad: started monitor for BQ load job %q", job.BQJobId)

	return nil
}

func (t *Table) importFileName(runId *int64, now time.Time) string {
	if runId != nil {
		return fmt.Sprintf("%s_%s__run_%d__%d.csv", t.Dataset, t.Name, *runId, now.Unix())
	}
	return fmt.Sprintf("%s_%s_%d.csv", t.Dataset, t.Name, now.Unix())

}
