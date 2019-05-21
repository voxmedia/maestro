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

// Package model is the core of Maestro. Model interfaces with all the
// other packages and a *Model (conventionally *m) is frequently
// passed around across other packages.
//
// The model also defines core structures, many of which are persisted
// in the Maestro database.
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"relative/bq"
	"relative/gcs"
	"relative/git"
	"relative/gsheets"
	"relative/slack"
)

// Everything Maestro does is done via the Model. It contains
// refrences to all other things, such as database, BigQuery, external
// databases, etc, etc.
type Model struct {
	db
	bq            *bq.BigQuery
	gcs           *gcs.GCS
	allowedDomain string
	git           *git.Git
	slk           *slack.Config
	gsh           *gsheets.GSheets

	// For import jobs
	ctx           context.Context
	cancel        context.CancelFunc
	wg            *sync.WaitGroup
	imports       importStatusMap
	importQueue   chan *importJob
	externalWaits map[int64]*externalWait
	x             *sync.Mutex
}

type externalWait struct {
	ch  chan bool
	job *BQJob
}

// Create a new Model. Db is an implementation of the db interface,
// domain is the domain to which OAuth access will be restricted, bq,
// gcs, gsh, gt and slk are instances of structs docuemnted in
// corresponding packages.
func New(db db, domain string, bq *bq.BigQuery, gcs *gcs.GCS, gsh *gsheets.GSheets, gt *git.Git, slk *slack.Config) *Model {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Model{
		db:            db,
		allowedDomain: domain,
		bq:            bq,
		gcs:           gcs,
		gsh:           gsh,
		git:           gt,
		slk:           slk,
		ctx:           ctx,
		cancel:        cancel,
		wg:            new(sync.WaitGroup),
		imports:       make(importStatusMap),
		importQueue:   make(chan *importJob, 1024),
		externalWaits: make(map[int64]*externalWait),
		x:             new(sync.Mutex),
	}

	go monitorUnfinishedJobs(m)
	go monitorUnfinishedRuns(m)
	go triggerRuns(m)

	const WORKERS = 6
	for i := 0; i < WORKERS; i++ {
		m.wg.Add(1)
		go importWorker(m)
	}

	if err := m.createDefaultDataset(); err != nil {
		return nil
	}

	return m
}

// Gracefully stop Maestro, waiting for things that we can wait for to
// finish. Database imports are not waited on, those transaction will
// be rolled back. We also do not wait for any BigQuery jobs, they
// just carry on and their status will be checked when Maestro is
// started next.
func (m *Model) Stop() {
	log.Printf("Closing the queue channel...")
	close(m.importQueue)
	for _ = range m.importQueue { // drain the channel
	}

	log.Printf("Stopping any pending imports...")
	m.cancel()
	m.wg.Wait()
	log.Printf("Imports stopped")
}

// Set the domain to which OAuth is restricted.
func (m *Model) SetAllowedDomain(domain string) {
	m.allowedDomain = domain
}

// Check wether the user is valid based on email. If the user does not
// exist, it is created as disabled and a slack notification is
// sent. Email can be blank, in which case only the oAuthId is checked
// and the user cannot be created.
//
// The very first user ever created (id == 1) is automatically made
// admin and not disabled.
func (m *Model) ValidUser(oAuthId, email string) int64 {
	if m.allowedDomain == "" {
		log.Printf("WARNING: AllowedDomain is blank, all OAuth authentication will fail.")
		return 0
	}

	email = strings.ToLower(email)
	if email != "" && !strings.HasSuffix(email, m.allowedDomain) {
		defer func(email string) {
			m.SlackAlert(fmt.Sprintf("WARNING: Maestro access denied (invalid domain) for: %s",
				email))
		}(email)
		return 0
	}

	user, created, err := m.SelectOrInsertUserByOAuthId(oAuthId, email)
	if err != nil {
		log.Printf("ValidUser(): error %v", err)
		return 0
	}

	if created {
		if user.Id == 1 {
			// Make first user admin and create default group
			if err := m.InsertGroup("default", 1); err != nil {
				log.Printf("ValidUser(): error %v", err)
				return 0
			}
			user.Admin = true
			user.Disabled = false
			user.Groups = append(user.Groups, &Group{Id: 1})
			if err := m.SaveUser(user); err != nil {
				log.Printf("ValidUser(): error %v", err)
				return 0
			}
		} else {
			defer func(user *User) {
				m.SlackAlert(fmt.Sprintf("New (disabled) Maestro user created: (%d) %s",
					user.Id, user.Email))
			}(user)
		}
	}

	if user.Disabled {
		log.Printf("ValidUser(): disabled user access denied id: %d", user.Id)
		return 0
	}

	// We need to check again because now we verify that the email
	// from the database is conforming.
	if strings.HasSuffix(user.Email, m.allowedDomain) {
		return user.Id
	}

	return 0
}

func timeFromMs(ms int64) *time.Time {
	t := time.Unix(ms/1e3, ms%1e3*1e6)
	return &t
}

const defaultDataset = "maestro"

// Create the default (first) dataset if it does not exist
func (m *Model) createDefaultDataset() error {
	dss, err := m.SelectDatasets()
	if err != nil {
		return err
	}
	if len(dss) > 0 { // Nothing to do
		return nil
	}
	_, err = m.InsertDataset(defaultDataset)
	return err
}

// DryRunTable is a good way to check syntax. This API call does not
// create a job, but returns immediately.
func (m *Model) DryRunTable(t *Table, userId *int64) error {
	// TODO: Capture and return data estimates?
	if t.IsImport() {
		return fmt.Errorf("Cannot dryrun import tables.")
	} else {
		return m.dryRunBQTable(t, userId)
	}
}

func (m *Model) dryRunBQTable(t *Table, userId *int64) error {
	if m.bq == nil {
		return fmt.Errorf("Cannot run without BigQuery configuration, please visit /admin/bq_config to set it up.")
	}
	if t.Running {
		return fmt.Errorf("Table already running.")
	}

	job := t.newBQQueryJob(m.bq, userId, nil, nil)

	return dryRunJob(job, m)
}

// Run a table, userId is who initiated the run (not always same as
// table owner). userId may be nil if this is an action triggered by
// Maestro, e.g. a periodic run.
func (m *Model) RunTable(t *Table, userId *int64) (jobId string, err error) {
	if t.IsImport() {
		err = m.runImportTable(t, userId)
	} else if t.IsExternal() {
		err = m.runExternalTable(t)
	} else {
		jobId, err = m.runBQTable(t, userId)
	}
	return jobId, err
}

func (m *Model) runBQTable(t *Table, userId *int64) (string, error) {
	if m.bq == nil {
		return "", fmt.Errorf("Cannot run without BigQuery configuration, please visit /admin/bq_config to set it up.")
	}
	if t.Running {
		return "", fmt.Errorf("Table already running.")
	}

	job := t.newBQQueryJob(m.bq, userId, nil, nil)
	job, err := m.InsertBQJob(job)
	if err != nil {
		return "", err
	}

	err = submitJob(job, m)
	if err != nil {
		// Query Errors do not go unrecorded!
		if err := t.setError(m, err); err != nil {
			log.Printf("RunTable() error: %v", err)
		}
		return "", err
	}

	if err := t.setRunning(m, true); err != nil {
		log.Printf("runBQTable() error: %v", err)
	}

	go monitorJob(job, m)
	log.Printf("runBQTable: started monitor for BQ job %q (table %d)", job.BQJobId, job.TableId)

	return job.BQJobId, nil
}

func (m *Model) runImportTable(t *Table, userId *int64) error {
	if err := m.queueImport(t, userId, nil); err != nil {
		return err
	}
	return nil
}

func (m *Model) runExternalTable(t *Table) error {
	// this sets running to true as well
	return t.startExternalWait(m, nil)
}

func (m *Model) ReimportTable(t *Table, userId *int64, runId *int64) (err error) {
	var job *BQJob
	if !t.IsImport() {
		return fmt.Errorf("Not an import table")
	}

	t.Disposition, t.LastId = "WRITE_TRUNCATE", ""
	if err = m.SaveTable(t); err != nil {
		return err
	}

	ds, err := t.ImportDataset(m)
	if err != nil {
		return err
	}

	now := time.Now()

	var runIdInt int64
	if runId != nil {
		runIdInt = *runId
	}
	name := fmt.Sprintf("%s_%s__run_%d__%d.csv", t.Dataset, t.Name, runIdInt, now.Unix())

	gcsUrl := m.gcs.UrlForName(name)
	job = t.newBQLoadJob(m.bq, userId, runId, []string{gcsUrl}, ds, now)
	if job, err = m.InsertBQJob(job); err != nil {
		return err
	}

	return m.queueImport(t, userId, job)
}

// Extract table to GCS. The userId is of the user initiating the
// action, can be nil if this is a part of a run. The runId can also
// be nil if this is not part of a run. Returns the BigQuery job id.
func (m *Model) ExtractTableToGCS(t *Table, userId *int64, runId *int64) (string, error) {
	if t.Running {
		return "", fmt.Errorf("Table already running.")
	}

	job := t.newBQExtractJob(m.bq, userId, runId)
	job, err := m.InsertBQJob(job)
	if err != nil {
		return "", err
	}

	if err := submitJob(job, m); err != nil {
		log.Printf("ExtractTableToGCS() error: %v", err)
		return "", err
	}

	if err := t.setRunning(m, true); err != nil {
		log.Printf("ExtractTableToGCS() error: %v", err)
	}

	go monitorJob(job, m)
	log.Printf("ExtractTableToGCS: started monitor for BQ extract job %q (table %d)", job.BQJobId, job.TableId)

	return job.BQJobId, nil
}

func (m *Model) notifyCompletion(t *Table, job *BQJob) error {

	if job.ExtractStats == nil || t.NotifyExtractUrl == "" {
		return nil // nothing to do
	}

	var urls []string
	if err := json.Unmarshal([]byte(*job.DestinationUrls), &urls); err != nil {
		return err
	}

	var signeds []string // NB: with double-quotes
	for _, url := range urls {
		_, filename := filepath.Split(url)
		signed, err := m.bq.SignedStorageUrl(filename, "GET")
		if err != nil {
			return err
		}
		signeds = append(signeds, fmt.Sprintf("%q", signed))
	}

	// Get the schema
	td, err := m.bq.GetTable(t.Dataset, t.Name)
	if err != nil {
		return err
	}
	schema, err := json.Marshal(td.Schema.Fields)
	if err != nil {
		return err
	}

	// NB: "String values encode as JSON strings coerced to valid
	// UTF-8", which means & becomes "\u0026". We do not use
	// json.Marshal because of this and construct the JSON manually.

	// TODO: deprecate extractUrl in favor of extractUrls
	jsonStr := []byte(fmt.Sprintf("{\"extractUrl\":%s,\n\"extractUrls\":[%s],\n\"schema\":%s,\n\"dataset\":%q,\n\"name\":%q}\n",
		signeds[0], strings.Join(signeds, ","), schema, t.Dataset, t.Name))

	start := time.Now()

	req, err := http.NewRequest("POST", t.NotifyExtractUrl, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)

	var respBody []byte
	var hdrStr string
	if err == nil {
		defer resp.Body.Close()
		respBody, _ = ioutil.ReadAll(resp.Body) // Ignore error
		hdrStr = fmt.Sprintf("%v", resp.Header)
	}

	duration := start.Sub(time.Now())

	var errStr *string
	if err != nil {
		m.SlackAlert(fmt.Sprintf("notifyCompletion() notify error in %s: %v",
			"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", t.Id, t.Name), err))
		s := err.Error()
		errStr = &s
	}
	notify := &Notification{
		TableId:     job.TableId,
		BqJobId:     job.Id,
		CreatedAt:   start,
		DurationMs:  duration.Nanoseconds() / 1e6,
		Error:       errStr,
		Url:         t.NotifyExtractUrl,
		Method:      "POST",
		Body:        string(jsonStr),
		RespHeaders: hdrStr,
		RespBody:    string(respBody),
	}
	if resp != nil {
		notify.RespStatusCode = resp.StatusCode
		notify.RespStatus = resp.Status
	}
	if err := m.LogNotification(notify); err != nil {
		return err
	}

	return nil
}

// Extract table to a Google Sheet. This extract uses the BigQuery
// paging API, so this only works well for small tables. If the
// spreadsheet already exists, the extract will be pre-pended as the
// first sheet (tab), and trailing sheets after a certain number
// (configurable in the gsheets package) are deleted.
func (m *Model) ExtractTableToSheet(t *Table) error {

	// Check the size of the table in BQ
	td, err := m.bq.GetTable(t.Dataset, t.Name)
	if err != nil {
		return err
	}

	// TODO: Make me configurable
	maxRows, maxBytes := uint64(200*1000), int64(20*1000*1000)

	if td.NumRows > maxRows {
		return fmt.Errorf("Maximum allowed rows for Sheets export is %d, this table has %d.", maxRows, td.NumRows)
	}

	if td.NumBytes > maxBytes {
		return fmt.Errorf("Maximum allowed size for Sheets export is %d, this table is %d.", maxBytes, td.NumBytes)
	}

	// Mark table as running
	if err := t.setRunning(m, true); err != nil {
		log.Printf("ExtractTableToSheet() error: %v", err)
		return err
	}

	// Create the call
	call := &extractToSheetsCall{
		m: m,
		t: t,
	}

	// Column headers
	for _, col := range td.Schema.Fields {
		call.headers = append(call.headers, col.Name)
	}

	go func(c *extractToSheetsCall) {
		if err := c.Do(); err != nil {
			log.Printf("ExtractTableToSheet error in Do(): %v", err)
		}
	}(call)

	return nil
}

// Export a BigQuery table to an external database. The export is done
// via GCS, which is much faster than the BigQuery paging API.
func (m *Model) ExportTable(t *Table) error {
	// Check the size of the table in BQ
	td, err := m.bq.GetTable(t.Dataset, t.Name)
	if err != nil {
		return err
	}

	// TODO: Make me configurable
	maxRows, maxBytes := uint64(100*1000*1000), int64(1000*1000*1000)

	if td.NumRows > maxRows {
		return fmt.Errorf("Maximum allowed rows for db export is %d, this table has %d.", maxRows, td.NumRows)
	}

	if td.NumBytes > maxBytes {
		return fmt.Errorf("Maximum allowed size for db export is %d, this table is %d.", maxBytes, td.NumBytes)
	}

	// The last extract job contains the URLs
	j, err := t.LastExtractJob(m)
	if err != nil {
		return err
	}

	if j == nil || j.DestinationUrls == nil {
		return fmt.Errorf("No GCS extracts found for this table.")
	}

	idb, err := m.SelectDbConf(t.ExportDbId)
	if err != nil {
		return err
	}

	if !idb.Export {
		return fmt.Errorf("Not an export db")
	}

	// Mark table as running
	if err := t.setRunning(m, true); err != nil {
		return err
	}

	// Create the call
	call := &exportToDbCall{
		m:   m,
		t:   t,
		job: j,
		idb: idb,
	}

	// We do not monitor what happens. If Maestro exists, at this
	// point, this will probably just be an uncommited transaction in
	// the DB.
	go func(c *exportToDbCall) {
		if err := c.Do(); err != nil {
			log.Printf("ExportTable error in Do(): %v", err)
		}
	}(call)

	return nil
}

// Based on the SQL, return a list of pointers to tables that we know
// about and that are in our project and dataset.
func (m *Model) singleTableParents(child *Table, all map[string]*Table) ([]*Table, error) {
	set := make(map[string]*Table)
	for _, fullName := range child.parentNames() {
		// TODO check org as well
		proj, ds, name := bq.ParseTableSpec(fullName)

		if proj != "" && proj != m.bq.ProjectId() {
			continue
		}

		if child.Dataset == ds && child.Name == name { // self-reference
			continue
		}

		dsName := ds + "." + name
		if t, ok := all[dsName]; ok {
			set[dsName] = t
		}
	}
	result := make([]*Table, 0, len(set))
	for _, t := range set {
		result = append(result, t)
	}
	return result, nil
}

// Provide parents for every child in the children map. This is more
// efficient if we need to extract parents from a whole list of
// tables.
func (m *Model) TablesParents(children []*Table) (map[string][]*Table, error) {
	all, err := m.tablesAsMap()
	if err != nil {
		return nil, err
	}
	result := make(map[string][]*Table)
	for _, child := range children {
		parents, err := m.singleTableParents(child, all)
		if err != nil {
			return nil, err
		}
		result[child.Name] = parents
	}
	return result, nil
}

// Build a map of all tables by name, where name is
// in the form "dataset.table_name"
func (m *Model) tablesAsMap() (map[string]*Table, error) {
	all, err := m.Tables("id", "ASC", "")
	if err != nil {
		return nil, err
	}
	idbs, err := m.importDbsAsMap()
	if err != nil {
		return nil, err
	}

	result := make(map[string]*Table)

	for _, t := range all {
		var name string
		if t.IsImport() {
			name = idbs[t.ImportDbId].Dataset + "." + t.Name
		} else {
			name = t.Dataset + "." + t.Name
		}
		result[name] = t
	}
	return result, nil
}

func (m *Model) importDbsAsMap() (map[int64]*Db, error) {
	idbs, err := m.SelectDbs()
	if err != nil {
		return nil, err
	}
	result := make(map[int64]*Db)
	for _, idb := range idbs {
		result[idb.Id] = idb
	}
	return result, nil
}

// Generate a Slack alert (provided Slack is configured).
func (m *Model) SlackAlert(msg string) error {
	if m.slk.Url != "" {
		return slack.SendNotification(m.slk, msg)
	} else {
		return fmt.Errorf("Slack not configured.")
	}
}

// Return import status for table id.
func (m *Model) GetImportStatus(id int64) importStatus {
	m.x.Lock()
	defer m.x.Unlock()
	return m.imports[id]
}

func (m *Model) setImportStatus(id int64, status importStatus) {
	m.x.Lock()
	defer m.x.Unlock()
	m.imports[id] = status
}

// Delete import status for table id.
func (m *Model) DeleteImportStatus(id int64) {
	m.x.Lock()
	defer m.x.Unlock()
	delete(m.imports, id)
}

func (m *Model) queueImport(t *Table, userId *int64, job *BQJob) (err error) {
	if m.GetImportStatus(t.Id) != ImpNone {
		return fmt.Errorf("Already queued, running or done")
	}
	var fname string
	if job == nil {
		fname = t.importFileName(nil, time.Now())
	} else {
		if fname, err = job.importFileName(); err != nil {
			return err
		}
	}
	imp := &importJob{
		table:  t,
		userId: userId,
		fname:  fname,
		job:    job,
	}
	select {
	case m.importQueue <- imp:
		m.setImportStatus(t.Id, ImpQueued)
		return nil
	}
	// TODO: If the queue is full - the table does not run?
	return fmt.Errorf("Import queue is full!?")
}

func importWorker(m *Model) {
	defer m.wg.Done()
	for imp := range m.importQueue {
		imp.run(m)
	}
}
