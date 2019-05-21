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

	"relative/dbsync"

	"github.com/jmoiron/sqlx"
	bigquery "google.golang.org/api/bigquery/v2"
)

type importStatus int

// Import status - phases of the database import.
const (
	ImpNone importStatus = iota // 0
	ImpQueued
	ImpRunning
	ImpDone
	ImpError
)

type importStatusMap map[int64]importStatus

type importJob struct {
	table       *Table
	userId      *int64
	fname       string
	job         *BQJob
	schema      *bigquery.TableSchema
	importBegin time.Time
	importEnd   time.Time
	importBytes int64
	importRows  int64
}

func (i *importJob) markError(m *Model, note string, err error) {
	log.Printf("importTableJob() error: %v", err)
	m.setImportStatus(i.table.Id, ImpError)
	m.SlackAlert(fmt.Sprintf("importTableJob() error in %s: [%s] %v",
		"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", i.table.Id, i.table.Name), note, err))
	i.table.setError(m, err)
}

func (i *importJob) run(m *Model) (err error) {
	i.schema, err = i.runImport(m)
	if err != nil {
		return err
	}
	return i.runLoad(m)
}

func (i *importJob) runImport(m *Model) (*bigquery.TableSchema, error) {
	t := i.table

	m.setImportStatus(t.Id, ImpRunning)

	log.Printf("Importing table %s", t.Name)

	// mark the table as running
	if err := t.setRunning(m, true); err != nil {
		return nil, err
	}

	idb, err := m.SelectDbConf(t.ImportDbId)
	if err != nil {
		i.markError(m, "SelectDbConf", err)
		return nil, err
	}

	dbConn, err := sqlx.Connect(idb.Driver, idb.ConnectStr)
	if err != nil {
		i.markError(m, "sqlx.Connect", err)
		return nil, err
	}
	defer dbConn.Close()

	var stmt dbsync.PrimitiveSelect
	if t.Query == "" {

		stmt = dbsync.PrimitiveSelect{
			Select: "*",
			From:   t.Name,
		}

	} else {

		if err := json.Unmarshal([]byte(t.Query), &stmt); err != nil {
			return nil, err
		}

		tables := tablesInQuery(stmt.String())
		if len(tables) > 1 {
			err := fmt.Errorf("Import tables must select from only one table, we have %d: %v", len(tables), tables)
			// TODO: We should also disallow stuff like ORDER BY
			i.markError(m, "len(tables) > 1", err)
			return nil, err
		}
	}

	now := time.Now() // TODO: This should run time?

	if t.reimportCondSatisfied(now) {
		log.Printf("Reimporting %v because reimport_cond is satisfied.", t.Name)
		t.LastId = "" // This will cause a reimport
	}

	i.importBegin = now
	tr, err := dbsync.NewTableReader(dbConn, idb.Driver, t.Name, &stmt, t.IdColumn, t.LastId, false)
	if err != nil {
		i.markError(m, "dbsync.NewTableReader", err)
		return nil, err
	}
	log.Printf("Table reader with: %s", tr.Statement())

	// Report count every 1M rows
	tr.CountReport(func(c, b int64) {
		dur := time.Now().Sub(i.importBegin)
		log.Printf("Importing %s: %d rows (%d/s) %d bytes (%d/s)", t.Name, c, int(float64(c)/dur.Seconds()), b, int(float64(b)/dur.Seconds()))
	}, 1e6)

	log.Printf("Getting BQ Schema...")
	schema, err := tr.GetBQSchema(dbConn, stmt)
	if err != nil {
		i.markError(m, "tr.GetBQSchema", err)
		return nil, err
	}
	log.Printf("Getting BQ Schema DONE.")

	// This blocks for a long time.
	obj, err := m.gcs.Insert(m.ctx, i.fname, tr)
	if err != nil {
		if strings.Contains(err.Error(), "server encountered a temporary error") {
			log.Printf("runImport(): 'server encountered a temporary error' (table %d), retrying in 10s", t.Id)
			time.Sleep(10 * time.Second)
			obj, err = m.gcs.Insert(m.ctx, i.fname, tr)
		}
		if err != nil {
			i.markError(m, "m.gcs.Insert", err)
			return nil, err
		}
	}

	log.Printf("Extract of %s (%d) to GCS complete: %v", t.Name, t.Id, obj.SelfLink)

	if err := t.setLastId(m, tr.LastId()); err != nil {
		return nil, err
	}

	m.setImportStatus(t.Id, ImpDone)

	i.importEnd = time.Now()
	i.importBytes = tr.Bytes()
	i.importRows = tr.Rows()

	return schema, nil
}

func (i *importJob) runLoad(m *Model) (err error) {

	t, userId := i.table, i.userId

	if m.GetImportStatus(t.Id) != ImpDone {
		err := fmt.Errorf("Load started without prior successful import.")
		i.markError(m, "m.GetImportStatus", err)
		return err
	}

	if i.importRows == 0 {
		log.Printf("WARNING: No (new) data for table %s.", t.Name)
	}

	log.Printf("Starting BigQuery load for table %s (%d).", t.Name, t.Id)

	now := time.Now() // TODO: This should run time?

	gcsUrl := m.gcs.UrlForName(i.fname)
	if i.job == nil {
		// interactive job, runId will be nil
		ds, err := t.ImportDataset(m)
		if err != nil {
			return err
		}
		job := t.newBQLoadJob(m.bq, userId, nil, []string{gcsUrl}, ds, now)
		job.ImportBegin = &i.importBegin
		job.ImportEnd = &i.importEnd
		job.ImportBytes = i.importBytes
		job.ImportRows = i.importRows
		job, err = m.InsertBQJob(job)
		if err != nil {
			i.markError(m, "m.InsertBQJob", err)
			return err
		}
		i.job = job
	} else {
		i.job.ImportBegin = &i.importBegin
		i.job.ImportEnd = &i.importEnd
		i.job.ImportBytes = i.importBytes
		i.job.ImportRows = i.importRows
		if err = m.db.UpdateBQJob(i.job); err != nil {
			return err
		}
	}

	// Set the schema in the job
	if err := i.job.setLoadJobSchema(i.schema); err != nil {
		i.markError(m, "i.job.setLoadJobSchema", err)
		return err
	}

	if err := submitJob(i.job, m); err != nil {
		i.markError(m, "submitJob", err)
		return err
	}

	go monitorJob(i.job, m)
	log.Printf("importTableJob: started monitor for BQ load job %q.", i.job.BQJobId)

	return nil
}
