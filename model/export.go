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
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"relative/dbsync"
	"relative/gcs"

	"github.com/jmoiron/sqlx"
)

type exportToDbCall struct {
	m   *Model
	t   *Table
	job *BQJob
	idb *Db
}

func (e *exportToDbCall) markError(m *Model, note string, err error) error {
	log.Printf("exportToDbCall error: %v", err)
	m.SlackAlert(fmt.Sprintf("Export to db error in %s: [%s] %v",
		"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", e.t.Id, e.t.Name), note, err))
	e.t.setError(m, err)
	return err
}

// This is where the export to db happens. We do not have a wait lock
// because if Maestro quits, the worst that happens is we have a
// failed export and an uncommited transaction. (TODO: make this
// better).
func (e *exportToDbCall) Do() error {

	m, t, j, idb := e.m, e.t, e.job, e.idb

	dbConn, err := sqlx.Connect(idb.Driver, idb.ConnectStr)
	if err != nil {
		e.markError(m, "sqlx.Connect", err)
		return err
	}
	defer dbConn.Close()

	bt, err := e.t.GetBQInfo(e.m)
	if err != nil {
		e.markError(m, "GetBQInfo", err)
		return err
	}

	w, err := dbsync.NewTableWriter(dbConn, idb.Driver, bt, e.t.ExportTableName)
	if err != nil {
		e.markError(m, "dbsync.NewTableWriter", err)
		return err
	}

	err = w.Begin()
	if err != nil {
		e.markError(m, "Begin", err)
		return err
	}

	var urls []string
	json.Unmarshal([]byte(*j.DestinationUrls), &urls)
	for nf, url := range urls {

		_, file, _ := gcs.ParseGcsUri(url)

		// Get a reader
		r, err := m.gcs.GetReader(file)
		if err != nil {
			e.markError(m, "GetReader", err)
			return err
		}

		// Wrap it in a gzip reader
		g, err := gzip.NewReader(r)
		if err != nil {
			e.markError(m, "gzip.NewReader", err)
			return err
		}

		// Wrap it in a CSV reader
		c, n := csv.NewReader(g), 0
		for {
			n++
			row, err := c.Read()

			if err == io.EOF {
				break
			}
			if err != nil {
				e.markError(m, fmt.Sprintf("csv.Read(), file %d line %d", nf, n), err)
				return err
			}
			if n == 1 { // skip header (BQ has headers in every file)
				continue
			}

			if err := w.WriteRow(row); err != nil {
				e.markError(m, fmt.Sprintf("WriteRow(), file %d line %d", nf, n), err)
				return err
			}
		}
	}

	err = w.Commit()
	if err != nil {
		e.markError(m, "Commit", err)
		return err
	}

	if err := t.setRunning(m, false); err != nil {
		log.Printf("export to db error: %v", err)
	}
	log.Printf("Export to DB for table id %d (%s.%s) finished.", t.Id, t.Dataset, t.Name)
	return nil
}
