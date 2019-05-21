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
	"fmt"
	"log"
	"time"
)

type extractToSheetsCall struct {
	m       *Model
	t       *Table
	headers []string
}

func (e *extractToSheetsCall) markError(m *Model, note string, err error) error {
	log.Printf("extractToSheetsCall error: %v", err)
	m.SlackAlert(fmt.Sprintf("Extract to sheets error in %s: [%s] %v",
		"<{URL_PREFIX}"+fmt.Sprintf("/#/table/%d|%s>", e.t.Id, e.t.Name), note, err))
	e.t.setError(m, err)
	return err
}

// This is where the extract to sheets happens. We do not have a wait
// lock because if Maestro quits, the worst that happens is we have an
// incoplete sheets export. (TODO: make this better).
func (e *extractToSheetsCall) Do() error {
	m, t := e.m, e.t

	// If no spreadsheet exists for this table, create it
	if t.SheetId == "" {
		title := fmt.Sprintf("%s.%s", t.Dataset, t.Name)
		sh, err := m.gsh.CreateSpreadsheet(title)
		if err != nil {
			return e.markError(m, "CreateSpreadsheet", err)
		}
		log.Printf("Created Spreadsheet id %s.", sh.SpreadsheetId)

		if _, err = m.gsh.MakeFileDomainReadable(sh.SpreadsheetId); err != nil {
			return e.markError(m, "MakeFileDomainReadable", err)
		}

		t.SheetId = sh.SpreadsheetId
		if err = m.SaveTable(t); err != nil {
			return e.markError(m, "t.SheetId", err)
		}
	}

	// Get the sheet (even if we just created it)
	sh, err := m.gsh.GetSpreadsheet(t.SheetId)
	if err != nil {
		return e.markError(m, "m.gsh.GetSpreadsheet", err)
	}

	// Prepend our new sheet
	sheetTitle := time.Now().Format("2006-01-02 150405") // NB: colons cause problems
	_, err = m.gsh.PrependSheet(sh.SpreadsheetId, sheetTitle)
	if err != nil {
		return e.markError(m, "g.AppendSheet", err)
	}

	// Remove trailing sheets, if any
	if err := m.gsh.CleanupSheets(t.SheetId); err != nil {
		return e.markError(m, "m.gsh.CleanupSheets", err)
	}

	ch, err := m.gsh.AppendWorker(sh.SpreadsheetId, sheetTitle)
	if err != nil {
		return e.markError(m, "m.gsh.AppendWorker", err)
	}

	// First headers
	ch <- e.headers

	print := func(row []string) error {
		ch <- row
		return nil
	}

	m.bq.TableData(t.Dataset, t.Name, print)

	// Signal end of data
	close(ch)

	if err := t.setRunning(m, false); err != nil {
		log.Printf("extractToSheet() error: %v", err)
	}
	log.Printf("Google Sheet extract for table id %d (%s.%s) finished.", t.Id, t.Dataset, t.Name)
	return nil
}
