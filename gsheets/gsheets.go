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

// Package gsheets contains the functionality necessary to interface
// with Google Sheets.
package gsheets

import (
	"context"
	"fmt"
	"log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"
	sheets "google.golang.org/api/sheets/v4"
)

type Config struct {
	Email   string // Email (for authentication)
	Key     string // Key (for authentication)
	Domain  string // Sheets are made readable to this domain
	NSheets int    // Number of sheets (tabs) to keep in a spreadsheet
}

type GSheets struct {
	email   string
	key     []byte
	domain  string
	nSheets int
	sSrv    *sheets.Service
	dSrv    *drive.Service
}

// Create a GSheets instance given a Config.
func NewGSheets(cfg *Config) *GSheets {
	return &GSheets{
		email:   cfg.Email,
		key:     []byte(cfg.Key),
		domain:  cfg.Domain,
		nSheets: cfg.NSheets,
	}
}

func (g *GSheets) newDriveService() (*drive.Service, error) {
	cfg := &jwt.Config{
		Email:      g.email,
		PrivateKey: g.key,
		Scopes:     append([]string{}, drive.DriveScope), // copy
		TokenURL:   google.JWTTokenURL,
	}
	ctx := context.Background()
	return drive.New(oauth2.NewClient(ctx, cfg.TokenSource(ctx)))
}

func (g *GSheets) newSheetsService() (*sheets.Service, error) {
	cfg := &jwt.Config{
		Email:      g.email,
		PrivateKey: g.key,
		Scopes:     append([]string{}, sheets.SpreadsheetsScope), // copy
		TokenURL:   google.JWTTokenURL,
	}
	ctx := context.Background()
	return sheets.New(oauth2.NewClient(ctx, cfg.TokenSource(ctx)))
}

// Create a spreadsheet given a title. Returns a sheets.Spreadsheet pointer or error.
func (g *GSheets) CreateSpreadsheet(title string) (*sheets.Spreadsheet, error) {
	ssrv, err := g.newSheetsService()
	if err != nil {
		return nil, err
	}
	return ssrv.Spreadsheets.Create(&sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title: title,
		},
	}).Do()
}

// Get a spreadsheet given its id string.
func (g *GSheets) GetSpreadsheet(id string) (*sheets.Spreadsheet, error) {
	ssrv, err := g.newSheetsService()
	if err != nil {
		return nil, err
	}
	return ssrv.Spreadsheets.Get(id).Do()
}

// Prepend a sheet (tab) to spreadsheet.
func (g *GSheets) PrependSheet(id, title string) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	// This is not as easy as it seems!

	ssrv, err := g.newSheetsService()
	if err != nil {
		return nil, err
	}
	// Append the sheet to spreadsheet
	resp, err := ssrv.Spreadsheets.BatchUpdate(id,
		&sheets.BatchUpdateSpreadsheetRequest{
			Requests: []*sheets.Request{
				&sheets.Request{
					AddSheet: &sheets.AddSheetRequest{
						Properties: &sheets.SheetProperties{
							Title: title,
						},
					},
				},
			},
		}).Do()

	if err != nil {
		return resp, err
	}
	if resp == nil || len(resp.Replies) == 0 || resp.Replies[0].AddSheet == nil {
		return nil, fmt.Errorf("PrependSheet: received empty response?")
	}

	// Move the sheet to the leftmost position
	sheetId := resp.Replies[0].AddSheet.Properties.SheetId
	return ssrv.Spreadsheets.BatchUpdate(id,
		&sheets.BatchUpdateSpreadsheetRequest{
			Requests: []*sheets.Request{
				&sheets.Request{
					UpdateSheetProperties: &sheets.UpdateSheetPropertiesRequest{
						Fields: "index",
						Properties: &sheets.SheetProperties{
							SheetId: sheetId,
							Index:   0,
						},
					},
				},
			},
		}).Do()
}

// Remove Sheet1 and trailing sheets so that the number of sheets is
// at most nSheets.
func (g *GSheets) CleanupSheets(id string) error {
	sh, err := g.GetSpreadsheet(id)
	if err != nil {
		return err
	}

	if len(sh.Sheets) == 1 {
		return nil // nothing to do, Sheet1 cannot be removed
	}

	for _, sheet := range sh.Sheets {
		if sheet.Properties.SheetId == 0 { // Sheet1 (default)
			g.DeleteSheet(id, 0)
		}
		if sheet.Properties.Index >= int64(g.nSheets) {
			g.DeleteSheet(id, sheet.Properties.SheetId)
		}
	}
	return nil
}

// Delete a sheet from a spreadsheet.
func (g *GSheets) DeleteSheet(id string, sheetId int64) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	ssrv, err := g.newSheetsService()
	if err != nil {
		return nil, err
	}
	return ssrv.Spreadsheets.BatchUpdate(id,
		&sheets.BatchUpdateSpreadsheetRequest{
			Requests: []*sheets.Request{
				&sheets.Request{
					DeleteSheet: &sheets.DeleteSheetRequest{
						SheetId: sheetId,
					},
				},
			},
		}).Do()
}

// Make the spreadsheet (or any file really) readable by people in the
// organization (domain) who have a link.
func (g *GSheets) MakeFileDomainReadable(id string) (*drive.Permission, error) {
	if g.domain == "" {
		return nil, fmt.Errorf("Domain cannot be blank.")
	}
	dsrv, err := g.newDriveService()
	if err != nil {
		return nil, err
	}
	return dsrv.Permissions.Create(id,
		&drive.Permission{
			Role:   "reader",
			Type:   "domain",
			Domain: g.domain,
		}).Do()
}

// Create a goroutine which will do the work of appending rows to a
// sheet. Returns a channel which will receice a row at a time as a
// []string, or error. Closing the channel signals the worker to exit.
func (g *GSheets) AppendWorker(id, sheetTitle string) (chan<- []string, error) {
	ch := make(chan []string)

	go func(id string, ch <-chan []string) {

		ssrv, err := g.newSheetsService()
		if err != nil {
			log.Printf("AppendWorker() error: %v", err)
			return
		}

		vals := &sheets.ValueRange{
			MajorDimension: "ROWS",
			Values:         [][]interface{}{},
		}

		const BATCH = 1024

		for row := range ch {
			var irow []interface{}
			for _, cell := range row {
				irow = append(irow, cell)
			}
			vals.Values = append(vals.Values, irow)
			if len(vals.Values) == BATCH {
				_, err := ssrv.Spreadsheets.Values.Append(id, fmt.Sprintf("'%s'!1:1", sheetTitle), vals).ValueInputOption("RAW").Do()
				if err != nil {
					log.Printf("Append to sheet error: %v", err)
					return
				}
				vals = &sheets.ValueRange{
					MajorDimension: "ROWS",
					Values:         [][]interface{}{},
				}
			}
		}
		// Trailing send
		if len(vals.Values) > 0 {
			_, err = ssrv.Spreadsheets.Values.Append(id, fmt.Sprintf("'%s'!1:1", sheetTitle), vals).ValueInputOption("RAW").Do()
			if err != nil {
				log.Printf("Append to sheet error: %v", err)
				return
			}
		}
	}(id, ch)

	return ch, nil
}
