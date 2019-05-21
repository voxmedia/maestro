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

package dbsync

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"google.golang.org/api/bigquery/v2"
)

type pgTableWriter struct {
	table   string
	dbConn  *sqlx.DB
	txn     *sql.Tx
	stmt    *sql.Stmt
	bqTable *bigquery.Table
	rows    int
}

func newPgTableWriter(dbConn *sqlx.DB, bqTable *bigquery.Table, table string) (*pgTableWriter, error) {
	return &pgTableWriter{
		dbConn:  dbConn,
		bqTable: bqTable,
		table:   table,
	}, nil
}

func (w *pgTableWriter) Begin() (err error) {

	w.txn, err = w.dbConn.Begin()
	if err != nil {
		return err
	}

	sql := w.getCreateTable(true)

	_, err = w.txn.Exec(sql)
	if err != nil {
		return err
	}

	cols := make([]string, 0, len(w.bqTable.Schema.Fields))
	for _, col := range w.bqTable.Schema.Fields {
		cols = append(cols, col.Name)
	}

	w.stmt, err = w.txn.Prepare(pq.CopyIn(w.table, cols...))
	if err != nil {
		return err
	}

	return nil
}

func (w *pgTableWriter) WriteRow(row []string) error {

	// Convert to interface, while setting nulls where it is allowed
	vals := make([]interface{}, len(row))
	for i, val := range row {
		field := w.bqTable.Schema.Fields[i]
		if val == "" && field.Type != "STRING" && field.Mode != "REQUIRED" {
			vals[i] = nil
		} else {
			vals[i] = val
		}
	}

	_, err := w.stmt.Exec(vals...)

	w.rows++
	if w.rows%1000000 == 0 {
		log.Printf("Export of %s: %d rows sent...", w.table, w.rows)
	}
	return err
}

func (w *pgTableWriter) Commit() error {
	if _, err := w.stmt.Exec(); err != nil {
		return err
	}
	if err := w.stmt.Close(); err != nil {
		return err
	}
	log.Printf("Export of %s complete, %d rows sent.", w.table, w.rows)
	return w.txn.Commit()
}

func (w *pgTableWriter) getCreateTable(drop bool) string {

	typeMap := map[string]string{
		"STRING":    "TEXT",
		"BYTES":     "BYTEA",
		"INTEGER":   "BIGINT",
		"INT64":     "BIGINT",
		"FLOAT":     "DOUBLE PRECISION",
		"FLOAT64":   "DOUBLE PRECISION",
		"BOOLEAN":   "BOOLEAN",
		"BOOL":      "BOOLEAN",
		"TIMESTAMP": "TIMESTAMP WITH TIME ZONE",
		"DATE":      "DATE",
		"TIME":      "TIME",
		"DATETIME":  "TIMESTAMP",
	}

	var result string
	if drop {
		result = fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", w.table)
	}

	result += fmt.Sprintf("CREATE TABLE %s ", w.table)
	result += "(\n"

	cols := make([]string, 0, len(w.bqTable.Schema.Fields))
	for _, col := range w.bqTable.Schema.Fields {
		column := fmt.Sprintf("    %s %s", col.Name, typeMap[col.Type])
		if col.Mode == "REQUIRED" {
			column += " NOT NULL"
		}
		cols = append(cols, column)
	}

	result += strings.Join(cols, ",\n")
	result += ");"

	return result
}
