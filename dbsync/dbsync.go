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

// Package dbsync contains the code for transferring data in and out
// of external PostgreSQL and MySQL databases, primarily for Maestro
// import tables.
package dbsync

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	bigquery "google.golang.org/api/bigquery/v2"

	// DB drivers
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	compress   = false
	maxCsvSize = 1024 * 1024 * 10
)

// This interface should be satisfied by all the drivers. It is a
// Reader, as well as provides a bunch of transfer stats and the
// schema converted from driver-specific dialect to BigQuery.
type TableReader interface {
	Read(p []byte) (n int, err error)

	Bytes() int64
	Rows() int64
	LastId() string
	Statement() string
	CountReport(fn func(int64, int64), every int)
	GetBQSchema(dbConn *sqlx.DB, stmt PrimitiveSelect) (*bigquery.TableSchema, error)
}

// Create a new reader. The driver should be "potgres" or "mysql". The
// idColumn is the column used for incremental imports and should be
// indexed. lastId is the greatest id of the previous import. If
// compress is true, the data will be gzip compressed.
func NewTableReader(dbConn *sqlx.DB, driver, table string, stmt *PrimitiveSelect, idColumn, lastId string, compress bool) (TableReader, error) {
	switch driver {
	case "postgres":
		return newPgTableReader(dbConn, table, stmt.String(), idColumn, lastId, compress)
	case "mysql":
		return newMysqlTableReader(dbConn, table, stmt, idColumn, lastId, compress)
	}
	return nil, fmt.Errorf("Unsupported driver: %s", driver)
}

// Create a new writer. Driver should be "postgres" or "mysql"
// (NIY). The table argument allows giving the table an alternative
// name in the target external database.
func NewTableWriter(dbConn *sqlx.DB, driver string, bqTable *bigquery.Table, table string) (TableWriter, error) {
	switch driver {
	case "postgres":
		return newPgTableWriter(dbConn, bqTable, table)
	case "mysql":
		return nil, fmt.Errorf("MySQL not implemented yet") // TODO
	}
	return nil, fmt.Errorf("Unsupported driver: %s", driver)
}

// This interface should be satisfied by all writer drivers.
type TableWriter interface {
	Begin() error
	WriteRow([]string) error
	Commit() error
}

// A primitive select is an SQL statement which only supports SELECT,
// FROM, WHERE, LIMIT and ORDER BY.
type PrimitiveSelect struct {
	Select  string
	From    string
	Where   string
	Limit   string
	OrderBy string
}

// Convet a PrimitiveSelect to an actual SQL statement string.
func (s *PrimitiveSelect) String() string {
	cols := s.Select
	if cols == "" {
		cols = "*"
	}
	stmt := fmt.Sprintf("SELECT %s \n  FROM %s\n", cols, s.From)
	if s.Where != "" {
		stmt = fmt.Sprintf("%s WHERE %s\n", stmt, s.Where)
	}
	if s.OrderBy != "" {
		stmt = fmt.Sprintf("%s ORDER BY %s\n", stmt, s.OrderBy)
	}
	if s.Limit != "" {
		stmt = fmt.Sprintf("%s LIMIT %s", stmt, s.Limit)
	}
	return stmt
}
