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
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"
	bigquery "google.golang.org/api/bigquery/v2"
)

type myTableReader struct {
	driver string
	rows   *sql.Rows
	buf    *bytes.Buffer
	gz     *gzip.Writer
	table  string
	lastId string
	stmt   *PrimitiveSelect
	batch  int
	cvals  []interface{}

	bytes, count int64
	cntReport    func(int64, int64)
	every        int
	toobig       int
}

func (tr *myTableReader) Bytes() int64      { return tr.bytes }
func (tr *myTableReader) Rows() int64       { return tr.count }
func (tr *myTableReader) LastId() string    { return tr.lastId }
func (tr *myTableReader) Statement() string { return tr.stmt.String() }

func (tr *myTableReader) CountReport(fn func(int64, int64), every int) {
	tr.cntReport = fn
	tr.every = every
}

func newMysqlTableReader(dbConn *sqlx.DB, table string, stmt *PrimitiveSelect, idColumn, lastId string, compress bool) (*myTableReader, error) {
	var (
		rows *sql.Rows
		err  error
	)

	stmt = wrapMyStmt(stmt, idColumn, lastId)
	if idColumn != "" && lastId != "" {
		rows, err = dbConn.Query(stmt.String(), lastId)
	} else {
		rows, err = dbConn.Query(stmt.String())
	}
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	tr := &myTableReader{
		driver: "mysql",
		rows:   rows,
		buf:    &buf,
		table:  table,
		stmt:   stmt,
		lastId: lastId,
		batch:  64, // process this many rows at a time
	}
	if compress {
		tr.gz, _ = gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	}

	return tr, nil
}

func wrapMyStmt(stmt *PrimitiveSelect, idColumn, lastId string) *PrimitiveSelect {

	// Change * to t.* with an alias, since "blah, *" is not valid MySQL
	stmt.Select = strings.Replace(stmt.Select, "*", "t.*", -1)
	stmt.From = fmt.Sprintf("%s t", stmt.From)

	if idColumn != "" {
		if lastId != "" {
			if stmt.Where != "" {
				stmt.Where += " AND "
			}
			stmt.Where += fmt.Sprintf(" %s > ?", idColumn)
		}
		stmt.Select = fmt.Sprintf("%s AS _id, %s", idColumn, stmt.Select)
		stmt.OrderBy = idColumn
	} else {
		stmt.Select = fmt.Sprintf("-1 AS _id, %s", stmt.Select)
	}
	return stmt
}

func (tr *myTableReader) Read(p []byte) (n int, err error) { // io.Reader

	if n, err := tr.buf.Read(p); n > 0 {
		return n, err
	}

	for i := 0; i < tr.batch; i++ {

		// do we have rows?
		if !tr.rows.Next() {
			// end of stream
			if tr.gz != nil {
				tr.gz.Close()
			}
			n, err = tr.buf.Read(p)
			if tr.every != 0 && err == io.EOF {
				tr.cntReport(tr.count, tr.bytes) // final report
			}
			return n, err
		}

		tr.count++
		if tr.every != 0 && tr.count%int64(tr.every) == 0 {
			tr.cntReport(tr.count, tr.bytes)
		}

		if len(tr.cvals) == 0 {
			// Populate an array of [lastid, ... nullstring]
			tr.cvals = []interface{}{&tr.lastId}
			cols, _ := tr.rows.Columns()
			for i := 1; i < len(cols); i++ {
				var cval sql.NullString
				tr.cvals = append(tr.cvals, &cval)
			}
		}

		if err := tr.rows.Scan(tr.cvals...); err != nil {
			return n, err
		}

		var (
			csvBuf    bytes.Buffer
			csvRecord []string
		)

		w := csv.NewWriter(&csvBuf)
		for _, cval := range tr.cvals[1:] {
			field := *cval.(*sql.NullString)
			if field.Valid && len(field.String) < maxCsvSize {
				csvRecord = append(csvRecord, filterInvalidUtf(field.String))
			} else {
				csvRecord = append(csvRecord, "")
			}
		}

		if err := w.Write(csvRecord); err != nil {
			return n, err
		}
		w.Flush()

		val := csvBuf.Bytes()

		if len(val) >= maxCsvSize {
			tr.toobig++
			val = []byte("")
		}

		tr.bytes += int64(len(val))

		if tr.gz != nil {
			tr.gz.Write(val) // compress the stuff
		} else {
			tr.buf.Write(val) // no compression
		}
	}

	if tr.gz != nil {
		tr.gz.Flush()
	}

	// return the read
	return tr.buf.Read(p)
}

func myDbTypeToBQType(typ string) string {
	switch typ {
	case "INT", "BIGINT", "MEDIUMINT", "SMALLINT", "TINYINT", "YEAR":
		return "INT64"
	case "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		return "BYTES"
	case "BIT":
		return "BOOL"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "TIMESTAMP", "DATETIME":
		return "TIMESTAMP"
	case "FLOAT", "DECIMAL": // NB: BQ will evenutally support DECIMAL
		return "FLOAT64"
	}
	return "STRING" // Should work for most anything else
}

func (tr *myTableReader) GetBQSchema(dbConn *sqlx.DB, stmt PrimitiveSelect) (*bigquery.TableSchema, error) {

	stmt.Where = "" // This may contain params
	stmt.Limit = "0"
	stmt.OrderBy = ""

	rows, err := dbConn.Query(stmt.String())
	if err != nil {
		return nil, err
	}

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]*bigquery.TableFieldSchema, len(ctypes))
	for i, ctype := range ctypes[1:] { // First column is id
		fields[i] = &bigquery.TableFieldSchema{
			Name: ctype.Name(),
			Type: myDbTypeToBQType(ctype.DatabaseTypeName()),
		}
	}

	return &bigquery.TableSchema{
		Fields: fields,
	}, nil
}

// MySQL data can contain ASCII 0 which BQ does not like
func filterInvalidUtf(s string) string {
	ok := true
	for _, r := range s {
		if r == utf8.RuneError || r == '\000' {
			ok = false
			break
		}
	}
	if ok { // nothing to do
		return s
	}

	result := s
	valid := make([]rune, 0, len(s))
	for _, r := range s {
		if r != utf8.RuneError && r != '\000' {
			valid = append(valid, r)
		}
	}
	result = string(valid)
	return result
}
