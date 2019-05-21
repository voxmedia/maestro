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
	"fmt"
	"io"

	"github.com/jmoiron/sqlx"
	bigquery "google.golang.org/api/bigquery/v2"
)

type pgTableReader struct {
	driver       string
	rows         *sql.Rows
	buf          *bytes.Buffer
	gz           *gzip.Writer
	table        string
	lastId       string
	stmt         string
	batch        int
	bytes, count int64

	cntReport func(int64, int64)
	every     int
	toobig    int
}

func (tr *pgTableReader) Bytes() int64      { return tr.bytes }
func (tr *pgTableReader) Rows() int64       { return tr.count }
func (tr *pgTableReader) LastId() string    { return tr.lastId }
func (tr *pgTableReader) Statement() string { return tr.stmt }

func (tr *pgTableReader) CountReport(fn func(int64, int64), every int) {
	tr.cntReport = fn
	tr.every = every
}

func newPgTableReader(dbConn *sqlx.DB, table, stmt, idColumn, lastId string, compress bool) (*pgTableReader, error) {
	var (
		rows *sql.Rows
		err  error
	)

	stmt = wrapPgStmt(stmt, idColumn, lastId)
	if idColumn != "" && lastId != "" {
		rows, err = dbConn.Query(stmt, lastId)
	} else {
		rows, err = dbConn.Query(stmt)
	}
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	tr := &pgTableReader{
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

// The ROW() reduces the number of columns to 1 which greately speeds up the
// transfer as well as automatically gives us the CSV format if we strip the
// parenthesis.
func wrapPgStmt(stmt, idColumn, lastId string) string {
	if idColumn != "" {
		var where string
		if lastId != "" {
			where = fmt.Sprintf("WHERE %s > $1", idColumn)
		}
		// NB: If the statement had a LIMIT, the results are going to be bogus, since
		// ORDER BY happens after the LIMIT. TODO: This can be solved by smarter
		// PrimitiveSelect use.
		stmt = fmt.Sprintf("SELECT %s, ROW(t.*) AS row FROM (%s) t %s ORDER BY %s", idColumn, stmt, where, idColumn)
	} else {
		stmt = fmt.Sprintf("SELECT -1, ROW(t.*) AS row FROM (%s) t", stmt)
	}
	return stmt
}

func (tr *pgTableReader) Read(p []byte) (n int, err error) { // io.Reader

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

		// get the row data
		var val []byte
		if err := tr.rows.Scan(&tr.lastId, &val); err != nil {
			return n, err
		}

		// because we are using ROW()
		val = val[1:]          // strip starting paren
		val[len(val)-1] = '\n' // replace trailing with EOL

		if len(val) >= maxCsvSize {
			tr.toobig++
			val = []byte("")
		}

		tr.bytes += int64(len(val))

		if tr.gz != nil {
			// compress the stuff
			tr.gz.Write(val)
		} else {
			tr.buf.Write(val)
		}
	}

	if tr.gz != nil {
		tr.gz.Flush()
	}

	// return the read
	return tr.buf.Read(p)
}

func pgDbTypeToBQType(typ string) string {
	switch typ {
	case "INT8", "INT4", "INT2":
		return "INT64"
	case "BYTEA":
		return "BYTES"
	case "BOOL":
		return "BOOL"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ":
		return "TIMESTAMP"
	case "FLOAT4", "FLOAT8":
		return "FLOAT64"
	}
	return "STRING" // Should work for most anything else
}

func (tr *pgTableReader) GetBQSchema(dbConn *sqlx.DB, stmt PrimitiveSelect) (*bigquery.TableSchema, error) {

	limit := fmt.Sprintf("SELECT * FROM (%s) t LIMIT 0", stmt.String())
	rows, err := dbConn.Query(limit)
	if err != nil {
		return nil, err
	}

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]*bigquery.TableFieldSchema, len(ctypes))
	for i, ctype := range ctypes {
		fields[i] = &bigquery.TableFieldSchema{
			Name: ctype.Name(),
			Type: pgDbTypeToBQType(ctype.DatabaseTypeName()),
		}
	}

	return &bigquery.TableSchema{
		Fields: fields,
	}, nil
}
