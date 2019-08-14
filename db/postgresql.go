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

// Package db is where all database-specific code which maintains
// Maestro state and configuration resides.
package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"encoding/hex"
	"encoding/json"

	"relative/crypto"
	"relative/model"
	"relative/scheduler"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	tableColumns = "id, user_id, group_id, dataset_id, name, query, disposition, partitioned, legacy_sql, description, " +
		"error, running, extract, notify_extract_url, sheets_extract, sheet_id, import_db_id, imported_at, id_column, " +
		"last_id, freq_id, conditions, reimport_cond, created_at, deleted_at, last_ok_run_end_at, external_tmout, " +
		"export_db_id, export_table_name "
	bqJobColumns = "id, created_at, table_id, user_id, run_id, parents, bq_job_id, configuration, " +
		"type, status, query_stats, load_stats, extract_stats, creation_time, start_time, " +
		"end_time, total_bytes_processed, total_bytes_billed, destination_urls, " +
		"import_begin, import_end, import_bytes, import_rows "
)

type Config struct {
	ConnectString string
	Prefix        string
	Secret        string
}

// Given a database config, return an instance which satisfies the
// model.Db interface for all database operations.
func InitDb(cfg Config) (*pgDb, error) {
	if dbConn, err := sqlx.Connect("postgres", cfg.ConnectString); err != nil {
		return nil, err
	} else {
		p := &pgDb{dbConn: dbConn, prefix: cfg.Prefix, secret: cfg.Secret}
		if err := p.dbConn.Ping(); err != nil {
			return nil, err
		}
		if err := p.createTablesIfNotExist(); err != nil {
			return nil, err
		}
		if err := p.prepareSqlStatements(); err != nil {
			return nil, err
		}
		return p, nil
	}
}

type pgDb struct {
	dbConn *sqlx.DB
	prefix string
	secret string

	//sqlSelectBQJob          *sqlx.Stmt
}

func (p *pgDb) Close() error {
	return p.dbConn.Close()
}

func (p *pgDb) createTablesIfNotExist() error {
	create_sql := `

    -- users

       CREATE TABLE IF NOT EXISTS %[1]susers (
         id SERIAL NOT NULL PRIMARY KEY,
         oauth_id TEXT NOT NULL,
         email TEXT NOT NULL,
         api_token TEXT NOT NULL DEFAULT '',
         admin BOOL NOT NULL DEFAULT false,
         disabled BOOL NOT NULL DEFAULT false,
         created_at TIMESTAMP WITH TIME ZONE DEFAULT now()::timestamp
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]susers_oauth_id_idx ON %[1]susers(oauth_id);

    -- groups

       CREATE TABLE IF NOT EXISTS %[1]sgroups (
         id SERIAL NOT NULL PRIMARY KEY,
         name TEXT NOT NULL,
         admin_user_id INT NOT NULL REFERENCES %[1]susers(id) ON DELETE RESTRICT,
         created_at TIMESTAMP WITH TIME ZONE DEFAULT now()::timestamp
       );

       CREATE TABLE IF NOT EXISTS %[1]suser_groups (
         user_id INT NOT NULL REFERENCES %[1]susers(id) ON DELETE RESTRICT,
         group_id INT NOT NULL REFERENCES %[1]sgroups(id) ON DELETE RESTRICT
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]suser_groups_user_id_group_id_idx ON %[1]suser_groups(user_id, group_id);

    -- freqs

       CREATE TABLE IF NOT EXISTS %[1]sfreqs (
         id SERIAL NOT NULL PRIMARY KEY,
         name TEXT NOT NULL,
         period BIGINT NOT NULL,             -- seconds
         "offset" BIGINT NOT NULL DEFAULT 0, -- seconds
         active BOOLEAN NOT NULL DEFAULT false
       );

    -- datasets

       CREATE TABLE IF NOT EXISTS %[1]sdatasets (
         id SERIAL NOT NULL PRIMARY KEY,
         dataset TEXT NOT NULL
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sdatasets_dataset_idx ON %[1]sdatasets (dataset);

    -- dbs

       CREATE TABLE IF NOT EXISTS %[1]sdbs (
         id SERIAL NOT NULL PRIMARY KEY,
         name TEXT NOT NULL,
         dataset_id INT NOT NULL REFERENCES %[1]sdatasets(id) ON DELETE RESTRICT,
         driver TEXT NOT NULL,
         connect_str TEXT NOT NULL,
         secret TEXT NOT NULL,
         export BOOL NOT NULL DEFAULT False
       );

    -- tables

       CREATE OR REPLACE FUNCTION %[1]svalid_array_range(arr jsonb, minv integer, maxv integer) RETURNS boolean AS $f$
       DECLARE
           elem integer;
       BEGIN
       FOR elem IN SELECT jsonb_array_elements(arr) LOOP
           IF elem < minv OR elem > maxv THEN
              RETURN false;
           END IF;
       END LOOP;
       RETURN true;
       END;
       $f$ LANGUAGE 'plpgsql' IMMUTABLE;

       CREATE OR REPLACE FUNCTION %[1]svalidate_conditions(conditions jsonb) RETURNS boolean AS $f$
       DECLARE
           cond jsonb;
       BEGIN
       IF jsonb_typeof(conditions) = 'array' THEN
           FOR cond IN SELECT jsonb_array_elements(conditions) LOOP
              IF NOT cond ? 'weekdays' AND NOT cond ? 'months' AND NOT cond ? 'days' AND NOT cond ? 'hours' THEN
                  RETURN false;
              END IF;
              IF cond ? 'weekdays' AND (jsonb_typeof((cond->>'weekdays')::jsonb) != 'array' OR valid_array_range((cond->>'weekdays')::jsonb, 0, 6) = false) THEN
                  RETURN false;
              END IF;
              IF cond ? 'months' AND (jsonb_typeof((cond->>'months')::jsonb) != 'array' OR valid_array_range((cond->>'months')::jsonb, 1, 12) = false) THEN
                  RETURN false;
              END IF;
              IF cond ? 'days' AND (jsonb_typeof((cond->>'days')::jsonb) != 'array' OR valid_array_range((cond->>'days')::jsonb, 1, 31) = false) THEN
                  RETURN false;
              END IF;
              IF cond ? 'hours' AND (jsonb_typeof((cond->>'hours')::jsonb) != 'array' OR valid_array_range((cond->>'hours')::jsonb, 0, 23) = false) THEN
                  RETURN false;
              END IF;
           END LOOP;
           RETURN true;
       END IF;
       RETURN false;
       END;
       $f$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;

       CREATE TABLE IF NOT EXISTS %[1]stables (
         id SERIAL NOT NULL PRIMARY KEY,
         user_id INT NOT NULL REFERENCES %[1]susers(id) ON DELETE RESTRICT,
         group_id INT NOT NULL REFERENCES %[1]sgroups(id) ON DELETE RESTRICT,
         name TEXT NOT NULL,
         dataset_id INT NOT NULL REFERENCES %[1]sdatasets(id) ON DELETE RESTRICT,
         description TEXT NOT NULL DEFAULT '',
         query TEXT NOT NULL,
         legacy_sql BOOL NOT NULL DEFAULT False,
         disposition TEXT NOT NULL DEFAULT 'WRITE_TRUNCATE',
         partitioned BOOL NOT NULL DEFAULT False,
         running BOOL NOT NULL DEFAULT False,
         error TEXT NOT NULL DEFAULT '',
         extract BOOL NOT NULL DEFAULT False,
         notify_extract_url TEXT NOT NULL DEFAULT '',
         sheets_extract BOOL NOT NULL DEFAULT False,
         sheet_id TEXT NOT NULL DEFAULT '',
         export_db_id INT REFERENCES %[1]sdbs(id) ON DELETE RESTRICT,
         export_table_name TEXT NOT NULL DEFAULT '',
         freq_id INT REFERENCES %[1]sfreqs(id) ON DELETE RESTRICT,
         conditions JSONB NOT NULL DEFAULT '[]',
         external_tmout BIGINT,
         external_format TEXT NOT NULL DEFAULT '',
         import_db_id INT REFERENCES %[1]sdbs(id) ON DELETE RESTRICT,
         imported_at TIMESTAMP WITH TIME ZONE,
         id_column TEXT NOT NULL DEFAULT '',
         last_id TEXT NOT NULL DEFAULT '',
         reimport_cond JSONB NOT NULL DEFAULT '[]',
         created_at TIMESTAMP WITH TIME ZONE DEFAULT now()::timestamp,
         deleted_at TIMESTAMP WITH TIME ZONE,
         last_ok_run_end_at TIMESTAMP WITH TIME ZONE,
         CONSTRAINT %[1]stables_valid_conditions CHECK (validate_conditions(conditions)),
         CONSTRAINT %[1]stables_valid_reimport_cond CHECK (validate_conditions(reimport_cond))
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]stables_name_dataset_id_idx
           ON %[1]stables (name, dataset_id)
         WHERE deleted_at IS NULL;
       CREATE INDEX IF NOT EXISTS %[1]stables_user_id_idx ON %[1]stables (user_id);

    -- runs

       CREATE TABLE IF NOT EXISTS %[1]sruns (
         id SERIAL NOT NULL PRIMARY KEY,
         user_id INT REFERENCES %[1]susers(id) ON DELETE RESTRICT,
         created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
         start_time TIMESTAMP WITH TIME ZONE,
         end_time TIMESTAMP WITH TIME ZONE,
         error TEXT,
         freq_id INT REFERENCES %[1]sfreqs(id) ON DELETE RESTRICT
       );

       -- Allow only one unfinished run (end_time is null)
       -- https://www.toadworld.com/platforms/postgres/b/weblog/archive/2017/07/12/allowing-only-one-null
       -- NOTE: Index MUST named "freq_id_end_time_null_idx", see InsertRun()
       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sruns_freq_id_end_time_null_idx
         ON %[1]sruns (freq_id, (end_time IS NULL)) WHERE end_time IS NULL;

    -- bq_jobs

       CREATE TABLE IF NOT EXISTS %[1]sbq_jobs (
         id SERIAL NOT NULL PRIMARY KEY,
         created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
         table_id INT NOT NULL REFERENCES %[1]stables(id) ON DELETE CASCADE,
         user_id INT REFERENCES %[1]susers(id) ON DELETE RESTRICT,
         run_id INT REFERENCES %[1]sruns(id) ON DELETE CASCADE,
         parents JSONB,
         type TEXT NOT NULL,
         bq_job_id TEXT NOT NULL,
         configuration JSONB,
         status JSONB,
         query_stats JSONB,
         load_stats JSONB,
         extract_stats JSONB,
         creation_time TIMESTAMP WITH TIME ZONE,
         start_time TIMESTAMP WITH TIME ZONE,
         end_time TIMESTAMP WITH TIME ZONE,
         total_bytes_processed BIGINT NOT NULL DEFAULT 0,
         total_bytes_billed BIGINT NOT NULL DEFAULT 0,
         destination_urls JSONB,  -- extract only
         import_begin TIMESTAMP WITH TIME ZONE,
         import_end TIMESTAMP WITH TIME ZONE,
         import_bytes BIGINT NOT NULL DEFAULT 0,
         import_rows BIGINT NOT NULL DEFAULT 0
       );

       CREATE INDEX IF NOT EXISTS %[1]sbq_jobs_table_id_idx ON %[1]sbq_jobs(table_id);
       CREATE INDEX IF NOT EXISTS %[1]sbq_jobs_user_id_idx ON %[1]sbq_jobs (user_id);
       CREATE INDEX IF NOT EXISTS %[1]sbq_jobs_run_id_idx ON %[1]sbq_jobs (run_id);
       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sbq_jobs_nb_bq_job_id_idx ON %[1]sbq_jobs (bq_job_id) WHERE bq_job_id <> '';

    -- notifications

       CREATE TABLE IF NOT EXISTS %[1]snotifications (
         id SERIAL NOT NULL PRIMARY KEY,
         table_id INT NOT NULL REFERENCES %[1]stables(id) ON DELETE CASCADE,
         bq_job_id INT NOT NULL REFERENCES %[1]sbq_jobs(id) ON DELETE CASCADE,
         created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
         duration_ms BIGINT NOT NULL,
         error TEXT,
         url TEXT NOT NULL,
         method TEXT NOT NULL,
         body TEXT NOT NULL,
         resp_status_code INT NOT NULL,
         resp_status TEXT NOT NULL,
         resp_headers TEXT NOT NULL,
         resp_body TEXT NOT NULL
       );

    -- bq_conf

       CREATE TABLE IF NOT EXISTS %[1]sbq_conf (
         project_id TEXT NOT NULL,
         email TEXT NOT NULL,
         private_key_id TEXT NOT NULL,
         key TEXT NOT NULL,
         gcs_bucket TEXT NOT NULL DEFAULT '',
           -- This table can only have one row
         one_row BOOL NOT NULL DEFAULT true,
         CONSTRAINT one_row_true CHECK(one_row = true)
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sbq_conf_one_row_idx ON %[1]sbq_conf(one_row);

    -- oauth_conf

       CREATE TABLE IF NOT EXISTS %[1]soauth_conf (
         client_id TEXT NOT NULL,
         secret TEXT NOT NULL,
         redirect TEXT NOT NULL,
         allowed_domain TEXT NOT NULL,
         cookie_secret TEXT NOT NULL,
           -- This table can only have one row
         one_row BOOL NOT NULL DEFAULT true,
         CONSTRAINT one_row_true CHECK(one_row = true)
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]soauth_conf_one_row_idx ON %[1]soauth_conf(one_row);

    -- git_conf

       CREATE TABLE IF NOT EXISTS %[1]sgit_conf (
         url TEXT NOT NULL,
         token TEXT NOT NULL,
           -- This table can only have one row
         one_row BOOL NOT NULL DEFAULT true,
         CONSTRAINT one_row_true CHECK(one_row = true)
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sgit_conf_one_row_idx ON %[1]sgit_conf(one_row);

    -- slack_conf

       CREATE TABLE IF NOT EXISTS %[1]sslack_conf (
         url TEXT NOT NULL,
         username TEXT NOT NULL,
         channel TEXT NOT NULL,
         iconemoji TEXT NOT NULL DEFAULT '',
         url_prefix TEXT NOT NULL,
           -- This table can only have one row
         one_row BOOL NOT NULL DEFAULT true,
         CONSTRAINT one_row_true CHECK(one_row = true)
       );

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sslack_conf_one_row_idx ON %[1]sslack_conf(one_row);
    `
	if rows, err := p.dbConn.Query(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		log.Printf("ERROR: initial CREATE TABLE failed: %v", err)
		return err
	} else {
		rows.Close()
	}
	return nil
}

func (p *pgDb) prepareSqlStatements() error {
	// No longer used, but leaving it here as an example
	// var err error
	// if p.sqlSelectBQJob, err = p.dbConn.Preparex(fmt.Sprintf(
	// 	"SELECT id, table_id, user_id, bq_job_id, configuration, status, query_stats, load_stats, extract_stats, "+
	// 		"creation_time, start_time, end_time, total_bytes_processed, total_bytes_billed "+
	// 		"FROM  %[1]sbq_jobs AS jobs WHERE id = $1", p.prefix)); err != nil {
	// 	return err
	// }
	return nil
}

func (p *pgDb) SelectOrInsertUserByOAuthId(oAuthId, email string) (*model.User, bool, error) {

	stmt := fmt.Sprintf(
		"SELECT id, oauth_id, email, api_token, admin, disabled FROM  %[1]susers AS users WHERE oauth_id = $1",
		p.prefix)

	rows, err := p.dbConn.Queryx(stmt, oAuthId)
	if err != nil {
		log.Printf("SelectOrInsertUserByOAuthId(): error: %v", err)
		return nil, false, err
	}

	created := false
	if !rows.Next() {
		if email == "" {
			return nil, false, fmt.Errorf("SelectOrInsertUserByOAuthId: email cannot be blank for insert")
		}
		stmt := fmt.Sprintf(
			"INSERT INTO %[1]susers AS users (oauth_id, email, disabled) VALUES ($1, $2, true) "+
				"ON CONFLICT (oauth_id) DO UPDATE SET oauth_id = $1 "+
				"RETURNING id, oauth_id, email, api_token, admin, disabled", p.prefix)
		rows, err = p.dbConn.Queryx(stmt, oAuthId, email)
		if err != nil {
			log.Printf("SelectOrInsertUserByOAuthId(): error: %v", err)
			return nil, false, err
		}
		if !rows.Next() {
			return nil, false, fmt.Errorf("SelectOrInsertUserByOAuthId(): unable to insert?")
		}
		created = true // this can be wrong, but it's okay
	}
	defer rows.Close()

	user, err := p.userFromRow(rows)
	return user, created, err
}

func (p *pgDb) SelectUser(id int64) (*model.User, error) {

	stmt := fmt.Sprintf(
		"SELECT id, oauth_id, email, api_token, admin, disabled FROM  %[1]susers AS users WHERE id = $1",
		p.prefix)

	rows, err := p.dbConn.Queryx(stmt, id)
	if err != nil {
		log.Printf("SelectUser(): error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var user *model.User
	if rows.Next() {
		if user, err = p.userFromRow(rows); err != nil {
			return nil, err
		}
	}

	// Get groups
	if user.Admin {
		// Admins see all groups
		stmt = fmt.Sprintf(
			`SELECT g.id, g.name, g.admin_user_id
           FROM %[1]sgroups g
           WHERE $1 = $1 -- must use the param somehow
        `, p.prefix)

	} else {
		stmt = fmt.Sprintf(
			`SELECT g.id, g.name, g.admin_user_id
           FROM %[1]suser_groups ug
           JOIN %[1]sgroups g ON g.id = ug.group_id
          WHERE ug.user_id = $1
        `, p.prefix)
	}
	user.Groups = make([]*model.Group, 0, 4) // Avoid json null
	if err := p.dbConn.Select(&user.Groups, stmt, id); err != nil {
		if err != sql.ErrNoRows {
			log.Printf("SelectUser(): error: %v", err)
			return nil, err
		}
	}

	return user, nil
}

func (p *pgDb) SaveUser(u *model.User) error {
	if len(u.PlainToken) > 0 {
		cryptToken, err := crypto.EncryptString(fmt.Sprintf("%x", u.PlainToken), p.secret)
		if err != nil {
			return err
		}
		u.CryptToken = cryptToken
	}
	stmt := fmt.Sprintf(
		"UPDATE %[1]susers "+
			"SET oauth_id = :oauth_id, email = :email, admin = :admin, disabled = :disabled, api_token = :api_token "+
			"WHERE id = :id", p.prefix)
	if _, err := p.dbConn.NamedExec(stmt, u); err != nil {
		return err
	}

	// Update user groups
	txn, err := p.dbConn.Begin()
	if err != nil {
		return err
	}
	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM %[1]suser_groups WHERE user_id = $1", p.prefix), u.Id); err != nil {
		txn.Rollback()
		return err
	}
	for _, g := range u.Groups {
		if _, err := txn.Exec(fmt.Sprintf("INSERT INTO %[1]suser_groups (user_id, group_id) "+
			"VALUES($1, $2)", p.prefix), u.Id, g.Id); err != nil {
			txn.Rollback()
			return err
		}
	}
	return txn.Commit()
}

func (p *pgDb) userFromRow(rows *sqlx.Rows) (*model.User, error) {
	var user = model.User{
		Groups: make(model.GroupArray, 0), // avoid JSON null
	}
	if err := rows.StructScan(&user); err != nil {
		log.Printf("userFromRow(): error scanning row: %v", err)
		return nil, err
	}
	if user.CryptToken != "" {
		hexTok, err := crypto.DecryptString(user.CryptToken, p.secret)
		if err != nil {
			log.Printf("userFromRow(): error decrypting: %v", err)
			return nil, err
		}
		tok, err := hex.DecodeString(hexTok)
		if err != nil {
			log.Printf("userFromRow(): error decoding: %v", err)
			return nil, err
		}
		user.PlainToken = tok
	}
	return &user, nil
}

func (p *pgDb) SelectUsers() ([]*model.User, error) {

	// FILTER is required to avoid arrays with nulls
	stmt := fmt.Sprintf(
		"SELECT u.id, oauth_id, email, api_token, admin, disabled, "+
			" TO_JSON(ARRAY_AGG(g.*) FiLTER (WHERE g.id IS NOT NULL)) AS groups "+
			"FROM %[1]susers u "+
			"LEFT JOIN %[1]suser_groups ug ON ug.user_id = u.id "+
			"LEFT JOIN %[1]sgroups g ON ug.group_id = g.id "+
			"GROUP BY 1, 2, 3, 4, 5, 6 "+
			"ORDER BY email",
		p.prefix)

	rows, err := p.dbConn.Queryx(stmt)
	if err != nil {
		log.Printf("SelectUsers(): error: %v", err)
		return nil, err
	}
	defer rows.Close()

	users := make([]*model.User, 0, 16)
	for rows.Next() {
		user, err := p.userFromRow(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

func (p *pgDb) InsertGroup(name string, adminId int64) error {
	stmt := fmt.Sprintf("INSERT INTO %[1]sgroups (name, admin_user_id) VALUES ($1, $2)", p.prefix)
	_, err := p.dbConn.Exec(stmt, name, adminId)
	return err
}

func (p *pgDb) SaveGroup(g *model.Group) error {
	stmt := fmt.Sprintf("UPDATE %[1]sgroups SET name = $1, admin_user_id = $2 WHERE id = $3", p.prefix)
	_, err := p.dbConn.Exec(stmt, g.Name, g.AdminUserId, g.Id)
	return err
}

func (p *pgDb) SelectGroups() ([]*model.Group, error) {

	stmt := fmt.Sprintf(
		"SELECT id, name, admin_user_id FROM %[1]sgroups ORDER BY id",
		p.prefix)

	rows, err := p.dbConn.Query(stmt)
	if err != nil {
		log.Printf("SelectGroups(): error: %v", err)
		return nil, err
	}
	defer rows.Close()

	groups := make([]*model.Group, 0, 16)
	if err := p.dbConn.Select(&groups, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Printf("SelectGroups(): error: %v", err)
		return nil, err
	}

	return groups, nil
}

func (p *pgDb) DeleteGroup(id int64) error {

	// First we need to check whether tables belong to a group
	stmt := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %[1]stables WHERE group_id = $1", p.prefix)
	var cnt int64
	if err := p.dbConn.Get(&cnt, stmt, id); err != nil {
		log.Printf("DeleteGroup(): error: %v", err)
		return err
	}

	if cnt > 0 {
		return fmt.Errorf("This group cannot be deleted because tables belong to it.")
	}

	txn, err := p.dbConn.Begin()
	if err != nil {
		log.Printf("DeleteGroup(): error: %v", err)
		return err
	}

	stmt = fmt.Sprintf("DELETE FROM %[1]suser_groups WHERE group_id = $1", p.prefix)
	if _, err := txn.Exec(stmt, id); err != nil {
		log.Printf("DeleteGroup(): error: %v", err)
		txn.Rollback()
		return err
	}

	stmt = fmt.Sprintf("DELETE FROM %[1]sgroups WHERE id = $1", p.prefix)
	if _, err := txn.Exec(stmt, id); err != nil {
		log.Printf("DeleteGroup(): error: %v", err)
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

type tableRec struct {
	Id               int64
	UserId           int64 `db:"user_id"`
	GroupId          int64 `db:"group_id"`
	Email            string
	DatasetId        int64 `db:"dataset_id"`
	Dataset          string
	Name             string
	Query            string
	Disposition      string
	Partitioned      bool
	LegacySQL        bool `db:"legacy_sql"`
	Description      string
	Error            string
	Running          bool
	Extract          bool
	NotifyExtractUrl string     `db:"notify_extract_url"`
	SheetsExtract    bool       `db:"sheets_extract"`
	SheetId          string     `db:"sheet_id"` // this is a Google id
	ExportDbId       *int64     `db:"export_db_id"`
	ExportTableName  string     `db:"export_table_name"`
	FreqId           *int64     `db:"freq_id"`
	RawConditions    []byte     `db:"conditions"`
	RawReimportCond  []byte     `db:"reimport_cond"`
	ExternalTmout    *int64     `db:"external_tmout"`
	ExternalFormat   string     `db:"external_format"`
	ImportDbId       *int64     `db:"import_db_id"`
	ImportedAt       *time.Time `db:"imported_at"`
	IdColumn         string     `db:"id_column"`
	LastId           string     `db:"last_id"`
	CreatedAt        *time.Time `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
	LastOkRunEndAt   *time.Time `db:"last_ok_run_end_at"`
}

func tableFromTableRec(t *tableRec) (*model.Table, error) {
	var conditions []*scheduler.Condition
	if len(t.RawConditions) > 0 {
		if err := json.Unmarshal(t.RawConditions, &conditions); err != nil {
			return nil, err
		}
	}
	var reimportCond []*scheduler.Condition
	if len(t.RawReimportCond) > 0 {
		if err := json.Unmarshal(t.RawReimportCond, &reimportCond); err != nil {
			return nil, err
		}
	}
	result := model.Table{
		Id:               t.Id,
		UserId:           t.UserId,
		GroupId:          t.GroupId,
		Email:            t.Email,
		DatasetId:        t.DatasetId,
		Dataset:          t.Dataset,
		Name:             t.Name,
		Query:            t.Query,
		Disposition:      t.Disposition,
		Partitioned:      t.Partitioned,
		LegacySQL:        t.LegacySQL,
		Description:      t.Description,
		Error:            t.Error,
		Running:          t.Running,
		Extract:          t.Extract,
		NotifyExtractUrl: t.NotifyExtractUrl,
		SheetsExtract:    t.SheetsExtract,
		SheetId:          t.SheetId,
		IdColumn:         t.IdColumn,
		LastId:           t.LastId,
		ExternalFormat:   t.ExternalFormat,
		Conditions:       conditions,
		ReimportCond:     reimportCond,
		ExportTableName:  t.ExportTableName,
	}
	if t.FreqId != nil {
		result.FreqId = *t.FreqId
	}
	if t.ImportDbId != nil {
		result.ImportDbId = *t.ImportDbId
	}
	if t.ImportedAt != nil {
		result.ImportedAt = *t.ImportedAt
	}
	if t.ExportDbId != nil {
		result.ExportDbId = *t.ExportDbId
	}
	if t.CreatedAt != nil {
		result.CreatedAt = *t.CreatedAt
	}
	if t.DeletedAt != nil {
		result.DeletedAt = *t.DeletedAt
	}
	if t.LastOkRunEndAt != nil {
		result.LastOkRunEndAt = *t.LastOkRunEndAt
	}
	if t.ExternalTmout != nil {
		result.ExternalTmout = *t.ExternalTmout
	}
	return &result, nil
}

func tableRecFromTable(t *model.Table) (*tableRec, error) {
	if len(t.Conditions) == 0 {
		// so that we do not marshal it to "null"
		t.Conditions = make([]*scheduler.Condition, 0)
	}
	rawConditions, err := json.Marshal(t.Conditions)
	if err != nil {
		return nil, err
	}
	if len(t.ReimportCond) == 0 {
		// so that we do not marshal it to "null"
		t.ReimportCond = make([]*scheduler.Condition, 0)
	}
	rawReimportCond, err := json.Marshal(t.ReimportCond)
	if err != nil {
		return nil, err
	}

	result := tableRec{
		Id:               t.Id,
		UserId:           t.UserId,
		GroupId:          t.GroupId,
		DatasetId:        t.DatasetId,
		Name:             t.Name,
		Query:            t.Query,
		Disposition:      t.Disposition,
		Partitioned:      t.Partitioned,
		LegacySQL:        t.LegacySQL,
		Description:      t.Description,
		Error:            t.Error,
		Running:          t.Running,
		NotifyExtractUrl: t.NotifyExtractUrl,
		Extract:          t.Extract,
		SheetsExtract:    t.SheetsExtract,
		SheetId:          t.SheetId,
		ExportDbId:       &t.ExportDbId,
		ExportTableName:  t.ExportTableName,
		FreqId:           &t.FreqId,
		RawConditions:    rawConditions,
		RawReimportCond:  rawReimportCond,
		ImportDbId:       &t.ImportDbId,
		ImportedAt:       &t.ImportedAt,
		IdColumn:         t.IdColumn,
		LastId:           t.LastId,
		CreatedAt:        &t.CreatedAt,
		DeletedAt:        &t.DeletedAt,
		LastOkRunEndAt:   &t.LastOkRunEndAt,
		ExternalTmout:    &t.ExternalTmout,
		ExternalFormat:   t.ExternalFormat,
	}
	if t.FreqId == 0 {
		result.FreqId = nil
	}
	if t.ImportDbId == 0 {
		result.ImportDbId = nil
	}
	if t.ExportDbId == 0 {
		result.ExportDbId = nil
	}
	if t.Disposition == "" {
		result.Disposition = "WRITE_TRUNCATE"
	}
	if t.ImportedAt.IsZero() {
		result.ImportedAt = nil
	}
	if t.CreatedAt.IsZero() {
		result.CreatedAt = nil
	}
	if t.DeletedAt.IsZero() {
		result.DeletedAt = nil
	}
	if t.LastOkRunEndAt.IsZero() {
		result.LastOkRunEndAt = nil
	}
	if t.ExternalTmout == 0 {
		result.ExternalTmout = nil
	}
	return &result, nil
}

func (p *pgDb) Tables(sorttype, order, filter string) ([]*model.Table, error) {
	var (
		tableRecs []*tableRec
	)

	where := "WHERE deleted_at IS NULL"
	if filter == "bq" {
		where += " AND import_db_id IS NULL AND external_tmout IS NULL"
	} else if filter == "import" {
		where += " AND import_db_id IS NOT NULL"
	} else if filter == "external" {
		where += " AND external_tmout IS NOT NULL"
	}

	// NB: the subselect hides "id" column from datasets to avoid "ambiguous reference" error
	stmt := fmt.Sprintf("SELECT %[2]s, dataset FROM %[1]stables t "+
		"JOIN (SELECT id d_id, dataset FROM %[1]sdatasets) d ON t.dataset_id = d_id "+
		where+" ORDER BY %[3]s %[4]s",
		p.prefix, tableColumns, sorttype, order)

	if err := p.dbConn.Select(&tableRecs, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Printf("Tables(): error: %v", err)
		return nil, err
	}

	tables := make([]*model.Table, 0, 16) // make to avoid json null

	for _, rec := range tableRecs {
		table, err := tableFromTableRec(rec)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// Get tables for a specific run.
func (p *pgDb) TablesByFrequency(freqId int64) ([]*model.Table, error) {
	// Since the table list is always going to be relatively small we
	// do this in-memory, not SQL. May be some day it should be an SQL
	// WHERE.

	tables, err := p.Tables("id", "ASC", "")
	if err != nil {
		return nil, err
	}

	result := make([]*model.Table, 0, len(tables))
	for _, t := range tables {
		if t.FreqId == freqId {
			result = append(result, t)
		}
	}

	return result, nil
}

func (p *pgDb) SelectTable(id int64) (*model.Table, error) {
	stmt := fmt.Sprintf(
		`SELECT %[2]s, dataset, email FROM %[1]stables t
            JOIN (SELECT id d_id, dataset FROM %[1]sdatasets) d ON t.dataset_id = d_id
            JOIN (SELECT id u_id, email FROM %[1]susers) u ON t.user_id = u_id
            WHERE id = $1 AND deleted_at IS NULL`,
		p.prefix, tableColumns)

	var tableRec tableRec
	if err := p.dbConn.Get(&tableRec, stmt, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectTable(): error: %v", err)
		return nil, err
	}
	return tableFromTableRec(&tableRec)
}

func (p *pgDb) SelectTableIdByName(dataset, name string) (*int64, error) {
	stmt := fmt.Sprintf(
		"SELECT t.id FROM %[1]stables t JOIN %[1]sdatasets d ON t.dataset_id = d.id "+
			"WHERE d.dataset = $1 AND t.name = $2 AND deleted_at IS NULL",
		p.prefix, dataset, name)

	var id int64
	if err := p.dbConn.Get(&id, stmt, dataset, name); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectTableIdByName(): error: %v", err)
		return nil, err
	}

	return &id, nil
}

func (p *pgDb) SaveTable(t *model.Table) error {
	stmt := fmt.Sprintf("UPDATE %[1]stables AS tables SET user_id = :user_id, dataset_id = :dataset_id, name = :name, query = :query, "+
		"group_id = :group_id, disposition = :disposition, partitioned = :partitioned, "+
		"legacy_sql = :legacy_sql, description = :description, error = :error, running = :running, extract = :extract, "+
		"notify_extract_url = :notify_extract_url, sheets_extract = :sheets_extract, sheet_id = :sheet_id, import_db_id = :import_db_id, "+
		"freq_id = :freq_id, conditions = :conditions, "+
		"reimport_cond = :reimport_cond, imported_at = :imported_at, id_column = :id_column, last_id = :last_id, "+
		"created_at = :created_at, deleted_at = :deleted_at, last_ok_run_end_at = :last_ok_run_end_at, external_tmout = :external_tmout, "+
		"external_format = :external_format, export_db_id = :export_db_id, export_table_name = :export_table_name "+
		"WHERE id = :id ", p.prefix)

	table, err := tableRecFromTable(t)
	if err != nil {
		return err
	}
	if _, err := p.dbConn.NamedExec(stmt, table); err != nil {
		log.Printf("SaveTable(): error: %v", err)
		return err
	}
	return nil
}

func (p *pgDb) InsertTable(t *model.Table) (*model.Table, error) {
	tr, err := tableRecFromTable(t)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]stables AS tables "+
			"       (user_id, dataset_id, name, query, disposition, partitioned, legacy_sql, "+
			"        description, import_db_id, id_column, freq_id, conditions, reimport_cond, "+
			"        extract, notify_extract_url, sheets_extract, sheet_id, created_at, deleted_at, "+
			"        last_ok_run_end_at, external_tmout, external_format, export_db_id, export_table_name, group_id) "+
			"VALUES ($1,      $2,         $3,   $4,    $5,          $6,          $7,"+
			"        $8,          $9,           $10,       $11,     $12,        $13,"+
			"        $14,     $15,                $16,            $17,      $18,        $19,"+
			"        $20,                $21,            $22,             $23,          $24,               $25) "+
			"RETURNING %[2]s", p.prefix, tableColumns)
	var result tableRec
	if err := p.dbConn.Get(&result, stmt, tr.UserId, tr.DatasetId, tr.Name, tr.Query, tr.Disposition, tr.Partitioned, tr.LegacySQL,
		tr.Description, tr.ImportDbId, tr.IdColumn, tr.FreqId, tr.RawConditions, tr.RawReimportCond,
		tr.Extract, tr.NotifyExtractUrl, tr.SheetsExtract, tr.SheetId, tr.CreatedAt, tr.DeletedAt,
		tr.LastOkRunEndAt, tr.ExternalTmout, tr.ExternalFormat, tr.ExportDbId, tr.ExportTableName, tr.GroupId); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("InsertTable(): error: %v", err)
		return nil, err
	}
	return tableFromTableRec(&result)
}

func (p *pgDb) InsertBQJob(j *model.BQJob) (*model.BQJob, error) {
	stmt := fmt.Sprintf("INSERT INTO %[1]sbq_jobs AS jobs "+
		"(table_id, user_id, run_id, parents, bq_job_id, type, configuration, status, query_stats, load_stats, extract_stats, "+
		"creation_time, start_time, end_time, total_bytes_processed, total_bytes_billed, destination_urls, "+
		"import_begin, import_end, import_bytes, import_rows) "+
		"VALUES (:table_id, :user_id, :run_id, :parents, :bq_job_id, :type, :configuration, :status, :query_stats, "+
		":load_stats, :extract_stats, "+
		":creation_time, :start_time, :end_time, :total_bytes_processed, :total_bytes_billed, :destination_urls,"+
		":import_begin, :import_end, :import_bytes, :import_rows) "+
		"RETURNING %[2]s", p.prefix, bqJobColumns)

	var j2 model.BQJob
	rows, err := p.dbConn.NamedQuery(stmt, j)
	if err != nil {
		log.Printf("InsertBQJob(): error: %v", err)
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.StructScan(&j2); err != nil {
			return nil, err
		}
		return &j2, nil
	}
	return nil, nil // should never happen
}

// func (p *pgDb) SelectBQJob(id int64) (*model.BQJob, error) {
// 	var j model.BQJob
// 	if err := p.sqlSelectBQJob.Get(&j, id); err != nil {
// 		if err == sql.ErrNoRows {
// 			return nil, nil // Not found
// 		}
// 		log.Printf("SelectBQJob(): error: %v", err)
// 		return nil, err
// 	}
// 	return &j, nil
// }

func (p *pgDb) UpdateBQJob(j *model.BQJob) error {
	stmt := fmt.Sprintf("UPDATE %[1]sbq_jobs AS jobs "+
		"SET created_at = :created_at, table_id = :table_id, user_id = :user_id, run_id = :run_id, "+
		"parents = :parents, bq_job_id = :bq_job_id, type = :type, "+
		"configuration = :configuration, status = :status, query_stats = :query_stats, load_stats = :load_stats, "+
		"extract_stats = :extract_stats, creation_time = :creation_time, start_time = :start_time, end_time = :end_time, "+
		"total_bytes_processed = :total_bytes_processed, total_bytes_billed = :total_bytes_billed, "+
		"destination_urls = :destination_urls, import_begin = :import_begin, import_end = :import_end, "+
		"import_bytes = :import_bytes, import_rows = :import_rows "+
		"WHERE id = :id", p.prefix)
	if _, err := p.dbConn.NamedExec(stmt, j); err != nil {
		log.Printf("UpdateBQJob(): error: %v", err)
		return err
	}
	return nil
}

func (p *pgDb) RunningBQJobs() ([]*model.BQJob, error) {
	stmt := fmt.Sprintf("SELECT %[2]s FROM %[1]sbq_jobs AS jobs WHERE bq_job_id != '' AND end_time IS NULL OR end_time < start_time ",
		p.prefix, bqJobColumns)

	var result []*model.BQJob
	err := p.dbConn.Select(&result, stmt)
	if err != nil {
		log.Printf("RunningBQJobs(): error: %v", err)
	}
	return result, err
}

func (p *pgDb) SelectBQJobsByTableId(tableId int64, offset, limit int) ([]*model.BQJob, error) {

	stmt := fmt.Sprintf(
		"SELECT %[2]s FROM %[1]sbq_jobs AS jobs WHERE table_id = $1 ORDER BY id DESC OFFSET $2 LIMIT $3",
		p.prefix, bqJobColumns)

	jobs := make([]*model.BQJob, 0) // NB: must be empty slice, not nil for json
	if err := p.dbConn.Select(&jobs, stmt, tableId, offset, limit); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectBQJobsByTableId(): error: %v", err)
		return nil, err
	}
	return jobs, nil
}

func (p *pgDb) SelectBQJobsByRunId(runId int64) ([]*model.BQJob, error) {
	stmt := fmt.Sprintf("SELECT %[2]s FROM %[1]sbq_jobs AS jobs WHERE run_id = $1 ORDER BY id",
		p.prefix, bqJobColumns)
	jobs := make([]*model.BQJob, 0) // NB: must be empty slice, not nil for json
	if err := p.dbConn.Select(&jobs, stmt, runId); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectBQJobsByRunId(): error: %v", err)
		return nil, err
	}
	return jobs, nil
}

func (p *pgDb) SelectBQJobByBQJobId(bqJobId string) (*model.BQJob, error) {
	stmt := fmt.Sprintf(
		"SELECT %[2]s FROM %[1]sbq_jobs AS jobs WHERE bq_job_id = $1", p.prefix, bqJobColumns)
	var j model.BQJob
	if err := p.dbConn.Get(&j, stmt, bqJobId); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectBQJobByBQJobId(): error: %v", err)
		return nil, err
	}
	return &j, nil
}

func (p *pgDb) SetBQConf(projectId, email, privKeyId, key, bucket string) error {

	// Encrypt key
	cryptKey, err := crypto.EncryptString(key, p.secret)
	if err != nil {
		return err
	}
	if _, err = p.dbConn.Exec(fmt.Sprintf("TRUNCATE %[1]sbq_conf", p.prefix)); err != nil {
		return err
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sbq_conf "+
			"       (project_id, email, private_key_id, key, gcs_bucket) "+
			"VALUES ($1,         $2,    $3,             $4,  $5) ", p.prefix)
	_, err = p.dbConn.Exec(stmt, projectId, email, privKeyId, cryptKey, bucket)
	return err
}

func (p *pgDb) SelectBQConf() (*model.BQConf, error) {
	var c model.BQConf
	stmt := fmt.Sprintf("SELECT project_id, email, gcs_bucket, private_key_id, key FROM %[1]sbq_conf LIMIT 1", p.prefix)
	if err := p.dbConn.Get(&c, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectBQConf(): error: %v", err)
		return nil, err
	}

	key, err := crypto.DecryptString(c.CryptKey, p.secret)
	if err != nil {
		log.Printf("SelectBQConf(): error decrypting: %v", err)
		return nil, err
	}
	c.PlainKey = key

	return &c, nil
}

func (p *pgDb) InsertOAuthConf(clientId, secret, redirect, allowedDomain string) error {

	// Encrypt stuff
	cryptSecret, err := crypto.EncryptString(secret, p.secret)
	if err != nil {
		return err
	}
	// Cookie secret is same as our secret
	cryptCookieSecret, err := crypto.EncryptString(p.secret, p.secret)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %[1]soauth_conf "+
			"       (client_id, secret, redirect, cookie_secret, allowed_domain) "+
			"VALUES ($1,        $2,     $3,       $4,            $5) ", p.prefix)
	_, err = p.dbConn.Exec(stmt, clientId, cryptSecret, redirect, cryptCookieSecret, allowedDomain)
	return err
}

func (p *pgDb) SelectOAuthConf() (*model.OAuthConf, error) {
	var c model.OAuthConf
	stmt := fmt.Sprintf("SELECT client_id, secret, redirect, allowed_domain, cookie_secret FROM %[1]soauth_conf LIMIT 1", p.prefix)
	if err := p.dbConn.Get(&c, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectOAuthConf(): error: %v", err)
		return nil, err
	}

	secret, err := crypto.DecryptString(c.CryptSecret, p.secret)
	if err != nil {
		log.Printf("SelectOAuthConf(): error decrypting: %v", err)
		return nil, err
	}
	c.PlainSecret = secret

	secret, err = crypto.DecryptString(c.CryptCookieSecret, p.secret)
	if err != nil {
		log.Printf("SelectOAuthConf(): error decrypting: %v", err)
		return nil, err
	}
	c.PlainCookieSecret = secret

	return &c, nil
}

func (p *pgDb) SetGitConf(repo, token string) error {

	// Encrypt stuff
	cryptToken, err := crypto.EncryptString(token, p.secret)
	if err != nil {
		return err
	}
	if _, err = p.dbConn.Exec(fmt.Sprintf("TRUNCATE %[1]sgit_conf", p.prefix)); err != nil {
		return err
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sgit_conf "+
			"       (url, token) "+
			"VALUES ($1,  $2) ", p.prefix)
	_, err = p.dbConn.Exec(stmt, repo, cryptToken)
	return err
}

func (p *pgDb) SelectGitConf() (*model.GitConf, error) {
	var c model.GitConf
	stmt := fmt.Sprintf("SELECT url, token FROM %[1]sgit_conf LIMIT 1", p.prefix)
	if err := p.dbConn.Get(&c, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectGitConf(): error: %v", err)
		return nil, err
	}

	token, err := crypto.DecryptString(c.CryptToken, p.secret)
	if err != nil {
		log.Printf("SelectGitConf(): error decrypting: %v", err)
		return nil, err
	}
	c.PlainToken = token

	return &c, nil
}

func (p *pgDb) InsertRun(userId *int64, freqId int64) (*model.Run, error) {
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sruns AS runs "+
			"       (user_id, freq_id) "+
			"VALUES ($1,      $2) "+
			"RETURNING id, user_id, created_at, start_time, end_time, error, freq_id", p.prefix)
	var r model.Run
	if err := p.dbConn.Get(&r, stmt, userId, freqId); err != nil {
		es := err.Error()
		if strings.Contains(es, "violates unique constraint") && strings.Contains(es, "freq_id_end_time_null_idx") {
			return nil, fmt.Errorf("There already exists an unfinished run for freq_id %d (end_time NULL).", freqId)
		}
		return nil, err
	}
	return &r, nil
}

func (p *pgDb) UpdateRun(r *model.Run) error {
	stmt := fmt.Sprintf(
		"UPDATE %[1]sruns AS runs "+
			"SET user_id = :user_id, start_time = :start_time, end_time = :end_time, "+
			"error = :error, freq_id = :freq_id "+
			"WHERE id = :id", p.prefix)
	_, err := p.dbConn.NamedExec(stmt, r)
	return err
}

func (p *pgDb) UnfinishedRuns() ([]*model.Run, error) {
	stmt := fmt.Sprintf(
		"SELECT id, user_id, created_at, start_time, end_time, error, freq_id "+
			"FROM %[1]sruns AS runs "+
			"WHERE end_time IS NULL", p.prefix)
	var runs []*model.Run
	if err := p.dbConn.Select(&runs, stmt); err != nil {
		return nil, err
	}
	return runs, nil
}

func (p *pgDb) Runs(offset, limit int) ([]*model.Run, error) {
	stmt := fmt.Sprintf(
		"SELECT runs.id, user_id, created_at, start_time, end_time, error, freq_id, freqs.name AS freq_name, total_bytes "+
			"FROM %[1]sruns AS runs "+
			"JOIN %[1]sfreqs AS freqs ON runs.freq_id = freqs.id "+
			"JOIN LATERAL ( "+
			"  SELECT SUM(total_bytes_billed) AS total_bytes "+
			"    FROM bq_jobs j "+
			"   WHERE j.run_id = runs.id "+
			"  GROUP BY run_id "+
			") j ON true "+
			"ORDER BY id DESC OFFSET $1 LIMIT $2", p.prefix)
	runs := make([]*model.Run, 0)
	if err := p.dbConn.Select(&runs, stmt, offset, limit); err != nil {
		return nil, err
	}
	return runs, nil
}

func (p *pgDb) SelectRun(id int64) (*model.Run, error) {
	stmt := fmt.Sprintf(
		"SELECT id, user_id, created_at, start_time, end_time, error, freq_id "+
			"FROM %[1]sruns AS runs "+
			"WHERE id = $1", p.prefix)
	runs := make([]*model.Run, 0)
	if err := p.dbConn.Select(&runs, stmt, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		return nil, err
	}
	return runs[0], nil
}

func (p *pgDb) SelectFreqs() ([]*model.Freq, error) {
	stmt := fmt.Sprintf(
		"SELECT id, name, period*1000000000 AS period, \"offset\"*1000000000 AS offset, active "+
			"FROM %[1]sfreqs AS freqs "+
			"ORDER BY period", p.prefix)
	freqs := make([]*model.Freq, 0) // avoid json "null"
	if err := p.dbConn.Select(&freqs, stmt); err != nil {
		if err == sql.ErrNoRows {
			return freqs, nil // Not found
		}
		log.Printf("SelectFreqs(): error: %v", err)
		return nil, err
	}
	return freqs, nil
}

func (p *pgDb) InsertFreq(name string, period, offset int, active bool) (*model.Freq, error) {
	var f model.Freq
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sfreqs AS f (name, period, \"offset\", active) VALUES ($1, $2, $3, $4) "+
			"RETURNING id, name, period, \"offset\", active", p.prefix)
	if err := p.dbConn.Get(&f, stmt, name, period, offset, active); err != nil {
		return nil, fmt.Errorf("InsertFreq: unable to insert dataset: %v", err)
	}
	return &f, nil
}

func (p *pgDb) UpdateFreq(f *model.Freq) error {
	stmt := fmt.Sprintf(
		"UPDATE %[1]sfreqs SET name = $2, period = $3, \"offset\" = $4, active = $5 "+
			"WHERE id = $1", p.prefix)
	_, err := p.dbConn.Exec(stmt, f.Id, f.Name, f.Period/1e9, f.Offset/1e9, f.Active)
	return err
}

func (p *pgDb) LogNotification(notify *model.Notification) error {
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]snotifications AS n "+
			"       ( table_id, bq_job_id, created_at, duration_ms, error, url, method, body, resp_status_code, resp_status, resp_headers, resp_body) "+
			"VALUES (:table_id,:bq_job_id,:created_at,:duration_ms, :error,:url,:method,:body,:resp_status_code,:resp_status,:resp_headers,:resp_body) ",
		p.prefix)
	_, err := p.dbConn.NamedExec(stmt, notify)
	return err
}

func (p *pgDb) SelectSlackConf() (*model.SlackConf, error) {
	var c model.SlackConf
	stmt := fmt.Sprintf("SELECT url, username, channel, iconemoji, url_prefix FROM %[1]sslack_conf LIMIT 1", p.prefix)
	if err := p.dbConn.Get(&c, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectSlackConf(): error: %v", err)
		return nil, err
	}
	return &c, nil
}

func (p *pgDb) SetSlackConf(url, username, channel, emoji, prefix string) error {

	if _, err := p.dbConn.Exec(fmt.Sprintf("TRUNCATE %[1]sslack_conf", p.prefix)); err != nil {
		return err
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sslack_conf "+
			"       (url, username, channel, iconemoji, url_prefix) "+
			"VALUES ($1,  $2,       $3,      $4,        $5) ", p.prefix)
	_, err := p.dbConn.Exec(stmt, url, username, channel, emoji, prefix)
	return err
}

func (p *pgDb) SelectDbs() ([]*model.Db, error) {
	// NB: no connect_str or secret here
	idbs := make([]*model.Db, 0, 4) // avoid JSON null
	stmt := fmt.Sprintf("SELECT id, name, dataset_id, driver, dataset, export, connect_str FROM %[1]sdbs idb "+
		"JOIN (SELECT id d_id, dataset FROM %[1]sdatasets) d ON idb.dataset_id = d_id ", p.prefix)
	if err := p.dbConn.Select(&idbs, stmt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectDbs(): error: %v", err)
		return nil, err
	}
	return idbs, nil
}

func (p *pgDb) SelectDbConf(id int64) (*model.Db, error) {
	var idb model.Db
	stmt := fmt.Sprintf("SELECT name, dataset_id, dataset, driver, connect_str, secret, export FROM %[1]sdbs idb "+
		"JOIN (SELECT id d_id, dataset FROM %[1]sdatasets) d ON idb.dataset_id = d_id "+
		"WHERE id = $1", p.prefix)
	if err := p.dbConn.Get(&idb, stmt, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		log.Printf("SelectDbConf(): error: %v", err)
		return nil, err
	}

	// Only interpolate if the secret is not blank
	if idb.CryptSecret != "" {
		secret, err := crypto.DecryptString(idb.CryptSecret, p.secret)
		if err != nil {
			log.Printf("SelectImportDbConf(): error decrypting: %v", err)
			return nil, err
		}
		idb.ConnectStr = fmt.Sprintf(idb.ConnectStr, secret)
	}
	return &idb, nil
}

func (p *pgDb) InsertDbConf(name, driver, dataset string, export bool, connstr, secret string) (err error) {

	dss, err := p.SelectDatasets()
	if err != nil {
		return err
	}

	var dsId int64
	for _, ds := range dss {
		if ds.Dataset == dataset {
			dsId = ds.Id
		}
	}

	if dsId == 0 { // No dataset, create it
		ds, err := p.InsertDataset(dataset)
		if err != nil {
			return err
		}
		dsId = ds.Id
	}

	var cryptSecret string
	if secret != "" {
		if cryptSecret, err = crypto.EncryptString(secret, p.secret); err != nil {
			return err
		}
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sdbs "+
			"       (name, driver, connect_str, dataset_id, secret) "+
			"VALUES ($1,   $2,     $3,          $4,         $5) ", p.prefix)
	_, err = p.dbConn.Exec(stmt, name, driver, connstr, dsId, cryptSecret)
	return err
}

func (p *pgDb) UpdateDbConf(id int64, name, driver, dataset string, export bool, connstr, secret string) (err error) {

	dss, err := p.SelectDatasets()
	if err != nil {
		return err
	}

	var dsId int64
	for _, ds := range dss {
		if ds.Dataset == dataset {
			dsId = ds.Id
		}
	}

	if dsId == 0 { // No dataset, create it
		ds, err := p.InsertDataset(dataset)
		if err != nil {
			return err
		}
		dsId = ds.Id
	}

	var cryptSecret string
	if secret != "" {
		if cryptSecret, err = crypto.EncryptString(secret, p.secret); err != nil {
			return err
		}
		stmt := fmt.Sprintf(
			"UPDATE %[1]sdbs "+
				"SET name = $2, driver = $3, dataset_id = $4, export = $5, connect_str = $6, secret = $7 "+
				"WHERE id = $1", p.prefix)
		_, err = p.dbConn.Exec(stmt, id, name, driver, dsId, export, connstr, cryptSecret)
		return err
	}
	// update without secret
	stmt := fmt.Sprintf(
		"UPDATE %[1]sdbs "+
			"SET name = $2, driver = $3, dataset_id = $4, export = $5, connect_str = $6 "+
			"WHERE id = $1", p.prefix)
	_, err = p.dbConn.Exec(stmt, id, name, driver, dsId, export, connstr)
	return err
}

// Create a dataset. Should be called only when dataset does not exist.
func (p *pgDb) InsertDataset(dataset string) (*model.Dataset, error) {
	ds := &model.Dataset{}
	stmt := fmt.Sprintf(
		"INSERT INTO %[1]sdatasets AS ds (dataset) VALUES ($1) "+
			"RETURNING id, dataset", p.prefix)
	if err := p.dbConn.Get(ds, stmt, dataset); err != nil {
		return nil, fmt.Errorf("InsertDataset: unable to insert dataset: %v", err)
	}
	return ds, nil
}

// Update a dataset given a Dataset pointer.
func (p *pgDb) UpdateDataset(ds *model.Dataset) error {
	stmt := fmt.Sprintf(
		"UPDATE %[1]sdatasets SET dataset = $2 "+
			"WHERE id = $1", p.prefix)
	_, err := p.dbConn.Exec(stmt, ds.Id, ds.Dataset)
	return err
}

func (p *pgDb) SelectDatasets() ([]*model.Dataset, error) {
	stmt := fmt.Sprintf("SELECT id, dataset FROM %[1]sdatasets", p.prefix)

	dss := make([]*model.Dataset, 0) // avoid json "null"
	if err := p.dbConn.Select(&dss, stmt); err != nil {
		if err == sql.ErrNoRows {
			return dss, nil // Not found
		}
		log.Printf("SelectDatasets(): error: %v", err)
		return nil, err
	}
	return dss, nil
}
