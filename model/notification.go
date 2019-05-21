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

import "time"

// Record of HTTP notification which optionally happens after a
// table's GCS extract is complete.
type Notification struct {
	Id             int64
	TableId        int64     `db:"table_id"`         // Table
	BqJobId        int64     `db:"bq_job_id"`        // BigQuery job id of the GCS extract
	CreatedAt      time.Time `db:"created_at"`       // Time of notification
	DurationMs     int64     `db:"duration_ms"`      // Duration of the HTTP request/response
	Error          *string   `db:"error"`            // HTTP error, if any
	Url            string    `db:"url"`              // Target URL
	Method         string    `db:"method"`           // HTTP method used (always POST)
	Body           string    `db:"body"`             // Request body
	RespStatusCode int       `db:"resp_status_code"` // Response status code
	RespStatus     string    `db:"resp_status"`      // Response status string
	RespHeaders    string    `db:"resp_headers"`     // Response headers
	RespBody       string    `db:"resp_body"`        // Response body
}
