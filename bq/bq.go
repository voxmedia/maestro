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

// Package bq is responsible for all interactions with BigQuery.
//
// Note that it uses the low level google.golang.org/api/bigquery/v2
// for reasons that may longer be valid, but at the time this was
// written certain functionality was not supported by the higher level
// cloud.google.com/go/bigquery, specifically it had to do with how to
// create a client without a GOOGLE_APPLICATION_CREDENTIALS
// environment variable, see newBqApiClient().
package bq

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// Configration parameters to instantiate a BigQuery connection.
type Config struct {
	ProjectId     string        // ProjectId
	Email         string        // Email (for authentication)
	Key           string        // Key (for authentication)
	GcsBucket     string        // Bucket used for GCS exports
	GcsExpiration time.Duration // Expire GCS URL after (defaults to 4 hours)
}

// BigQuery is a struct containing the information necessary to
// communicate with the BigQuery API.
type BigQuery struct {
	projectId  string
	email      string
	key        string
	bucket     string
	expiration time.Duration
}

const expirationDefault = 4 * time.Hour

// NewBigQuery returns a BigQuery instance given a Config pointer.
func NewBigQuery(cfg *Config) *BigQuery {
	bq := &BigQuery{
		projectId:  cfg.ProjectId,
		email:      cfg.Email,
		key:        cfg.Key,
		bucket:     cfg.GcsBucket,
		expiration: cfg.GcsExpiration,
	}
	if bq.expiration == 0 {
		bq.expiration = expirationDefault
	}
	return bq
}

func (b *BigQuery) ProjectId() string { return b.projectId }

// If this is a googleapi.Error, then try to extract the essential
// message, otherwise return as is
func extractSubmitError(err error) error {
	if gerr, ok := err.(*googleapi.Error); ok {
		return fmt.Errorf("%v", gerr.Message)
	}
	return err
}

// Construct and return a cryptographically signed URL to a GCS
// file. This URL can be used to retrieve the file without any
// authentication. The signature expires after duration provided to
// NewBigQuery().
func (b *BigQuery) SignedStorageUrl(filename, method string) (string, error) {
	// NB: This doesn't try to access Google at all, it's pure crypto
	var contentType string
	if method == "PUT" {
		contentType = "application/octet-stream"
	}
	url, err := storage.SignedURL(b.bucket, filename,
		&storage.SignedURLOptions{
			GoogleAccessID: b.email,
			PrivateKey:     []byte(b.key),
			Method:         method,
			ContentType:    contentType,
			Expires:        time.Now().Add(4 * time.Hour), // TODO make configurable
		})
	if err != nil {
		return "", err
	}
	return url, nil
}

func (b *BigQuery) NewLoadJobConfiguration(table, dataset, wdisp string, gsUrls []string, format string) *bigquery.JobConfiguration {
	return &bigquery.JobConfiguration{
		Load: &bigquery.JobConfigurationLoad{
			SourceFormat:        format, // CSV, NEWLINE_DELIMITED_JSON, AVRO, etc
			SourceUris:          gsUrls,
			Autodetect:          true,
			AllowQuotedNewlines: true,
			CreateDisposition:   "CREATE_IF_NEEDED",
			WriteDisposition:    wdisp,
			DestinationTable: &bigquery.TableReference{
				DatasetId: dataset,
				ProjectId: b.projectId,
				TableId:   table, // TODO "$" partitioning
			},
			// Schema: this is set afterwards, just before the load job is started
		},
	}
}

// Create an extract JobConfiguration to be able to start an extract job.
func (b *BigQuery) NewExtractJobConfiguration(dataset, table string) *bigquery.JobConfiguration {
	gsUrl := fmt.Sprintf("gs://%s/%s_%s_%d_*.csv.gz", b.bucket, dataset, table, time.Now().Unix())
	return &bigquery.JobConfiguration{
		Extract: &bigquery.JobConfigurationExtract{
			DestinationUris:   []string{gsUrl},
			Compression:       "GZIP", // NONE
			DestinationFormat: "CSV",  // JSON, AVRO
			FieldDelimiter:    ",",
			SourceTable: &bigquery.TableReference{
				ProjectId: b.projectId,
				DatasetId: dataset,
				TableId:   table,
			},
		},
	}
}

// Create a query JobConfiguration given the SQL, the destination
// dataset and table, disposition ("WRITE_APPEND" or "WRITE_TRUNCATE),
// whether legacy SQL is used and whether the resulting table is DAY
// partitioned."
func (b *BigQuery) NewQueryJobConfiguration(sql, dataset, table, disp string, legacy, partitioned bool) *bigquery.JobConfiguration {
	var partSpec *bigquery.TimePartitioning
	if partitioned {
		partSpec = &bigquery.TimePartitioning{Type: "DAY"}
	}
	return &bigquery.JobConfiguration{
		Query: &bigquery.JobConfigurationQuery{
			Query: sql,
			DestinationTable: &bigquery.TableReference{
				ProjectId: b.projectId,
				DatasetId: dataset,
				//TableId:   summaryTable + "$" + partition,
				TableId: table,
			},
			WriteDisposition:  disp,
			CreateDisposition: "CREATE_IF_NEEDED",
			// SchemaUpdateOptions: []string{"ALLOW_FIELD_ADDITION"},
			// //, "ALLOW_FIELD_RELAXATION"}, // NB: Relaxation will cause "changed from NULLABLE to REQUIRED" error

			AllowLargeResults: true,

			// Must use ForceSendFields or it won't register for standard SQL!
			// https://github.com/GoogleCloudPlatform/google-cloud-go/blob/master/bigquery/query.go#L169
			UseLegacySql:    &legacy,
			ForceSendFields: []string{"UseLegacySql"},

			TimePartitioning: partSpec,

			// UserDefinedFunctionResources: []*bigquery.UserDefinedFunctionResource{
			// 	&bigquery.UserDefinedFunctionResource{
			// 		InlineCode: string(udf),
			// 	},
			// },
		},
	}

}

// Start a BigQuery job given a JobConfiguration.
func (b *BigQuery) StartJob(conf *bigquery.JobConfiguration) (*bigquery.Job, error) {

	client, err := newBqApiClient(b.email, b.key)
	if err != nil {
		return nil, err
	}

	bqc, err := newBQClient(client, b.projectId)
	if err != nil {
		return nil, err
	}
	bqJob, err := bqc.startJob(conf)
	return bqJob, extractSubmitError(err)
}

// Retrieve Job information from the BigQuery API given a JobId.
func (b *BigQuery) GetJob(jobId string) (*bigquery.Job, error) {

	client, err := newBqApiClient(b.email, b.key)
	if err != nil {
		return nil, err
	}

	bqc, err := newBQClient(client, b.projectId)
	if err != nil {
		return nil, err
	}

	return bqc.getJob(jobId)
}

// Given a function which accepts a []string, call it repeatdly for
// every row of data. This is meant for tables that do not have
// repeated fields, but if a repeated field is encountered, it will be
// simply marshalled as JSON. This uses the BigQuery paging API which
// is slow and only suitable for small tables.
func (b *BigQuery) TableData(dataset, table string, f func([]string) error) error {
	client, err := newBqApiClient(b.email, b.key)
	if err != nil {
		return err
	}
	bqc, err := newBQClient(client, b.projectId)
	if err != nil {
		return err
	}
	return bqc.getTableData(dataset, table, f)
}

// Get table column names as a []string
func (b *BigQuery) TableColumnNames(dataset, table string) ([]string, error) {
	client, err := newBqApiClient(b.email, b.key)
	if err != nil {
		return nil, err
	}
	bqc, err := newBQClient(client, b.projectId)
	if err != nil {
		return nil, err
	}
	return bqc.getTableColumnNames(dataset, table)
}

// Retrieve BigQuery table information form the BigQuery API given a
// dataset and a table.
func (b *BigQuery) GetTable(dataset, table string) (*bigquery.Table, error) {
	client, err := newBqApiClient(b.email, b.key)
	if err != nil {
		return nil, err
	}
	bqc, err := newBQClient(client, b.projectId)
	if err != nil {
		return nil, err
	}
	return bqc.getTable(dataset, table)
}

// Use provided credentials instead of
// GOOGLE_APPLICATION_CREDENTIALS file
func newBqApiClient(email, key string) (*http.Client, error) {
	cfg := &jwt.Config{
		Email:      email,
		PrivateKey: []byte(key),
		Scopes:     append([]string{}, bigquery.BigqueryScope),
		TokenURL:   google.JWTTokenURL,
	}
	ctx := context.Background()
	return oauth2.NewClient(ctx, cfg.TokenSource(ctx)), nil

	// To simply use the creds pointed to
	// GOOGLE_APPLICATION_CREDENTIALS
	//ctx := context.Background()
	//return google.DefaultClient(ctx, bigquery.BigqueryScope)

}
