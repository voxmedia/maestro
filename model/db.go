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

// db is an interface through which all database operations are
// done. the model retains a reference to the db.

type db interface {
	SelectOrInsertUserByOAuthId(oAuthId, email string) (*UserWithGroups, bool, error)
	SelectUser(id int64) (*UserWithGroups, error)
	SaveUser(*UserWithGroups) error
	SelectUsers() ([]*UserWithGroups, error)

	SelectGroups() ([]*Group, error)
	DeleteGroup(id int64) error
	InsertGroup(name string, adminId int64) error
	SaveGroup(g *Group) error

	Tables(sorttype, order, filter string) ([]*Table, error)
	TablesByFrequency(freqId int64) ([]*Table, error)
	SelectTable(id int64) (*Table, error)
	InsertTable(q *Table) (*Table, error)
	SaveTable(q *Table) error
	SelectTableIdByName(dataset, name string) (*int64, error)

	SelectBQConf() (*BQConf, error)
	SetBQConf(projectId, email, privKeyId, key, bucket string) error

	InsertBQJob(j *BQJob) (*BQJob, error)
	UpdateBQJob(j *BQJob) error
	//SelectBQJob(id int64) (*BQJob, error)
	RunningBQJobs() ([]*BQJob, error)
	SelectBQJobByBQJobId(bqJobId string) (*BQJob, error)
	SelectBQJobsByTableId(tableId int64, offset, limit int) ([]*BQJob, error)
	SelectBQJobsByRunId(runId int64) ([]*BQJob, error)

	InsertRun(userId *int64, freqId int64) (*Run, error)
	SelectRun(id int64) (*Run, error)
	UpdateRun(*Run) error
	UnfinishedRuns() ([]*Run, error)
	Runs(offset, limit int) ([]*Run, error)

	SelectFreqs() ([]*Freq, error)
	InsertFreq(string, int, int, bool) (*Freq, error)
	UpdateFreq(*Freq) error

	InsertDataset(name string) (*Dataset, error)
	UpdateDataset(*Dataset) error
	SelectDatasets() ([]*Dataset, error)

	LogNotification(*Notification) error

	SelectDbs() ([]*Db, error)
	SelectDbConf(id int64) (*Db, error)
	InsertDbConf(name, driver, dataset string, export bool, connstr, secret string) error
	UpdateDbConf(id int64, name, driver, dataset string, export bool, connstr, secret string) error

	SelectOAuthConf() (*OAuthConf, error)
	InsertOAuthConf(clientId, secret, redirect, allowedDomain string) error

	SelectGitConf() (*GitConf, error)
	SetGitConf(repo, token string) error

	SelectSlackConf() (*SlackConf, error)
	SetSlackConf(url, username, channel, emoji, prefix string) error
}
