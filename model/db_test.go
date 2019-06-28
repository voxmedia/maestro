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

// This unwieldy construct satisfies the Model interface. We can make
// it return whatever we want by supplying our own version of every
// function.

type tDb struct {
	selectFreqs func() ([]*Freq, error)
}

func (m *tDb) SelectOrInsertUserByOAuthId(oAuthId, email string) (*User, bool, error) {
	return nil, false, nil
}
func (m *tDb) SelectUser(id int64) (*User, error) { return nil, nil }
func (m *tDb) SaveUser(*User) error               { return nil }
func (m *tDb) SelectUsers() ([]*User, error)      { return nil, nil }

func (m *tDb) SelectGroups() ([]*Group, error)              { return nil, nil }
func (m *tDb) DeleteGroup(id int64) error                   { return nil }
func (m *tDb) InsertGroup(name string, adminId int64) error { return nil }
func (m *tDb) SaveGroup(g *Group) error                     { return nil }

func (m *tDb) Tables(sorttype, order, filter string) ([]*Table, error)  { return nil, nil }
func (m *tDb) TablesByFrequency(freqId int64) ([]*Table, error)         { return nil, nil }
func (m *tDb) SelectTable(id int64) (*Table, error)                     { return nil, nil }
func (m *tDb) InsertTable(q *Table) (*Table, error)                     { return nil, nil }
func (m *tDb) SaveTable(q *Table) error                                 { return nil }
func (m *tDb) SelectTableIdByName(dataset, name string) (*int64, error) { return nil, nil }

func (m *tDb) SelectBQConf() (*BQConf, error)                                  { return nil, nil }
func (m *tDb) SetBQConf(projectId, email, privKeyId, key, bucket string) error { return nil }

func (m *tDb) InsertBQJob(j *BQJob) (*BQJob, error)                { return nil, nil }
func (m *tDb) UpdateBQJob(j *BQJob) error                          { return nil }
func (m *tDb) RunningBQJobs() ([]*BQJob, error)                    { return nil, nil }
func (m *tDb) SelectBQJobByBQJobId(bqJobId string) (*BQJob, error) { return nil, nil }
func (m *tDb) SelectBQJobsByTableId(tableId int64, offset, limit int) ([]*BQJob, error) {
	return nil, nil
}
func (m *tDb) SelectBQJobsByRunId(runId int64) ([]*BQJob, error) { return nil, nil }

func (m *tDb) InsertRun(userId *int64, freqId int64) (*Run, error) { return nil, nil }
func (m *tDb) SelectRun(id int64) (*Run, error)                    { return nil, nil }
func (m *tDb) UpdateRun(*Run) error                                { return nil }
func (m *tDb) UnfinishedRuns() ([]*Run, error)                     { return nil, nil }
func (m *tDb) Runs(offset, limit int) ([]*Run, error)              { return nil, nil }

func (m *tDb) SelectFreqs() ([]*Freq, error)                    { return m.selectFreqs() }
func (m *tDb) InsertFreq(string, int, int, bool) (*Freq, error) { return nil, nil }
func (m *tDb) UpdateFreq(*Freq) error                           { return nil }

func (m *tDb) InsertDataset(name string) (*Dataset, error) { return nil, nil }
func (m *tDb) UpdateDataset(*Dataset) error                { return nil }
func (m *tDb) SelectDatasets() ([]*Dataset, error)         { return nil, nil }

func (m *tDb) LogNotification(*Notification) error { return nil }

func (m *tDb) SelectDbs() ([]*Db, error)          { return nil, nil }
func (m *tDb) SelectDbConf(id int64) (*Db, error) { return nil, nil }
func (m *tDb) InsertDbConf(name, driver, dataset string, export bool, connstr, secret string) error {
	return nil
}
func (m *tDb) UpdateDbConf(id int64, name, driver, dataset string, export bool, connstr, secret string) error {
	return nil
}

func (m *tDb) SelectOAuthConf() (*OAuthConf, error)                                   { return nil, nil }
func (m *tDb) InsertOAuthConf(clientId, secret, redirect, allowedDomain string) error { return nil }

func (m *tDb) SelectGitConf() (*GitConf, error)    { return nil, nil }
func (m *tDb) SetGitConf(repo, token string) error { return nil }

func (m *tDb) SelectSlackConf() (*SlackConf, error)                            { return nil, nil }
func (m *tDb) SetSlackConf(url, username, channel, emoji, prefix string) error { return nil }
