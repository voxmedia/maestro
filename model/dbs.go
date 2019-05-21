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

// Configuration necessary to connect to import/export databases.
type Db struct {
	Id          int64  `db:"id"`
	Name        string `db:"name"`        // Symbolic name
	DatasetId   int64  `db:"dataset_id"`  // Data will be imported into this dataset
	Dataset     string `db:"dataset"`     // (from datasets table)
	Driver      string `db:"driver"`      // postgres or mysql
	ConnectStr  string `db:"connect_str"` // Connect string
	CryptSecret string `db:"secret"`      // Encrypted secret
	Export      bool   `db:"export"`      // Is this an export database?
}
