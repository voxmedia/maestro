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

// Configuration necessary for Git access.
type GitConf struct {
	Url        string `db:"url"`   // Repo URL (https://github.com/org/repo)
	CryptToken string `db:"token"` // GitHub token (encrypted)
	PlainToken string `db:"-"`     // Decrypted token
}
