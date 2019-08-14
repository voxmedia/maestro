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

import (
	"encoding/json"
	"fmt"
	"log"

	"relative/crypto"
)

// This type alias allows us to use StructScan with User.
type GroupArray []*Group

func (g *GroupArray) Scan(src interface{}) error {
	if src != nil {
		if err := json.Unmarshal(src.([]byte), g); err != nil {
			return err
		}
	}
	return nil
}

// User information.
type User struct {
	Id         int64
	OAuthId    string     `db:"oauth_id"`  // Id issued by OAuth
	Email      string     `db:"email"`     // User email (also retreived from OAuth)
	Disabled   bool       `db:"disabled"`  // If true, the used can never log in
	Admin      bool       `db:"admin"`     // Admins can do anything
	CryptToken string     `db:"api_token"` // The token for pythonlib (or other direct API access)
	PlainToken []byte     `db:"-"`         //
	Groups     GroupArray `db:"groups"`    // Groups this user is in (comes from user_groups table join)
}

// Generate and save an API access token. Such a token allowes access
// to some Maestro API's without other authentication, the generated
// token should be kept securely.
func (u *User) GenerateAndSaveToken(m *Model) error {
	token, err := crypto.GenerateToken()
	if err != nil {
		return err
	}
	u.PlainToken = token
	if err := m.SaveUser(u); err != nil {
		return err
	}
	// Log this to Slack
	defer func(user *User) {
		msg := fmt.Sprintf("User %s (%d) generated new Maestro API token.", user.Email, user.Id)
		if err := m.SlackAlert(msg); err != nil {
			log.Printf("GenerateAndSaveToken: slack error: %v", err)
		}
	}(u)
	return nil
}
