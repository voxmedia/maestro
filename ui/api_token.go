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

package ui

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"

	"relative/model"
)

type token struct {
	version byte
	userId  int64
	secret  []byte
}

func decodeToken(s string) (*token, error) {
	// The token format after base64decode is:
	// 1  byte  - version
	// 4  bytes - int32 (network order) user id
	// 32 bytes - secret
	bt, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(bt) != 37 {
		return nil, fmt.Errorf("Invalid token length: %v", len(bt))
	}
	if bt[0] != 0x01 {
		return nil, fmt.Errorf("Invalid version: %v", bt[0])
	}
	return &token{
		version: bt[0],
		userId:  int64(binary.BigEndian.Uint32(bt[1:5])),
		secret:  bt[5:],
	}, nil
}

func encodeToken(userId int64, tok []byte) string {
	result := make([]byte, 37)
	result[0] = 0x01
	binary.BigEndian.PutUint32(result[1:5], uint32(userId))
	copy(result[5:], tok)
	return base64.URLEncoding.EncodeToString(result)
}

func getTokenChecker(m *model.Model) *func(r *http.Request) int64 {
	result := func(r *http.Request) int64 {
		s := r.Header.Get("X-Api-Token")
		if s == "" {
			return 0
		}

		tok, err := decodeToken(s)
		if err != nil {
			log.Printf("getTokenChecker() error: %v", err)
			return 0
		}

		user, err := m.SelectUser(tok.userId)
		if err != nil {
			log.Printf("getTokenChecker() error: %v", err)
			return 0
		}
		if user == nil {
			log.Printf("getTokenChecker(): no such user: %v", tok.userId)
			return 0
		}
		if !bytes.Equal(user.PlainToken, tok.secret) {
			// NB: Resist the temptation to log the secret!
			log.Printf("getTokenChecker() bad token secret")
			return 0
		}
		return user.Id
	}
	return &result
}
