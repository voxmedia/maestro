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

// Package oauth provides the integration with Google OAuth.
package oauth

import (
	"net/http"

	"golang.org/x/net/context"
)

type userIdType string

const userIdKey = userIdType("userId")

// ValidUser can save a user whose info came from OAuth.
type userValidator interface {
	ValidUser(id, email string) int64
}

// Provider interface implements the functions necessary for OAuth
// authentication.
type Provider interface {
	Login(successURL string) http.Handler
	Callback() http.Handler
	Logout() http.Handler
	RequireLogin(h http.Handler, allowToken bool) http.Handler
	UpdateCreds(clientId, secret, redirect string)
	HasCreds() bool
}

// Store the userId in Conext of this session.
func SetHttpRequestUserId(r *http.Request, id int64) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), userIdKey, id))
}

// Return userId stored in Context associated with this session.
func GetHttpRequestUserId(r *http.Request) int64 {
	result, _ := r.Context().Value(userIdKey).(int64)
	return result
}
