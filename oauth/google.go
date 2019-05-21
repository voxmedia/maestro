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

package oauth

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dghubble/gologin"
	"github.com/dghubble/gologin/google"
	gologin_oauth2 "github.com/dghubble/gologin/oauth2"
	"github.com/dghubble/sessions"
	"github.com/gorilla/securecookie"
	"golang.org/x/oauth2"
	google_oauth2 "golang.org/x/oauth2/google"
)

type googleProvider struct {
	config          *oauth2.Config
	store           *sessions.CookieStore
	storeConfig     gologin.CookieConfig
	sessionName     string
	sessionUserKey  string
	sessionOAuthKey string
	loginURL        string
	tokenChecker    *func(r *http.Request) int64
	db              userValidator
	lastCheck       time.Time
	m               *sync.Mutex
}

type GoogleConfig struct {
	ClientID, ClientSecret, CookieSecret string
	LoginURL, RedirectURL                string
	SessionName, SessionUserKey          string
	Debug                                bool // if true, allows non-SSL cookies
	TokenChecker                         *func(r *http.Request) int64
	UserValidator                        userValidator
}

// Return a new instance of a Google OAuth Provider. This
// implementation maintains a session stored in an encrypted cookie
// using gorilla/securecookie.
func NewGoogle(cfg GoogleConfig) Provider {
	result := &googleProvider{
		config: &oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RedirectURL:  cfg.RedirectURL,
			Endpoint:     google_oauth2.Endpoint,
			Scopes:       []string{"profile", "email"},
		},
		// Note: second arg (blockKey to enable encryption) must be length 16, 24 or 32
		store:           sessions.NewCookieStore([]byte(cfg.CookieSecret), []byte(strings.Repeat(cfg.CookieSecret, 16))[0:16]),
		storeConfig:     gologin.DefaultCookieConfig,
		sessionName:     cfg.SessionName,
		sessionUserKey:  "id",
		sessionOAuthKey: "oid",
		loginURL:        cfg.LoginURL,
		tokenChecker:    cfg.TokenChecker,
		db:              cfg.UserValidator,
		m:               new(sync.Mutex),
	}
	if cfg.Debug {
		result.storeConfig = gologin.DebugOnlyCookieConfig
	}
	return result
}

func (g *googleProvider) UpdateCreds(clientId, secret, redirect string) {
	g.m.Lock()
	defer g.m.Unlock()
	g.config.ClientID = clientId
	g.config.ClientSecret = secret
	g.config.RedirectURL = redirect
}

func (g *googleProvider) HasCreds() bool {
	g.m.Lock()
	defer g.m.Unlock()
	return g.config.ClientSecret != ""
}

func (g *googleProvider) Login(url string) http.Handler {
	h := google.LoginHandler(g.config, nil)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		encodedState, err := securecookie.EncodeMulti("url", url, g.store.Codecs...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ctx := gologin_oauth2.WithState(r.Context(), encodedState)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (g *googleProvider) Callback() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// We need to verify the state parameter. In order to do that
		// we decrypt it via DecodeMulti, and if it decrypts OK, then
		// we save it in the context, this is so that when
		// google.CallbackHandler gets to compare it with what is
		// passed in, they match (because that's what it does -
		// compare what's in the context with what is in the
		// paratmeter).

		state := r.FormValue("state")
		var dst string
		err := securecookie.DecodeMulti("url", state, &dst, g.store.Codecs...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		h := google.CallbackHandler(g.config, g.issueSession(dst), nil)
		ctx := gologin_oauth2.WithState(r.Context(), state)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (g *googleProvider) Logout() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		g.store.Destroy(w, g.sessionName)
		http.Redirect(w, req, "/", http.StatusFound)
	})
}

func (g *googleProvider) RequireLogin(h http.Handler, allowToken bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check the token if TokenChecker is available
		if g.tokenChecker != nil && allowToken {
			if userId := (*g.tokenChecker)(r); userId != 0 {
				r = SetHttpRequestUserId(r, userId)
				h.ServeHTTP(w, r)
				return
			}
		}
		// Fallback to OAuth
		if !g.isAuthenticated(r) {
			if r.URL.String() == "/" { // special case for /, just show login page
				http.Redirect(w, r, g.loginURL, http.StatusFound)
			} else {
				g.Login(r.URL.String()).ServeHTTP(w, r)
			}
			return
		}

		if sess, err := g.store.Get(r, g.sessionName); err == nil {
			userId, _ := sess.Values[g.sessionUserKey].(int64)
			oauthId, _ := sess.Values[g.sessionOAuthKey].(string)

			// Periodically, not more than once per second, check that
			// the user is still valid.
			valid := true
			g.m.Lock()
			if time.Now().Sub(g.lastCheck) > time.Second {
				valid = (g.db.ValidUser(oauthId, "") != 0)
				g.lastCheck = time.Now()
			}
			g.m.Unlock()

			if !valid {
				// Log them out
				g.store.Destroy(w, g.sessionName)
				http.Redirect(w, r, "/", http.StatusFound)
				return
			}

			r = SetHttpRequestUserId(r, userId)
			h.ServeHTTP(w, r)
		}
	})
}

// isAuthenticated returns true if the user has a signed session cookie.
func (g *googleProvider) isAuthenticated(r *http.Request) bool {
	if sess, err := g.store.Get(r, g.sessionName); err == nil {
		userId, _ := sess.Values[g.sessionUserKey].(int64)
		return userId != 0
	}
	return false
}

// issueSession issues a cookie session after successful Google login
func (g *googleProvider) issueSession(redirect string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		googleUser, err := google.UserFromContext(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// The job of ValidUser is to (1) verify/create user and (2)
		// check that the user is allowed. TODO: This needs to be more
		// flexible, URL-specific.
		if id := g.db.ValidUser(googleUser.Id, googleUser.Email); id != 0 {
			session := g.store.New(g.sessionName)
			session.Values[g.sessionUserKey] = id
			session.Values[g.sessionOAuthKey] = googleUser.Id
			session.Save(w)

			http.Redirect(w, req, redirect, http.StatusFound)
			return
		}

		// Authentication/authrization failed, send them to login url
		http.Redirect(w, req, g.loginURL, http.StatusFound)
	})
}
