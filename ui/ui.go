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

// Package UI contains all the code behind the Maestro API.
package ui

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"relative/dbsync"
	"relative/model"
	"relative/oauth"

	"github.com/gorilla/mux"
)

type Config struct {
	GoogleClientID    string          // OAuth
	GoogleSecret      string          // OAuth
	GoogleRedirectURL string          // OAuth
	CookieSecret      string          // Just some random constant string
	AllowedDomain     string          // Emails from this domain will pass authentication
	Assets            http.FileSystem // Assets are served from here
	Babel             bool            // true if we are using Babel (vs Webpack)
	ForceSSL          bool            // Check X-Forwarded-Proto and insist on https://
}

// Start the HTTP service.
func Start(cfg Config, m *model.Model, listener net.Listener) {

	server := &http.Server{
		//Addr:           listenSpec,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 16}

	g := oauth.NewGoogle(oauth.GoogleConfig{
		ClientID:      cfg.GoogleClientID,
		ClientSecret:  cfg.GoogleSecret,
		RedirectURL:   cfg.GoogleRedirectURL,
		LoginURL:      "/login/",
		CookieSecret:  cfg.CookieSecret,
		SessionName:   "sess",
		Debug:         true,
		TokenChecker:  getTokenChecker(m),
		UserValidator: m,
	})

	const allowToken = true

	// Root router
	root := http.NewServeMux()
	root.Handle("/", g.RequireLogin(indexHandler(m, cfg.Babel), !allowToken))

	// OAuth (no login required)
	root.Handle("/login", checkOAuthConfig(&cfg, g, g.Login("/"))) // not same as /login/ below
	root.Handle("/login_config", loginConfigHandler(m, g))
	root.Handle("/auth/google_oauth2/callback", g.Callback())
	root.Handle("/logout", g.Logout())

	// Admin subrouter
	ar := mux.NewRouter().PathPrefix("/admin").Subrouter()
	ar.Handle("/", adminHandler(m))
	ar.Handle("/bq_config", bqConfigGetHandler(m)).Methods("GET")
	ar.Handle("/bq_config", bqConfigPostHandler(m)).Methods("POST")
	ar.Handle("/git_config", gitConfigGetHandler(m)).Methods("GET")
	ar.Handle("/git_config", gitConfigPostHandler(m)).Methods("POST")
	ar.Handle("/slack_config", slackConfigGetHandler(m)).Methods("GET")
	ar.Handle("/slack_config", slackConfigPostHandler(m)).Methods("POST")
	ar.Handle("/db_config", dbConfigPostHandler(m)).Methods("POST")
	ar.Handle("/freq", freqPostHandler(m)).Methods("POST")
	ar.Handle("/users", usersHandler(m)).Methods("GET")
	ar.Handle("/group/{id}", groupDeleteHandler(m)).Methods("DELETE")
	ar.Handle("/group", groupPostHandler(m)).Methods("POST")
	ar.Handle("/group", groupPutHandler(m)).Methods("PUT")
	ar.Handle("/user", userPutHandler(m)).Methods("PUT")
	root.Handle("/admin/", g.RequireLogin(requireAdmin(ar, m), !allowToken))

	// User
	ur := mux.NewRouter().PathPrefix("/user").Subrouter()
	ur.Handle("/", userGetHandler(m)).Methods("GET")
	ur.Handle("/token", userTokenGetHandler(m)).Methods("GET")
	root.Handle("/user/", g.RequireLogin(ur, !allowToken))

	// Table Api subrouter We have to wrap each in RequireLogin
	// manually, as some allow tokens.
	tr := mux.NewRouter().PathPrefix("/table").Subrouter()
	tr.Handle("/", g.RequireLogin(tablePostHandler(m), !allowToken)).Methods("POST")
	tr.Handle("/{id}", g.RequireLogin(tableGetHandler(m), allowToken)).Methods("GET")
	tr.Handle("/{id}", g.RequireLogin(tablePutHandler(m), !allowToken)).Methods("PUT")
	tr.Handle("/{id}", g.RequireLogin(tableDeleteHandler(m), !allowToken)).Methods("DELETE")
	tr.Handle("/{id}/status", g.RequireLogin(tableStatusHandler(m), allowToken)).Methods("GET")
	tr.Handle("/{id}/bq_info", g.RequireLogin(tableBQInfoHandler(m), allowToken)).Methods("GET")
	tr.Handle("/{name}/id", g.RequireLogin(tableGetIdByNameHandler(m), allowToken)).Methods("GET")
	tr.Handle("/{id}/load_external", g.RequireLogin(tableLoadExternalHandler(m), allowToken)).Methods("POST")
	tr.Handle("/{id}/jobs", g.RequireLogin(tableJobsHandler(m), !allowToken)).Methods("GET")
	tr.Handle("/{id}/run", g.RequireLogin(tableRunHandler(m), !allowToken)).Methods("GET")
	tr.Handle("/{id}/dryrun", g.RequireLogin(tableDryRunHandler(m), !allowToken)).Methods("GET")
	tr.Handle("/{id}/reimport", g.RequireLogin(tableReimportHandler(m), !allowToken)).Methods("GET")
	tr.Handle("/{id}/extract", g.RequireLogin(tableExtractHandler(m), !allowToken)).Methods("GET")
	tr.Handle("/{id}/sheets_extract", g.RequireLogin(tableSheetsExtractHandler(m), !allowToken)).Methods("GET")
	root.Handle("/table/", tr)

	// More Api
	root.Handle("/tables", g.RequireLogin(tablesHandler(m), !allowToken))
	root.Handle("/freqs", g.RequireLogin(freqsHandler(m), !allowToken))
	root.Handle("/dbs", g.RequireLogin(dbsHandler(m), !allowToken))
	root.Handle("/datasets", g.RequireLogin(datasetsHandler(m), !allowToken))
	root.Handle("/runs", g.RequireLogin(runsHandler(m), !allowToken))
	root.Handle("/groups", g.RequireLogin(groupsHandler(m), !allowToken))

	// Run Api subrouter
	rr := mux.NewRouter().PathPrefix("/run").Subrouter()
	rr.Handle("/{id}/start", requireAdmin(runStartGetHandler(m), m)).Methods("GET")
	rr.Handle("/{id}/resume", requireAdmin(runResumeGetHandler(m), m)).Methods("GET")
	rr.Handle("/{id}/graph", runGraphGetHandler(m)).Methods("GET")
	root.Handle("/run/", g.RequireLogin(rr, !allowToken))

	// Assets
	root.Handle("/login/", http.FileServer(cfg.Assets)) // No login required!
	root.Handle("/images/", g.RequireLogin(http.FileServer(cfg.Assets), !allowToken))
	root.Handle("/js/", g.RequireLogin(http.FileServer(cfg.Assets), !allowToken))
	root.Handle("/css/", g.RequireLogin(http.FileServer(cfg.Assets), !allowToken))

	// maestro.py
	if cfg.Babel {
		root.Handle("/py/", http.StripPrefix("/py/", g.RequireLogin(http.FileServer(http.Dir("pythonlib")), allowToken)))
	} else {
		root.Handle("/py/", g.RequireLogin(http.FileServer(cfg.Assets), allowToken))
	}

	// Test Slack
	root.Handle("/slacktest", g.RequireLogin(slackTestHandler(m), !allowToken))

	// Finally start HTTP
	server.Handler = checkSSL(root, cfg.ForceSSL)
	go server.Serve(listener)
}

// TODO: This is rather primitive
func checkSSL(h http.Handler, require bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if require {
			if r.Header.Get("X-Forwarded-Proto") != "https" {
				sslUrl := "https://" + r.Host + r.RequestURI
				http.Redirect(w, r, sslUrl, http.StatusTemporaryRedirect)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func checkOAuthConfig(cfg *Config, g oauth.Provider, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.HasCreds() {
			r.URL.Path = "/login/creds.html"
			http.FileServer(cfg.Assets).ServeHTTP(w, r)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}

func loginConfigHandler(m *model.Model, g oauth.Provider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// Bail if OAuth config already exists
		if oa, err := m.SelectOAuthConf(); err != nil || oa != nil {
			if oa != nil {
				log.Printf("loginConfigHandler: configuration already exists.")
			} else {
				log.Printf("loginConfigHandler: error: %v", err)
			}
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Set the credentials
		clientId := r.FormValue("clientid")
		secret := r.FormValue("secret")
		redirect := r.FormValue("redirect")
		domain := r.FormValue("domain")

		if len(secret) != 24 {
			http.Error(w, "The Client Secret must be 24 bytes", http.StatusBadRequest)
			return
		}
		if len(clientId) == 0 {
			http.Error(w, "The Client Id cannot be blank", http.StatusBadRequest)
			return
		}
		if len(redirect) == 0 {
			http.Error(w, "The Redirect cannot be blank", http.StatusBadRequest)
			return
		}
		if len(domain) == 0 {
			http.Error(w, "The Domain cannot be blank", http.StatusBadRequest)
			return
		}

		if err := m.InsertOAuthConf(clientId, secret, redirect, domain); err != nil {
			log.Printf("loginConfigHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		g.UpdateCreds(clientId, secret, redirect)
		m.SetAllowedDomain(domain)

		http.Redirect(w, r, "/login", http.StatusFound)
	})
}

func requireAdmin(h http.Handler, m *model.Model) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		userId := oauth.GetHttpRequestUserId(r)
		if userId == 0 {
			fmt.Fprintf(w, "No user information.")
			return
		}

		user, err := m.SelectUser(userId)
		if err != nil {
			log.Printf("requireAdmin(): %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if !user.Admin {
			http.Error(w, "Admin required", http.StatusNotFound)
			return
		}

		h.ServeHTTP(w, r)
	}
}

func adminHandler(m *model.Model) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Admin stuff here!\n")
	}
}

func usersHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		users, err := m.SelectUsers()
		if err != nil {
			log.Printf("usersHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		type UIUser struct {
			Id       int64
			Email    string
			Disabled bool
			Admin    bool
			Groups   []*model.Group
		}

		uius := make([]*UIUser, 0, len(users))
		for _, u := range users {
			uius = append(uius, &UIUser{
				Id:       u.Id,
				Email:    u.Email,
				Disabled: u.Disabled,
				Admin:    u.Admin,
				Groups:   u.Groups})
		}

		js, err := json.Marshal(uius)
		if err != nil {
			log.Printf("usersHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, string(js))
	})
}

func userPutHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var u model.User
		err := decodeJson(r, &u)
		if err != nil {
			log.Printf("userPutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		user, err := m.SelectUser(u.Id)
		if err != nil {
			log.Printf("userPutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Allow changing only these attributes
		user.Disabled = u.Disabled
		user.Admin = u.Admin
		user.Groups = u.Groups

		// Prevent re-encrypting the token
		user.PlainToken = nil

		if err = m.SaveUser(user); err != nil {
			log.Printf("userPutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Log this to Slack

		thisUserId := oauth.GetHttpRequestUserId(r)
		if thisUserId == 0 {
			fmt.Fprintf(w, "No user information.")
			return
		}

		thisUser, err := m.SelectUser(thisUserId)
		if err != nil {
			log.Printf("userPutHandler(): %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		defer func(user, thisUser *model.User) {
			msg := fmt.Sprintf("User %s (%d) changed to {Disabled: %v, Admin: %v} by %s.",
				user.Email, user.Id, user.Disabled, user.Admin, thisUser.Email)
			if err := m.SlackAlert(msg); err != nil {
				log.Printf("userPutHandler: slack error: %v", err)
			}
		}(user, thisUser)

		// Send response

		type UIUser struct {
			Id       int64
			Email    string
			Admin    bool
			Disabled bool
			Groups   []*model.Group
		}

		uiu := &UIUser{
			Id:       user.Id,
			Email:    user.Email,
			Admin:    user.Admin,
			Disabled: user.Disabled,
			Groups:   user.Groups,
		}

		fmt.Fprint(w, toJsonString(uiu))
	})
}

func groupPostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		type UIGroup struct {
			Name        string
			AdminUserId int64
		}
		var ug UIGroup
		err := decodeJson(r, &ug)
		if err != nil {
			log.Printf("groupPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if err := m.InsertGroup(ug.Name, ug.AdminUserId); err != nil {
			log.Printf("groupPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "OK")
	})
}

func groupPutHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var g model.Group
		err := decodeJson(r, &g)
		if err != nil {
			log.Printf("groupPutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if err := m.SaveGroup(&g); err != nil {
			log.Printf("groupPutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "OK")
	})
}

func groupDeleteHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		groupIds := mux.Vars(r)["id"]
		groupId, err := strconv.Atoi(groupIds)
		if err != nil {
			log.Printf("groupDeleteHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		if err := m.DeleteGroup(int64(groupId)); err != nil {
			if strings.Contains(err.Error(), "belong to") {
				http.Error(w, "{\"error\":\"This group cannot be deleted because tables belong to it.\"}", http.StatusBadRequest)
			} else {
				log.Printf("groupDeleteHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
			}
			return
		}
		log.Printf("Group %d deleted.", groupId)
		fmt.Fprintf(w, "OK")
	})
}

func groupsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		groups, err := m.SelectGroups()
		if err != nil {
			log.Printf("groupsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		js, err := json.Marshal(groups)
		if err != nil {
			log.Printf("groupsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, string(js))
	})
}

func bqConfigGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bqc, _ := m.SelectBQConf()
		var bucket string
		if bqc != nil {
			bucket = bqc.GcsBucket
		}
		var dataset string
		dss, _ := m.SelectDatasets()
		if len(dss) > 0 {
			dataset = dss[0].Dataset
		}
		fmt.Fprintf(w, "{\"bucket\": %q, \"dataset\": %q}\n", bucket, dataset)
	})
}

func bqConfigPostHandler(m *model.Model) http.Handler {
	type credsRec struct {
		// Common fields
		Type      string
		ClientID  string `json:"client_id"`
		ProjectID string `json:"project_id"`

		// User Credential fields
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`

		// Service Account fields
		ClientEmail  string `json:"client_email"`
		PrivateKeyID string `json:"private_key_id"`
		PrivateKey   string `json:"private_key"`
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		type UIBQCreds struct {
			Creds, Bucket, Dataset string
		}
		var c UIBQCreds
		err := decodeJson(r, &c)
		if err != nil {
			log.Printf("bqConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		bqc, err := m.SelectBQConf()
		if err != nil {
			log.Printf("bqConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if len(c.Creds) == 0 && len(c.Bucket) == 0 && len(c.Dataset) == 0 {
			http.Error(w, "No data provided?", http.StatusBadRequest)
			return
		}

		var creds credsRec
		if len(c.Creds) > 0 {
			if err := json.Unmarshal([]byte(c.Creds), &creds); err != nil {
				log.Printf("bqConfigPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		} else if bqc != nil {
			creds.ProjectID = bqc.ProjectId
			creds.ClientEmail = bqc.Email
			creds.PrivateKeyID = bqc.KeyId
			creds.PrivateKey = bqc.PlainKey
		}

		var bucket string
		if bqc != nil {
			bucket = bqc.GcsBucket
			if len(c.Bucket) > 0 {
				bucket = c.Bucket
			}
		}

		if err := m.SetBQConf(creds.ProjectID, creds.ClientEmail,
			creds.PrivateKeyID, creds.PrivateKey, bucket); err != nil {
			log.Printf("bqConfigPostHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		dss, err := m.SelectDatasets()
		if err != nil {
			log.Printf("bqConfigPostHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		if len(dss) > 0 && dss[0].Dataset != c.Dataset {
			dss[0].Dataset = c.Dataset
			if err := m.UpdateDataset(dss[0]); err != nil {
				log.Printf("bqConfigPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		fmt.Fprintf(w, "BigQuery configuration created, please restart Maestro now.\n")
	})
}

func gitConfigGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gc, _ := m.SelectGitConf()
		var url string
		if gc != nil {
			url = gc.Url
		}
		fmt.Fprintf(w, "{\"repo\": %q}\n", url)
	})
}

func gitConfigPostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type UIGitCreds struct {
			Repo, Token string
		}
		var c UIGitCreds
		err := decodeJson(r, &c)
		if err != nil {
			log.Printf("gitConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if len(c.Repo) == 0 && len(c.Token) == 0 {
			http.Error(w, "No data provided?", http.StatusBadRequest)
			return
		}

		gc, err := m.SelectGitConf()
		if err != nil {
			log.Printf("gitConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		var repo, tok string
		if gc != nil {
			repo = gc.Url
			tok = gc.PlainToken
		}

		if len(c.Repo) > 0 {
			repo = c.Repo
		}

		if len(c.Token) > 0 {
			tok = c.Token
			if len(tok) != 40 {
				http.Error(w, "GitHub token must be 40 bytes long.", http.StatusBadRequest)
				return
			}
		}

		if err := m.SetGitConf(repo, tok); err != nil {
			log.Printf("gitConfigPostHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "GitHub configuration created, please restart Maestro now.\n")
	})
}

func slackConfigGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc, _ := m.SelectSlackConf()
		if sc == nil {
			fmt.Fprintf(w, "{}\n")
			return
		}
		fmt.Fprintf(w, "{\"slack_url\":%q,\n\"slack_username\":%q,\n\"slack_channel\":%q,\n\"slack_emoji\":%q,\n\"slack_prefix\":%q}\n",
			sc.Url, sc.UserName, sc.Channel, sc.IconEmoji, sc.UrlPrefix)
	})
}

func slackConfigPostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type UISlackConfig struct {
			Url      string `json:"slack_url"`
			Username string `json:"slack_username"`
			Channel  string `json:"slack_channel"`
			Emoji    string `json:"slack_emoji"`
			Prefix   string `json:"slack_prefix"`
		}
		var c UISlackConfig
		err := decodeJson(r, &c)
		if err != nil {
			log.Printf("slackConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if err := m.SetSlackConf(c.Url, c.Username, c.Channel, c.Emoji, c.Prefix); err != nil {
			log.Printf("slackConfigPostHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "Slack configuration created, please restart Maestro now.\n")
	})
}

func dbConfigPostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type UIDbConfig struct {
			Id         int64
			Name       string
			Driver     string
			Dataset    string
			Export     bool
			ConnectStr string
			Secret     string
		}
		var c UIDbConfig
		err := decodeJson(r, &c)
		if err != nil {
			log.Printf("dbConfigPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if len(c.Name) == 0 || len(c.ConnectStr) == 0 || len(c.Dataset) == 0 {
			http.Error(w, "Name, connection string, dataset cannot be blank.", http.StatusBadRequest)
			return
		}

		if c.Driver != "mysql" && c.Driver != "postgres" {
			http.Error(w, "Driver must be mysql or postgres.", http.StatusBadRequest)
			return
		}

		if c.Id == 0 {
			if err := m.InsertDbConf(c.Name, c.Driver, c.Dataset, c.Export, c.ConnectStr, c.Secret); err != nil {
				log.Printf("dbConfigPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		} else {
			if err := m.UpdateDbConf(c.Id, c.Name, c.Driver, c.Dataset, c.Export, c.ConnectStr, c.Secret); err != nil {
				log.Printf("dbConfigPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		fmt.Fprintf(w, "DB configuration created, please restart Maestro now.\n")
	})
}

func freqPostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type UIFreq struct {
			Id             int
			Name           string
			Period, Offset int
			Active         bool
		}

		var uf UIFreq
		err := decodeJson(r, &uf)
		if err != nil {
			log.Printf("freqPostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if len(uf.Name) == 0 || uf.Period == 0 {
			http.Error(w, "Name cannot be blank, period must > 0.", http.StatusBadRequest)
			return
		}

		if uf.Offset >= uf.Period {
			http.Error(w, "Offset must be less than Period", http.StatusBadRequest)
			return
		}

		if uf.Id == 0 {
			if _, err := m.InsertFreq(uf.Name, uf.Period, uf.Offset, uf.Active); err != nil {
				log.Printf("freqPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		} else {
			f := model.Freq{
				Id:     int64(uf.Id),
				Name:   uf.Name,
				Period: time.Duration(uf.Period * 1e9),
				Offset: time.Duration(uf.Offset * 1e9),
				Active: uf.Active,
			}
			if err := m.UpdateFreq(&f); err != nil {
				log.Printf("freqPostHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		fmt.Fprintf(w, "Frequency update/created, please restart Maestro now.\n")
	})
}

func userGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		userId := oauth.GetHttpRequestUserId(r)
		if userId == 0 {
			fmt.Fprintf(w, "No user information.")
			return
		}

		user, err := m.SelectUser(userId)
		if err != nil {
			log.Printf("userGetHandler(): %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		type UIUser struct {
			Id     int64
			Email  string
			Admin  bool
			Groups []*model.Group
		}

		uiu := &UIUser{
			Id:     user.Id,
			Email:  user.Email,
			Admin:  user.Admin,
			Groups: user.Groups,
		}

		fmt.Fprint(w, toJsonString(uiu))
	})
}

func userTokenGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		userId := oauth.GetHttpRequestUserId(r)
		if userId == 0 {
			fmt.Fprintf(w, "No user information.")
			return
		}

		user, err := m.SelectUser(userId)
		if err != nil {
			log.Printf("userTokenGetHandler(): %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if user == nil {
			log.Printf("userTokenGetHandler(): user id %d not found", userId)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if len(user.PlainToken) == 0 {
			if err := user.GenerateAndSaveToken(m); err != nil {
				log.Printf("userTokenGetHandler(): generate token error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		// Log this to Slack
		defer func(user *model.User) {
			msg := fmt.Sprintf("User %s (%d) accessed their Maestro API token.", user.Email, user.Id)
			if err := m.SlackAlert(msg); err != nil {
				log.Printf("userTokenGetHandler: slack error: %v", err)
			}
		}(user)

		tok := encodeToken(user.Id, user.PlainToken)
		fmt.Fprintf(w, `{"api_token":%q}`, tok)
	})
}

func tablesHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sorttype := r.URL.Query().Get("sort")
		sortorder := r.URL.Query().Get("order")
		filter := r.URL.Query().Get("filter")

		if sorttype == "" {
			sorttype = "id"
		}
		if sortorder == "" {
			sortorder = "asc"
		}

		sortTypes := map[string]bool{
			"name": true,
			"id":   true,
		}
		sortOrders := map[string]bool{
			"asc":  true,
			"desc": true,
		}
		filters := map[string]bool{
			"bq":       true,
			"import":   true,
			"external": true,
		}
		if !sortTypes[sorttype] || !sortOrders[sortorder] {
			log.Printf("tablesHandler: error: invalid sort args: %v %v", sorttype, sortorder)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		if filter != "" && !filters[filter] {
			log.Printf("tablesHandler: error: invalid filter args: %v", filter)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if filter == "import" {
			sorttype = "import_db_id, " + sorttype
		}

		tables, err := m.Tables(sorttype, sortorder, filter)
		if err != nil {
			log.Printf("tablesHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		js, err := json.Marshal(tables)
		if err != nil {
			log.Printf("tablesHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, string(js))
	})
}

// List available frequencies
func freqsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		freqs, err := m.SelectFreqs()
		if err != nil {
			log.Printf("freqsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		js, err := json.Marshal(freqs)
		if err != nil {
			log.Printf("freqsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, string(js))
	})
}

// List available datasets
func datasetsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dss, err := m.SelectDatasets()
		if err != nil {
			log.Printf("datasetssHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		js, err := json.Marshal(dss)
		if err != nil {
			log.Printf("datasetsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, string(js))
	})
}

// List available dbs
func dbsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idbs, err := m.SelectDbs()
		if err != nil {
			log.Printf("dbsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		js, err := json.Marshal(idbs)
		if err != nil {
			log.Printf("dbsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprint(w, string(js))
	})
}

func tableGetHandler(m *model.Model) http.Handler {

	// model.Table with extra info
	type UITableParent struct {
		Id      int64
		Dataset string
		Name    string
	}
	type UITableExtract struct {
		Id        int64
		StartTime *time.Time
		URLs      []string
	}
	type UITable struct {
		*model.Table
		GithubUrl    string
		ImportSelect dbsync.PrimitiveSelect
		Parents      []*UITableParent
		Extracts     *UITableExtract
		UploadURL    string `json:",omitempty"`
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if t == nil {
			http.Error(w, fmt.Sprintf("Table id %v not found", id), http.StatusNotFound)
			return
		}

		tt := &UITable{
			Table:     t,
			GithubUrl: t.GithubUrl(m),
			Parents:   make([]*UITableParent, 0),
			Extracts:  new(UITableExtract),
		}

		if t.IsExternal() {
			tt.UploadURL = t.SignedUploadURL(m)
		}

		// Add a list of parents
		parents, _ := m.TablesParents([]*model.Table{t})
		for _, p := range parents[t.Name] {
			tt.Parents = append(tt.Parents, &UITableParent{
				Id:      p.Id,
				Dataset: p.Dataset,
				Name:    p.Name})
		}

		// GCS Extracts
		extractJob, _ := t.LastExtractJob(m)
		if extractJob != nil {
			signeds, _ := extractJob.SignedExtractURLs(m)
			tt.Extracts = &UITableExtract{
				Id:        extractJob.Id,
				StartTime: extractJob.StartTime,
				URLs:      signeds,
			}
		}

		if t.IsImport() && t.Query != "" {
			if err := json.Unmarshal([]byte(t.Query), &tt.ImportSelect); err != nil {
				log.Printf("tableGetHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		fmt.Fprint(w, toJsonString(tt))
	})
}

func tablePostHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var t model.Table
		err := decodeJson(r, &t)
		if err != nil {
			log.Printf("tablePostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t.UserId = oauth.GetHttpRequestUserId(r) // Assign owner

		user, err := m.SelectUser(t.UserId)
		if err != nil {
			log.Printf("tablePostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Check that the group is one of user's
		if !user.Admin {
			var ok bool
			for _, g := range user.Groups {
				if g.Id == t.GroupId {
					ok = true
					break
				}
			}
			if !ok {
				err := fmt.Errorf("Group id %d is not allowed for user %d.", t.GroupId, user.Id)
				log.Printf("tablePostHandler() error: %v\n", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		nt, err := m.InsertTable(&t)
		if err != nil {
			// pass the error to the client
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := nt.QueueGitCommit(m, user.Email); err != nil {
			log.Printf("tablePostHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, toJsonString(struct{ Id int64 }{nt.Id}))
	})
}

func tableWriteAccessAllowed(m *model.Model, uid, tid int64) bool {

	// Write access should be required for anything that modifies the
	// table or its parameters, e.g. any changes to the SQL, or the
	// actual running of the table.

	table, err := m.SelectTable(tid)
	if err != nil {
		log.Printf("tableWriteAccessAllowed() error: %v\n", err)
		return false
	}

	user, err := m.SelectUser(uid)
	if err != nil {
		log.Printf("tableWriteAccessAllowed() error: %v\n", err)
		return false
	}

	if user.Admin { // Admins can do anything
		return true
	}

	// If any user group matches the table group, we are good
	for _, ug := range user.Groups {
		if ug.Id == table.GroupId {
			return true
		}
	}
	return false
}

func tablePutHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var t model.Table
		err := decodeJson(r, &t)
		if err != nil {
			log.Printf("tablePutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		userId := oauth.GetHttpRequestUserId(r)
		if !tableWriteAccessAllowed(m, userId, t.Id) {
			log.Printf("tablePutHandler(): user %d has no write access to table %d", userId, t.Id)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Clear out errors and delete import status (if error)
		t.Error = ""
		if m.GetImportStatus(t.Id) == model.ImpError {
			m.DeleteImportStatus(t.Id)
		}

		err = m.SaveTable(&t)
		if err != nil {
			log.Printf("tablePutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		user, err := m.SelectUser(userId)
		if err != nil {
			log.Printf("tablePutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		if err := t.QueueGitCommit(m, user.Email); err != nil {
			log.Printf("tablePutHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "OK\n")
	})
}

func tableDeleteHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableDeleteHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableDeleteHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if t == nil {
			http.Error(w, fmt.Sprintf("Table id %v not found", id), http.StatusNotFound)
			return
		}

		// Check access
		userId := oauth.GetHttpRequestUserId(r)
		if !tableWriteAccessAllowed(m, userId, t.Id) {
			log.Printf("tableDeleteHandler(): user %d has no write access to table %d", userId, t.Id)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t.DeletedAt = time.Now()

		err = m.SaveTable(t)
		if err != nil {
			log.Printf("tableDeleteHandler() error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		user, err := m.SelectUser(userId)
		if err != nil {
			log.Printf("tableDeleteHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		if err := t.QueueGitCommit(m, user.Email); err != nil {
			log.Printf("tableDeleteHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "OK\n")
	})
}

func tableRunHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableRunHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableRunHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Check access
		userId := oauth.GetHttpRequestUserId(r)
		if !tableWriteAccessAllowed(m, userId, t.Id) {
			log.Printf("tableRunHandler(): user %d has no write access to table %d", userId, t.Id)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Starting run for table id: %v requested by userId: %v", t.Id, userId)

		jobId, err := m.RunTable(t, &userId)
		if err != nil {
			log.Printf("tableRunHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		log.Printf("Started run for table id: %v OK.", t.Id)

		fmt.Fprintf(w, fmt.Sprintf("{\"bq_job_id\": %q}\n", jobId))
	})
}

func tableDryRunHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableDryRunHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		q, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableDryRunHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Starting DRY run for table id: %v", q.Id)

		userId := oauth.GetHttpRequestUserId(r)
		err = m.DryRunTable(q, &userId)
		if err != nil {
			log.Printf("tableDryRunHandler: error: %v", err)
		}

		if err != nil {
			fmt.Fprintf(w, fmt.Sprintf("{\"error\": %q}\n", err.Error()))
		} else {
			fmt.Fprint(w, "{\"error\": null}\n")
		}
	})
}

func tableReimportHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableReimportHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableReimportHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Check access
		userId := oauth.GetHttpRequestUserId(r)
		if !tableWriteAccessAllowed(m, userId, t.Id) {
			log.Printf("tableReimportHandler(): user %d has no write access to table %d", userId, t.Id)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Starting reimport for table id: %v", t.Id)

		if err := m.ReimportTable(t, &userId, nil); err != nil {
			log.Printf("tableReimportHandler error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Started reimport for table id: %v OK.", t.Id)

		fmt.Fprintf(w, "OK\n")
	})
}

func tableStatusHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableStatusHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableStatusHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if t == nil {
			http.Error(w, fmt.Sprintf("Table id %v not found", id), http.StatusNotFound)
			return
		}

		status := ""
		if t.Running {
			status = "running"
		} else if t.Error != "" {
			status = "error"
		}

		fmt.Fprintf(w, "{\"Id\": %d, \"Status\": %q, \"Error\": %q, \"LastOkRunEndAt\": %q}\n", t.Id, status, t.Error, t.LastOkRunEndAt.Format(time.RFC3339))
	})
}

func tableBQInfoHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableBQInfoHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableBQInfoHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		bqi, err := t.GetBQInfo(m)
		if err != nil {
			log.Printf("tableBQInfoHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		js, err := json.Marshal(bqi)
		if err != nil {
			log.Printf("tableBQInfoHandler: error 4: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprint(w, string(js))
	})
}

func tableGetIdByNameHandler(m *model.Model) http.Handler {
	// dataset_name.table_name
	re := regexp.MustCompile("^([[:word:]]+)\\.([[:word:]]+)$")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fullname := mux.Vars(r)["name"]
		match := re.FindAllStringSubmatch(fullname, -1)
		if len(match) == 0 {
			fmt.Fprintf(w, "{\"Id\": null, \"Error\":\"Invalid dataset_name.table_name format\"}\n")
			return
		}

		dataset, name := match[0][1], match[0][2]
		id, err := m.SelectTableIdByName(dataset, name)
		if err != nil {
			log.Printf("tableGetIdByNameHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if id == nil {
			fmt.Fprintf(w, "{\"Id\": null}\n")
		} else {
			fmt.Fprintf(w, "{\"Id\": %d}\n", *id)
		}
	})
}

func tableLoadExternalHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableLoadExternalHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// just the filename, no signature - the bucket we already know
		fname := r.FormValue("fn")

		t, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableLoadExternalHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		t.Error = "" // clear error
		err = m.SaveTable(t)
		if err != nil {
			log.Printf("tableLoadExternalHandler: error: %v\n", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		userId := oauth.GetHttpRequestUserId(r)
		if err := t.ExternalLoad(m, fname, &userId); err != nil {
			log.Printf("tableLoadExternalHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "OK\n")
	})
}

func tableJobsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			page int
			err  error
		)

		ids := mux.Vars(r)["id"]
		if ids == "new" || ids == "new_import" || ids == "new_external" {
			fmt.Fprintf(w, "[]\n")
			return
		}

		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableJobsHandler: error 1: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		p := r.URL.Query().Get("p")
		if p != "" {
			page, err = strconv.Atoi(p)
			if err != nil {
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		const pageSize = 20
		offset := page * pageSize

		jobs, err := m.SelectBQJobsByTableId(int64(id), offset, pageSize)
		if err != nil {
			log.Printf("tableJobsHandler: error 3: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		js, err := json.Marshal(jobs)
		if err != nil {
			log.Printf("tableJobsHandler: error 4: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprint(w, string(js))
	})
}

func tableExtractHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		q, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Submitting a BQ extract for table id: %v", q.Id)

		userId := oauth.GetHttpRequestUserId(r)
		jobId, err := m.ExtractTableToGCS(q, &userId, nil)
		if err != nil {
			log.Printf("tableExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		log.Printf("Submitted BQ extract for table id: %v OK.", q.Id)

		fmt.Fprintf(w, fmt.Sprintf("{\"bq_job_id\": %q}\n", jobId))
	})
}

func tableSheetsExtractHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := mux.Vars(r)["id"]
		id, err := strconv.Atoi(ids)
		if err != nil {
			log.Printf("tableSheetsExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		q, err := m.SelectTable(int64(id))
		if err != nil {
			log.Printf("tableSheetsExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		log.Printf("Submitting a Sheets extract for table id: %v", q.Id)

		err = m.ExtractTableToSheet(q)
		if err != nil {
			log.Printf("tableSheetsExtractHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		log.Printf("Submitted a Sheets extract for table id: %v OK.", q.Id)

		fmt.Fprintf(w, "OK\n")
	})
}

func runsHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var (
			page int
			err  error
		)

		p := r.URL.Query().Get("p")
		if p != "" {
			page, err = strconv.Atoi(p)
			if err != nil {
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}
		}

		const pageSize = 20
		offset := page * pageSize

		runs, err := m.Runs(offset, pageSize)
		if err != nil {
			log.Printf("runsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		js, err := json.Marshal(runs)
		if err != nil {
			log.Printf("runsHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, string(js))
	})
}

func runStartGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// NB: id is a freq_id here
		freqIds := mux.Vars(r)["id"]
		freqId, err := strconv.Atoi(freqIds)
		if err != nil {
			log.Printf("runStartGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		userId := oauth.GetHttpRequestUserId(r)
		run := model.NewRun(int64(freqId), &userId)

		if err := run.Assemble(m, time.Now()); err != nil {
			log.Printf("runStartGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if err := run.Start(m); err != nil {
			log.Printf("runStartGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
	})
}

func runResumeGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// NB: id is a run_id here
		runIds := mux.Vars(r)["id"]
		runId, err := strconv.Atoi(runIds)
		if err != nil {
			log.Printf("runResumeGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		run, err := m.SelectRun(int64(runId))
		if err != nil {
			log.Printf("runResumeGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		if err := run.Resume(m); err != nil {
			log.Printf("runResumeGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "OK")
	})
}

func runGraphGetHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runIds := mux.Vars(r)["id"]
		runId, err := strconv.Atoi(runIds)
		if err != nil {
			log.Printf("runGraphGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		type UINode struct {
			Id      string `json:"id"`
			Dataset string `json:"dataset"`
			Table   string `json:"table"`
			Status  string `json:"status"`
			Error   string `json:"error"`
			Score   int    `json:"score"`
			Orphan  bool   `json:"orphan"`
		}

		type UIEdge struct {
			From string `json:"from"`
			To   string `json:"to"`
		}

		type UIGraph struct {
			Nodes []*UINode `json:"nodes"`
			Edges []*UIEdge `json:"edges"`
		}

		run := &model.Run{Id: int64(runId)}
		g, err := run.Graph(m)
		if err != nil {
			log.Printf("runGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		// Collect scores
		scores := make(map[string]int)
		for _, v := range g.Scores() {
			scores[v.Name] = v.Score
		}

		// Recreate the graph because we do not want the sentinel that g.Scores() added
		g, _ = run.Graph(m)
		ug := &UIGraph{Nodes: make([]*UINode, 0), Edges: make([]*UIEdge, 0)}
		for k, v := range g {

			job := v.Item.(*model.BQJob)
			tref, err := job.TableReference()
			if err != nil {
				log.Printf("runGetHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}

			status, errStr, err := job.GetStatus()
			if err != nil {
				log.Printf("runGetHandler: error: %v", err)
				http.Error(w, "This is an error", http.StatusBadRequest)
				return
			}

			// Nodes
			ug.Nodes = append(ug.Nodes, &UINode{
				Id:      v.Item.GetName(), // This is our table_id
				Dataset: tref.DatasetId,
				Table:   tref.TableId,
				Status:  status,
				Error:   errStr,
				Orphan:  len(v.Parents()) == 0 && len(v.Children()) == 0,
				Score:   scores[k],
			})

			// Edges
			for kk, _ := range v.Parents() {
				fr, to := k, kk
				ug.Edges = append(ug.Edges, &UIEdge{From: fr, To: to})
			}
		}

		js, err := json.Marshal(ug)
		if err != nil {
			log.Printf("runGetHandler: error: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, string(js))

	})
}

func slackTestHandler(m *model.Model) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msgs := r.URL.Query()["msg"]
		var msg string
		if len(msgs) > 0 {
			msg = msgs[0]
		} else {
			msg = "Someone hit the <{URL_PREFIX}/slacktest|/slacktest> URL in Maestro. If you are seeing this, " +
				"it means Slack integration is working."
		}
		if err := m.SlackAlert(msg); err != nil {
			log.Printf("slackTestHandler: %v", err)
			http.Error(w, "This is an error", http.StatusBadRequest)
		}
		fmt.Fprintf(w, "Message posted.\n")
		return
	})
}

func indexHandler(m *model.Model, babel bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if babel {
			fmt.Fprintf(w, indexHTMLBabel)
		} else {
			fmt.Fprintf(w, indexHTML)
		}
	})
}

const (
	// Remember that versions are also specified in package.json, you
	// must change them there as well as here for "make build"!
	cdnReact                  = "https://cdnjs.cloudflare.com/ajax/libs/react/16.4.2/umd/react.production.min.js"
	cdnReactDom               = "https://cdnjs.cloudflare.com/ajax/libs/react-dom/16.4.2/umd/react-dom.production.min.js"
	cdnReactRouter            = "https://cdnjs.cloudflare.com/ajax/libs/react-router/4.3.1/react-router.min.js"
	cdnReactRouterDom         = "https://cdnjs.cloudflare.com/ajax/libs/react-router-dom/4.3.1/react-router-dom.min.js"
	cdnPropTypes              = "https://cdnjs.cloudflare.com/ajax/libs/prop-types/15.6.2/prop-types.min.js"
	cdnAxios                  = "https://cdnjs.cloudflare.com/ajax/libs/axios/0.18.0/axios.min.js"
	cdnCodeMirror             = "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.28.0/codemirror.min.js"
	cdnCodeMirrorCss          = "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.28.0/codemirror.min.css"
	cdnReactBootstrap         = "https://cdnjs.cloudflare.com/ajax/libs/react-bootstrap/0.32.4/react-bootstrap.min.js"
	cdnReactRouterBootstrap   = "https://cdnjs.cloudflare.com/ajax/libs/react-router-bootstrap/0.24.4/ReactRouterBootstrap.min.js"
	cdnBootstrapCss           = "https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/3.3.7/css/bootstrap.min.css"
	cdnReactBootstrapTable    = "https://unpkg.com/react-bootstrap-table@4.3.1/dist/react-bootstrap-table.min.js"
	cdnReactBootstrapTableCss = "https://unpkg.com/react-bootstrap-table@4.3.1/dist/react-bootstrap-table.min.css"
	cdnVis                    = "https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"
	cdnVisCss                 = "https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.css"
)

const indexHTML = `
<!DOCTYPE HTML>
<html>
  <head>
    <meta charset="utf-8">
    <link rel="stylesheet" type="text/css" href="css/style.css">
    <link rel="stylesheet" href="` + cdnCodeMirrorCss + `">
    <link rel="stylesheet" href="` + cdnBootstrapCss + `">
    <link rel="stylesheet" href="` + cdnReactBootstrapTableCss + `">
    <link rel="stylesheet" href="` + cdnVisCss + `">
    <title>Maestro</title>
  </head>
  <body>
    <div id='root'></div>
    <script src="` + cdnReact + `"></script>
    <script src="` + cdnReactDom + `"></script>
    <script src="` + cdnReactRouter + `"></script>
    <script src="` + cdnReactRouterDom + `"></script>
    <script src="` + cdnReactBootstrap + `"></script>
    <script src="` + cdnReactRouterBootstrap + `"></script>
    <script src="` + cdnPropTypes + `"></script>
    <script src="` + cdnCodeMirror + `"></script>
    <script src="` + cdnAxios + `"></script>
    <script src="` + cdnReactBootstrapTable + `"></script>
    <script src="` + cdnVis + `"></script>
    <script src="js/app.js"></script>
  </body>
</html>
`

const indexHTMLBabel = `
<!DOCTYPE HTML>
<html>
  <head>
    <meta charset="utf-8">
    <link rel="stylesheet" type="text/css" href="css/style.css">
    <link rel="stylesheet" href="` + cdnCodeMirrorCss + `">
    <link rel="stylesheet" href="` + cdnBootstrapCss + `">
    <link rel="stylesheet" href="` + cdnReactBootstrapTableCss + `">
    <link rel="stylesheet" href="` + cdnVisCss + `">
    <title>M&aelig;stro</title>
  </head>
  <body>
    <div id='root'></div>
    <script src="js/systemjs/system.js"></script>
    <script>
      SystemJS.config({
        map: {
          'plugin-babel': 'js/systemjs/plugin-babel.js',
          'systemjs-babel-build': 'js/systemjs/systemjs-babel-browser.js',
          'react': '` + cdnReact + `',
          'prop-types': '` + cdnPropTypes + `',
          'react-dom': '` + cdnReactDom + `',
          'react-router': '` + cdnReactRouter + `',
          'codemirror': '` + cdnCodeMirror + `',
          'axios': '` + cdnAxios + `',
          'react-bootstrap': '` + cdnReactBootstrap + `',
          'react-router-dom': '` + cdnReactRouterDom + `',
          'react-router-bootstrap': '` + cdnReactRouterBootstrap + `',
          'react-bootstrap-table': '` + cdnReactBootstrapTable + `',
          'vis': '` + cdnVis + `',
          'create-react-class': 'js/create-react-class.min.js'
        },
        transpiler: 'plugin-babel',
        meta: {
          'js/*.js': { authorization: true },
          'js/*.jsx': {
            authorization: true,
            loader: 'plugin-babel',
            babelOptions: {
              react: true
            }
          }
        }
      });
      System.import('js/app.jsx');
    </script>
  </body>
</html>
`
