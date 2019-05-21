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

// Package daemon is responsible for all aspects of Maestro running as
// a service. The correct start and stop sequence, initiation and take
// down of all components/packages, as well as logging.
package daemon

import (
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"relative/bq"
	"relative/db"
	"relative/gcs"
	"relative/git"
	"relative/gsheets"
	"relative/model"
	"relative/slack"
	"relative/ui"
)

type Config struct {
	ListenSpec      string
	LogPath         string // log location
	LogCycleSeconds int    // logs rotated every, 0 == never
	SecretPath      string

	UI      ui.Config
	Db      db.Config
	BQ      bq.Config
	GCS     gcs.Config
	GSheets gsheets.Config
	Git     git.Config
	Slack   slack.Config
}

func Run(cfg *Config) error {

	setLog(cfg.LogPath, time.Duration(cfg.LogCycleSeconds)*time.Second)

	db, err := db.InitDb(cfg.Db)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return err
	}

	bc, err := db.SelectBQConf()
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	var (
		b  *bq.BigQuery
		gc *gcs.GCS
		gs *gsheets.GSheets
	)

	oa, err := db.SelectOAuthConf()
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}
	if oa == nil {
		log.Printf("Warning: No OAuth config found.")
	} else {
		cfg.UI.GoogleClientID = oa.ClientId
		cfg.UI.GoogleSecret = oa.PlainSecret
		cfg.UI.GoogleRedirectURL = oa.Redirect
		cfg.UI.CookieSecret = oa.PlainCookieSecret
		cfg.UI.AllowedDomain = oa.AllowedDomain
	}
	// Cookie secret defaults to DB secret
	if cfg.UI.CookieSecret == "" {
		cfg.UI.CookieSecret = cfg.Db.Secret
	}

	if bc != nil {
		cfg.BQ.ProjectId = bc.ProjectId
		cfg.BQ.Email = bc.Email
		cfg.BQ.Key = bc.PlainKey
		cfg.BQ.GcsBucket = bc.GcsBucket
		b = bq.NewBigQuery(&cfg.BQ)

		cfg.GCS.ProjectId = bc.ProjectId
		cfg.GCS.Email = bc.Email
		cfg.GCS.Key = bc.PlainKey
		cfg.GCS.Bucket = bc.GcsBucket
		gc = gcs.NewGCS(&cfg.GCS)

		cfg.GSheets.Email = bc.Email
		cfg.GSheets.Key = bc.PlainKey
		cfg.GSheets.Domain = oa.AllowedDomain
		cfg.GSheets.NSheets = 7 // TODO Make me configurable
		gs = gsheets.NewGSheets(&cfg.GSheets)
	}

	gtc, err := db.SelectGitConf()
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}
	if gtc == nil {
		log.Printf("No git config found, git integration disabled.")
	} else {
		cfg.Git.Url = gtc.Url
		cfg.Git.Token = gtc.PlainToken
	}
	var wg sync.WaitGroup
	gt, err := git.NewGit(cfg.Git, &wg)
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	if slk, err := db.SelectSlackConf(); slk != nil {
		cfg.Slack.Url = slk.Url
		cfg.Slack.UserName = slk.UserName
		cfg.Slack.Channel = slk.Channel
		cfg.Slack.IconEmoji = slk.IconEmoji
		cfg.Slack.UrlPrefix = slk.UrlPrefix
	} else if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	log.Printf("Starting, HTTP on: %s\n", cfg.ListenSpec)
	l, err := net.Listen("tcp", cfg.ListenSpec)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return err
	}

	m := model.New(db, cfg.UI.AllowedDomain, b, gc, gs, gt, &cfg.Slack) // It does stuff

	ui.Start(cfg.UI, m, l)

	waitForSignal()

	// Stop any ongoing jobs
	m.Stop()

	if cfg.Git.Url != "" {
		gt.Stop()
		log.Printf("Waiting for pending Git commits to finish...")
		wg.Wait()
		log.Printf("Git commits done.")
	}

	// Close DB connection
	db.Close()

	return nil
}

func waitForSignal() {
	// Wait for a SIGINT or SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	s := <-ch
	log.Printf("Got signal: %v", s)

}
