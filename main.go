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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"relative/daemon"

	"github.com/elazarl/go-bindata-assetfs"
)

var (
	builtinAssets string // -ldflags -X only works with strings
	assetsPath    string
)

func processFlags() *daemon.Config {
	cfg := &daemon.Config{}

	dftListenSpec := "localhost:3000"
	flag.StringVar(&cfg.ListenSpec, "listen", dftListenSpec, "HTTP listen spec")

	flag.StringVar(&cfg.LogPath, "logpath", "", "Log path, empty = stderr")
	flag.IntVar(&cfg.LogCycleSeconds, "logcycle", 0, "Cycle log file interval in seconds, 0 == never")

	flag.StringVar(&cfg.SecretPath, "secretpath", "", "Path to the file containing the secret to decrypt credentials in the database")
	flag.StringVar(&cfg.Db.Secret, "secret", "", "Secret to decrypt credentials in the database (not safe, please use -secretpath)")

	flag.StringVar(&cfg.Db.ConnectString, "db-connect", "host=/var/run/postgresql dbname=maestro sslmode=disable", "DB Connect String")

	flag.BoolVar(&cfg.UI.ForceSSL, "force-ssl", false, "Check X-Forwarded-Proto header and redirect.")

	if builtinAssets == "" {
		flag.StringVar(&assetsPath, "assets-path", "assets", "Path to assets dir")
	}

	flag.Parse()

	return cfg
}

func validateConfig(cfg *daemon.Config) error {

	if cfg.SecretPath == "" && cfg.Db.Secret == "" {
		return fmt.Errorf("-secretpath (or -secret) option is required")
	}

	if cfg.SecretPath != "" {
		secret, err := ioutil.ReadFile(cfg.SecretPath)
		if err != nil {
			return err
		}

		cfg.Db.Secret = strings.TrimSpace(string(secret))

	}
	return nil
}

// When we are baking in assets, builtinAssets is not blank (set via
// ldflags, see Makefile). Otherwise they are read from assetpath.
func setupHttpAssets(cfg *daemon.Config) {
	if builtinAssets != "" {
		log.Printf("Running with builtin assets.")
		cfg.UI.Assets = &assetfs.AssetFS{Asset: Asset, AssetDir: AssetDir, AssetInfo: AssetInfo, Prefix: builtinAssets}
	} else {
		log.Printf("Assets served from %q.", assetsPath)
		cfg.UI.Assets = http.Dir(assetsPath)
		cfg.UI.Babel = true
	}
}

func main() {
	cfg := processFlags()
	if err := validateConfig(cfg); err != nil {
		log.Printf("Configuration error: %v", err)
		return
	}

	setupHttpAssets(cfg)

	if err := daemon.Run(cfg); err != nil {
		log.Printf("Error in main(): %v", err)
	}
}
