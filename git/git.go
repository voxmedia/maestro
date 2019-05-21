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

// Package git provides the functionality for interacting with git/GitHub.
package git

import (
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Config struct {
	Url   string // https://github.com/user/repo
	Token string // Github token
}

type Git struct {
	url       *url.URL
	tokenUrl  *url.URL
	ch        chan *commitRequest
	clonepath string
}

type commitRequest struct {
	relpath string
	content []byte
	comment string
	name    string
	email   string
	when    time.Time
}

// Creates a new Git instance. Also starts a worker goroutine which
// will process commits in the background. To properly exit, call
// Stop(). The passed in WaitGroup is so that we can wait for all
// pending check ins to complete on exit.
func NewGit(cfg Config, wg *sync.WaitGroup) (*Git, error) {
	g := &Git{
		ch: make(chan *commitRequest, 4),
	}
	if cfg.Url != "" {

		// insert the auth token into the url
		u, err := url.Parse(cfg.Url)
		if err != nil {
			return nil, err
		}
		u.User = url.UserPassword("git", cfg.Token)
		g.tokenUrl = u

		g.url, _ = url.Parse(cfg.Url) // without token

		log.Printf("Starting Git commit worker.")
		go g.gitWorker(wg)
	}
	return g, nil
}

// Queue up a commit, which will be pushed to GitHub in a separate
// goroutine. Relpath is the part of path inside the repo,
// e.g. "tables/1/10.yaml", content is the content of this file;
// comment, name, email and when are attributes of the commit.
func (g *Git) QueueCommit(relpath string, content []byte, comment, name, email string, when time.Time) {
	if g.url != nil {
		g.ch <- &commitRequest{
			relpath: relpath,
			content: content,
			comment: comment,
			name:    name,
			email:   email,
			when:    when,
		}
	}
}

// Closes the queue channel thereby informing the git worker goroutine
// to exit.
func (g *Git) Stop() {
	close(g.ch)
}

// Return the URL to the repository.
func (g *Git) Url() url.URL {
	if g.url != nil {
		return *g.url
	}
	u, _ := url.Parse("http://example.com/git/not/available")
	return *u
}

func (g *Git) cloneRepo() (*git.Repository, error) {
	if _, err := os.Stat(os.TempDir()); os.IsNotExist(err) {
		os.Mkdir(os.TempDir(), 0777)
	}
	if g.clonepath == "" {
		td, err := ioutil.TempDir("", "repo")
		if err != nil {
			return nil, err
		}
		g.clonepath = td

		// initial clone
		return git.PlainClone(g.clonepath, false, &git.CloneOptions{
			URL: g.tokenUrl.String(),
		})
	}
	return git.PlainOpen(g.clonepath)
}

func (g *Git) gitWorker(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for cr := range g.ch {

		if g.url == nil {
			continue // Git disabled
		}

		repo, err := g.cloneRepo()
		if err != nil {
			log.Printf("Error cloning git repo: %v", err)
			continue
		}

		wt, err := repo.Worktree()
		if err != nil {
			log.Printf("Error creating git worktree: %v", err)
			continue
		}

		err = wt.Pull(&git.PullOptions{RemoteName: "origin"})
		if err != nil {
			if !strings.Contains(err.Error(), "already up-to-date") {
				log.Printf("Error in git pull: %v", err)
				continue
			}
		}

		if dir, _ := filepath.Split(cr.relpath); dir != "" {
			err = os.MkdirAll(filepath.Join(g.clonepath, dir), 0755)
			if err != nil {
				log.Printf("Error making directory %q in repo: %v", dir, err)
				continue
			}
		}

		f, err := os.Create(filepath.Join(g.clonepath, cr.relpath))
		if err != nil {
			log.Printf("Error creating file %q in worktree: %v", cr.relpath, err)
			continue
		}
		f.Write(cr.content)
		f.Close()

		_, err = wt.Add(cr.relpath)
		if err != nil {
			log.Printf("Error adding git file: %v", err)
			continue
		}

		commit, err := wt.Commit(cr.comment, &git.CommitOptions{
			Author: &object.Signature{
				Name:  cr.name,
				Email: cr.email,
				When:  cr.when,
			},
		})
		if err != nil {
			log.Printf("Error creating commit: %v", err)
			continue
		}

		_, err = repo.CommitObject(commit)
		if err != nil {
			log.Printf("Error creating commit object: %v", err)
			continue
		}

		err = repo.Push(&git.PushOptions{})
		if err != nil {
			log.Printf("Error pushing to git: %v", err)
			continue
		}
		log.Printf("Commited git revision %s OK.", commit)
	}
}
