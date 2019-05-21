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

// Package GCS contains the code to interface with Google Cloud
// Storage.
package gcs

import (
	"context"
	"fmt"
	"io"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/storage/v1"
)

type Config struct {
	ProjectId string // Project Id
	Email     string // Email (authentication)
	Key       string // Key (authentication)
	Bucket    string // GCS bucket
}

type GCS struct {
	projectId string
	email     string
	key       []byte
	bucket    string
}

// Create a GCS client.
func NewGCS(cfg *Config) *GCS {
	return &GCS{
		projectId: cfg.ProjectId,
		email:     cfg.Email,
		key:       []byte(cfg.Key),
		bucket:    cfg.Bucket,
	}
}

func (g *GCS) newService() (*storage.Service, error) {

	cfg := &jwt.Config{
		Email:      g.email,
		PrivateKey: g.key,
		Scopes:     append([]string{}, storage.DevstorageFullControlScope), // copy
		TokenURL:   google.JWTTokenURL,
	}
	ctx := context.Background()
	return storage.New(oauth2.NewClient(ctx, cfg.TokenSource(ctx)))
}

// Create a GCS file. Media is the Reader from which the data will be
// read and then sent to GCS.
func (g *GCS) Insert(ctx context.Context, name string, media io.Reader) (*storage.Object, error) {
	gs, err := g.newService()
	if err != nil {
		return nil, err
	}
	object := &storage.Object{Name: name}
	return gs.Objects.Insert(g.bucket, object).Media(media).Context(ctx).Do()
}

// Given a file name, return a fully qualified gs:// URL
func (g *GCS) UrlForName(name string) string {
	return fmt.Sprintf("gs://%s/%s", g.bucket, name)
}

func (g *GCS) deleteFile(name string) error {
	gs, err := g.newService()
	if err != nil {
		return err
	}
	return gs.Objects.Delete(g.bucket, name).Do()
}

// Delete files from GCS.
func (g *GCS) DeleteFiles(uris []string) error {
	for _, uri := range uris {
		bucket, name, err := ParseGcsUri(uri)
		if err != nil {
			return err
		}
		if bucket != g.bucket {
			return fmt.Errorf("Wrong bucket: %v != %v", bucket, g.bucket)
		}
		if err := g.deleteFile(name); err != nil {
			return err
		}
	}
	return nil
}

// Get a ReaderCloser of a GCS file.
func (g *GCS) GetReader(name string) (io.ReadCloser, error) {
	gs, err := g.newService()
	if err != nil {
		return nil, err
	}

	res, err := gs.Objects.Get(g.bucket, name).Download()
	return res.Body, nil
}

// ParseGcsUri parses a "gs://" URI into a bucket, name pair.
// Inspired by:
// https://github.com/GoogleCloudPlatform/gifinator/blob/master/internal/gcsref/gcsref.go#L37
func ParseGcsUri(uri string) (bucket, name string, err error) {
	const prefix = "gs://"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", fmt.Errorf("parse GCS URI %q: scheme is not %q", uri, prefix)
	}
	uri = uri[len(prefix):]
	i := strings.IndexByte(uri, '/')
	if i == -1 {
		return "", "", fmt.Errorf("parse GCS URI %q: no object name", uri)
	}
	bucket, name = uri[:i], uri[i+1:]
	return bucket, name, nil
}
