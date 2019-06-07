// +build !builtinassets

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

// Fake assetfs symbol satisfaction. Does not build when the
// builtin-assets tag is specified.

package main

import (
	"fmt"
	"log"
	"os"
)

func Asset(name string) ([]byte, error) {
	log.Printf("Stub Asset() called, this is not supposed to happen!")
	return nil, fmt.Errorf("Asset %s not found", name)
}

func AssetInfo(name string) (os.FileInfo, error) {
	log.Printf("Stub AssetInfo() called, this is not supposed to happen!")
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

func AssetDir(name string) ([]string, error) {
	log.Printf("Stub AssetDir() called, this is not supposed to happen!")
	return nil, fmt.Errorf("Asset %s not found", name)
}
