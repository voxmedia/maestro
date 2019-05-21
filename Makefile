# Copyright 2019 Vox Media, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ASSETS_SRC = "assets"
ASSETS_GEN = "assets_gen"
PYTHON_SRC = "pythonlib"
WEBPACK_ENTRY = "app=${ASSETS_SRC}/js/app.jsx"
BIN_NAME = "maestro"

.PHONY: all help

all: help

help:
	@echo "Valid commands:"
	@echo "  make build: Compile with assets baked in"
	@echo "  make dev: Same as go build"
	@echo "  make test: Run tests"

build: compile_js
	@export GOPATH=$${GOPATH-~/go} && \
	go get github.com/jteeuwen/go-bindata/... github.com/elazarl/go-bindata-assetfs/... && \
	$$GOPATH/bin/go-bindata -o bindata.go -tags builtinassets ${ASSETS_GEN}/... && \
	go build -o ${BIN_NAME} -tags builtinassets -ldflags "-X main.builtinAssets=${ASSETS_GEN}"

dev:
	@go build

compile_js: node_modules/.dirstamp
	@rm -rf ${ASSETS_GEN} && mkdir -p ${ASSETS_GEN}/css ${ASSETS_GEN}/images ${ASSETS_GEN}/login ${ASSETS_GEN}/py && \
	cp -r ${ASSETS_SRC}/css/*.css ${ASSETS_GEN}/css && \
	cp -r ${ASSETS_SRC}/images/* ${ASSETS_GEN}/images && \
	cp -r ${ASSETS_SRC}/login/* ${ASSETS_GEN}/login && \
	cp -r ${PYTHON_SRC}/*.py ${ASSETS_GEN}/py && \
	./node_modules/.bin/webpack ${WEBPACK_ENTRY} --config webpack.config.js --optimize-minimize --output-path ${ASSETS_GEN}/js

compile_js_babel: node_modules/.dirstamp
	@rm -rf ${ASSETS_GEN} && cp -r ${ASSETS_SRC} ${ASSETS_GEN} && \
	./node_modules/.bin/babel --presets es2015,react ${ASSETS_SRC} --out-dir ${ASSETS_GEN}

node_modules/.dirstamp:
	@npm install && touch $@

test:
	@go test ./...

secrets:
	@./.git-secrets --install && \
	git config --local include.path '../.gitconfig'
