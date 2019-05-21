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

package daemon

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const logPrefix = "2006-01-02 15:04:05.999999 "

var logWg sync.WaitGroup

func init() {
	log.SetFlags(0) // no prefix, we provide our own
}

var setLog = func(logPath string, cycle time.Duration) *logWriter {
	lw := newLogWriter(logPath, cycle)
	log.SetOutput(lw)
	return lw
}

type logWriter struct {
	mu   sync.Mutex
	file *os.File
}

var newLogWriter = func(path string, cycleDur time.Duration) *logWriter {
	w := &logWriter{}
	if path == "" {
		w.file = os.Stderr
	} else {
		logDir, _ := filepath.Split(path)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			logFatalf("Unable to create log directory: '%s' (%v).", logDir, err)
		}
		log.Printf("Logs will be written to '%s'.", path)
		logFileCycler(w, path, cycleDur)
	}
	return w
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	// Write to our log file, whatever it is
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	return w.file.Write(append(now.AppendFormat(nil, logPrefix), p...))
}

func (w *logWriter) Close() error {
	logWg.Wait()
	return nil
}

func renameLogFile(path string) {
	logDir, logFile := filepath.Split(path)
	filename := time.Now().Format(logFile + "-20060102_150405")
	fullpath := filepath.Join(logDir, filename)
	log.Printf("Starting new log file, current log archived as: '%s'", fullpath)
	os.Rename(path, fullpath)
}

func cycleLogFile(w *logWriter, path string) {
	if w.file != nil {
		renameLogFile(path)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0666) // open with O_SYNC
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Unable to open log file '%s', %s\n", path, err)
		os.Exit(1)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		w.file.Close()
	}
	w.file = file
}

func logFileCycler(w *logWriter, path string, duration time.Duration) {

	cycleLogFile(w, path) // Initial cycle

	cycleLogCh := make(chan int)

	go func() { // Wait for a cycle signal
		for {
			_ = <-cycleLogCh
			cycleLogFile(w, path)
		}
	}()

	if duration > 0 {
		go func() { // Periodic cycling
			for {
				time.Sleep(duration)
				cycleLogCh <- 1
			}
		}()
	}
}

// Write to both stderr and log
func logFatalf(format string, v ...interface{}) {
	// TODO what happens with an ELB if we have a logFatal?
	fmt.Fprintf(os.Stderr, format, v...)
	// if pidPath != "" {
	// 	os.Remove(pidPath)
	// }
	log.Fatalf(format, v...)
}
