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

# This is Python 2 and 3 compatible

"""Maestro Python client.

This module provides a few conveniences for interoperating with
Maestro table execution, such as waiting for table completion or start
of execution of an external table, as well as seamless download and
upload of GCS files for BigQuery export or load operations.

Under the hood it uses the Maestro API. The API authenticates clients
using a Maestro access token which can be obtained from the Maestro UI.

Installation: simply copy this file into your site-packages directory.

"""

import os
import sys
from dateutil.parser import parse as timeparse
import time
from gzip import GzipFile


import requests # $ pip install requests

try:
    from urllib.parse import urlparse, urljoin
except ImportError:
    from urlparse import urlparse, urljoin

def _sleep_time(max_sleep=60, step=1.0):
    """ Decaying time intervals. Each subsequent call will return a higher value up to max_sleep. """
    import random
    n = 1.0
    while True:
        yield n+random.random()
        if n < max_sleep:
            n += step

class Table(object):
    """Access to a Maestro table. This object should be created using the Maestro.Table() method, see its documentation for details."""

    def __init__(self, url, token, table_id, wait=False, max_sleep=60, gcs_fetch_path=None, cleanup=False):
        """ See Maestro.Table() method for description. """

        self._url = url
        self._token = token

        if not isinstance(table_id, int):
            table_id = self._id_by_name(table_id)
            if not table_id:
                raise Exception("Table not found.")

        self._table_id = table_id
        self._wait = wait
        self._max_sleep = max_sleep
        self._gcs_fetch_path = gcs_fetch_path
        self._files = []
        self._cleanup = cleanup

        self._update_status_full()
        self._update_bq_info()
        self._prev_ok_run = self._last_ok_run

    def _id_by_name(self, name):
        headers = { "X-Api-Token" : self._token }
        url = urljoin(self._url, "table/%s/id" % name)
        r = requests.get(url, headers=headers)
        return r.json()["Id"]

    def _update_status_full(self):
        headers = { "X-Api-Token" : self._token }
        url = urljoin(self._url, "table/%d" % self._table_id)
        r = requests.get(url, headers=headers)

        self._status = r.json()
        self._name = self._status['Name']
        self._dataset = self._status['Dataset']
        self._last_ok_run = timeparse(self._status["LastOkRunEndAt"])

        return self._status

    def _update_status_short(self):
        headers = { "X-Api-Token" : self._token }
        url = urljoin(self._url, "table/%d/status" % self._table_id)
        r = requests.get(url, headers=headers)

        status = r.json()
        if status["Error"]:
            raise Exception(status["Error"])
        self._last_ok_run = timeparse(status["LastOkRunEndAt"])
        self._status["Running"] = status["Status"] == "running"

        return self._status

    def _update_bq_info(self):
        headers = { "X-Api-Token" : self._token }
        url = urljoin(self._url, "table/%d/bq_info" % self._table_id)
        r = requests.get(url, headers=headers)
        self._bq_info = r.json()
        return self._bq_info

    def _is_external(self):
        """Returns True if this table is external."""
        return self._status["ExternalTmout"] > 0

    def _wait_for_external(self):
        st = _sleep_time(self._max_sleep)
        while not self._status["Running"]:
            time.sleep(next(st))
            self._update_status_short()
        self._update_status_full()

    def _wait_for_internal(self):
        st = _sleep_time(self._max_sleep)
        while self._last_ok_run <= self._prev_ok_run:
            time.sleep(next(st))
            self._update_status_short()
        self._update_status_full()

    def _wait_for_table(self):
        if self._is_external():
            print("Waiting for external table %s.%s (%d) to start running..." % (self._dataset, self._name, self._table_id))
            self._wait_for_external()
        else:
            print("Waiting for table %s.%s (%d) LastOkRunEndAt to change..." % (self._dataset, self._name, self._table_id))
            self._wait_for_internal()


    def _fetch_file(self, url, destdir):
        chunk_sz = 1024*8
        filename = os.path.basename(urlparse(url).path)
        destpath = os.path.join(destdir, filename)
        print("Fetching %s into %s..." % (filename, destdir))
        tot, start = 0, time.time()
        response = requests.get(url)
        with open(destpath, 'wb') as out:
            for chunk in response.iter_content(chunk_size=chunk_sz):
                out.write(chunk)
                tot += len(chunk)
        dur = time.time() - start
        print("Transferred %d bytes in %02fs (%02f B/s)." % (tot, dur, tot/dur))
        self._files.append(destpath)
        return destpath


    def reader(self, chunk_sz=1024*64):
        """Read data directly from GCS.

        Returns an object with a readline() method, which can be used
        to iterate over all GCS files in sequence. Only the first
        file's CSV header will be present, all others will be skipped.
        """

        if sys.version_info[0] < 3:
            raise Exception("Table.line_reader() only supported on Python 3")

        class _url_reader(object):
            def __init__(self, url, chunk_sz=chunk_sz):
                self._url = url
                self._chunk_sz = chunk_sz
                self._reader = self._iterator()
                self._buf = b""

            def _iterator(self):
                response = requests.get(self._url)
                for chunk in response.iter_content(chunk_size=self._chunk_sz):
                    yield chunk

            def _read_more(self):
                self._buf += next(self._reader, b"")

            def read(self, size=-1):
                if not self._buf:
                    self._read_more()
                result = b""
                if len(self._buf) <= size or size == -1:
                    result += self._buf
                    self._buf = b""
                else:
                    result = self._buf[:size]
                    self._buf = self._buf[size:]
                return result

        class _multiurl_reader(object):
            def __init__(self, urls, chunk_sz=1024*4):
                self._chunk_sz = chunk_sz
                self._gzs = self._iterator(urls)
                self._gz = next(self._gzs, None)
                self._buf = b""
                self._start, self._rows, self._bytes = time.time(), -1, 0 # -header

            def read(self, size=-1):
                if not self._buf:
                    self._buf += self.readline()
                result = b""
                if len(self._buf) <= size or size == -1:
                    result += self._buf
                    self._buf = b""
                else:
                    result = self._buf[:size]
                    self._buf = self._buf[size:]
                return result

            def readline(self):
                s = self._gz.readline()
                if not s:
                    self._gz = next(self._gzs, None)
                    if not self._gz:
                        dur = time.time() - self._start
                        rps = self._rows / dur
                        bps = self._bytes / dur
                        print("Transferred %d rows (%d bytes) in %02fs (%02f row/s, %02f B/s)." % (self._rows, self._bytes, dur, rps, bps))
                        return b""
                    else:
                        self._gz.readline() # This is a CSV header, we ignore it
                        s = self._gz.readline()
                self._rows += 1
                self._bytes += len(s)
                return s

            # iterate over urls
            def _iterator(self, urls):
                for url in urls:
                    rr = _url_reader(url, self._chunk_sz)
                    yield GzipFile(fileobj=rr)

            # line iterator protocol
            def __iter__(self):
                return self

            def __next__(self):
                line = self.readline()
                if not line:
                    raise StopIteration()
                return str(line, 'utf8')

        if not self._status["Extracts"]["URLs"]:
            raise Exception("No GCS extracts found for this table.")
        return  _multiurl_reader(self._status["Extracts"]["URLs"], chunk_sz=chunk_sz)


    def gcs_fetch(self, dest):
        """Fetch GCS exports.

        This method requests a signed URL from Maestro and proceeds to
        download the GCS exports into the dest directory. The dest
        directory must exist. No GCS credentials are required for this
        operation.

        """
        if not self._status["Extract"]:
            raise Exception("GCS extract not enabled for this table.")
        self._files = []
        for url in self._status["Extracts"]["URLs"]:
            self._fetch_file(url, dest)
        return self._files

    def _load_external(self, filename):
        if not self._is_external():
            raise Exception("Cannot upload: not an external table.")
        print("Starting BigQuery load job for %s..." % self._name)
        url = urljoin(self._url, "table/%d/load_external" % self._table_id)
        data = { "fn" : filename }
        headers = { "X-Api-Token" : self._token }
        requests.post(url, headers=headers, data=data)
        self._wait_for_internal() # correct
        if self._status["Error"]:
            print("BigQuery load job finished with error: " + self._status["Error"])
        else:
            print("BigQuery load job finished OK.")

    def gcs_upload(self, source):
        """Upload external table data.

        For external tables, upload the data. Supported formats are
        CSV and newline delimited JSON, i.e. one line per row. On
        table creation the schema is automatically inferred by
        BigQuery.

        """
        if not self._is_external():
            raise Exception("Cannot upload: not an external table.")
        tot = os.stat(source).st_size
        start = time.time()
        data = open(source, 'rb')
        dest = self._status["UploadURL"]
        without_args = dest.split('?')[0]
        filename = without_args[without_args.rfind("/")+1:]
        print("Uploading %s into %s..." % (source, without_args))
        requests.put(dest, data, headers={'Content-Type': 'application/octet-stream'})
        dur = time.time() - start
        print("Transferred %d bytes in %02fs (%02f B/s)." % (tot, dur, tot/dur))
        self._load_external(filename)
        self._files.append(source)

    def files(self):
        """Return a list of downloaded file paths."""
        return self._files

    def schema(self):
        """Return the BQ schema of the table."""
        return self._bq_info['schema']['fields']

    def __exit__(self, exc_type, exc_value, tb):
        """Called on exit of a with block. Optionally deletes downloaded files."""
        if self._cleanup:
            for path in self._files:
                print("Cleanup: deleting", path)
                os.unlink(path)

    def __enter__(self):
        """Called on entrance of a with block. Optionally waits for table status."""

        if self._wait:
            self._wait_for_table()

        if self._gcs_fetch_path:
            self.gcs_fetch(self._gcs_fetch_path)

        return self

    def pg_create_table(self, name, if_not_exists=False):
        """Return a PostgreSQL CREATE TABLE statement for this table."""

        # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
        # we do not support RECORD or STRUCT
        type_map = {"STRING": "TEXT",
                    "BYTES":  "BYTEA",
                    "INTEGER":"BIGINT",
                    "INT64":  "BIGINT",
                    "FLOAT":  "DOUBLE PRECISION",
                    "FLOAT64":"DOUBLE PRECISION",
                    "BOOLEAN":"BOOLEAN",
                    "BOOL":   "BOOLEAN",
                    "TIMESTAMP":"TIMESTAMP WITH TIME ZONE",
                    "DATE"    :"DATE",
                    "TIME"    :"TIME",
                    "DATETIME":"TIMESTAMP"
        }
        fields = self.schema()
        ifne = if_not_exists and "IF NOT EXISTS " or ""
        result = "CREATE TABLE %s %s(\n" % (name, ifne)
        columns = []
        for field in fields:
            column = "    %s %s" % (field["name"], type_map[field["type"]])
            # REPEATED is not supported
            if field["mode"] == "REQUIRED":
                column += " NOT NULL"
            columns.append(column)
        result += ",\n".join(columns)
        return result + ");"


class Maestro(object):
    """
    Access to a Maestro instance.
    """

    def __init__(self, url, token):
        """
        Constructor for a Maestro instance.

        Attributes:
            url (string): The root URL of the instance, e.g. "https://maestro.example.com:8888/".
            token (string): The Maestro API access token.

        """
        self._url = url
        self._token = token

    def Table(self, table_id, wait=False, max_sleep=60, gcs_fetch_path=None, cleanup=False):
        """Create a Maestro Table object.

        The returning object is compatible with the "with" protocol,
        i.e. it can be used as part of a with block:

            with m.Table(123) as t:
                # do something
                pass

        Parameters:
            table_id (int or string): Integer id of the table or a string name in
                dataset.table format.

            wait (bool): For external tables, this waits for beginning
                of execution (only happens during scheduled runs). For
                all other tables wait for next successful completion.

            max_sleep (int): When waiting, table status is obtained by
                polling Maestro periodically.  The poll interval
                gradually increases up to max_sleep (in seconds).

            gcs_fetch_path (string): Directory for GCS files. For
                summary tables with a GCS export, proceed to download
                the exported file(s) into this directory. The
                directory must exist prior and is not created or
                cleaned in any way.  Alternatively, the same can be
                accomplished more explicitely using the gcs_fetch()
                method.

            cleanup (bool): Delete downloaded files when exiting the with block.
        """
        return Table(self._url, self._token, table_id, wait=wait, max_sleep=max_sleep, gcs_fetch_path=gcs_fetch_path, cleanup=cleanup)
