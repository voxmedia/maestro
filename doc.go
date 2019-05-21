/* Copyright 2019 Vox Media, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Maestro is a SQL-centric tool for orchestrating BigQuery jobs. Maestro
also supports data transfers from and to Google Cloud Storage (GCS)
and relational databases (presently PotgresSQL and MySQL).

Maestro is a "catalog" of SQL statements. Key feature of Maestro is
the ability to infer dependencies by examining the SQL and without any
additional configuration. Maestro can execute all tasks in correct
order without a manually specified order (i.e. a "DAG"). Execution can
be associated with a frequency (cadence) without requiring any cron or
cron-like configuration.

Maestro is an ever-running service (daemon). It uses PostgreSQL to
store the SQL and all other configuration, state and history. The
daemon takes great care to maintain all of its state in PostgreSQL so
that it can be stopped or restarted without interrupting any
in-progress jobs (in most cases).

Maestro records all BigQuery job and other history and has a notion of
users and groups which is useful for attributing costs and resource
utilization to users and groups.

Maestro has a basic web-based user interface implemented in React,
though its API can also be used directly. Maestro can notify arbitrary
applications of job completion via a simple HTTP request. Maestro also
provides a Python client library for a more native Python experience.

Maestro integrates with Google OAuth for authentication, Google Sheets
for simple exports, Github (for SQL revision control) and Slack (for
alerts and notifications).

Introduction

Maestro was designed with simplicity as one of its primary goals. It
trades flexibility usually afforded by configurability in various
languages for the transaprency and clarity achievable by leveraging
the declarative nature of SQL.

Maestro works best for environments where BigQuery is the primary
store of all data for analyitcal purposes. E.g. the data may be
periodically imported into BigQuery from various databases. Once
imported, data may be subsequently summarized or transformed via a
sequence of BigQuery jobs. The summarized data can then be exported to
external databases/application for additional processing (e.g. SciPy)
and possibly be imported back into BigQiery, and so on. Every step of
this process can be orchestrated by Maestro without relying on any
external scheduling facility such as cron.

Key Concepts

Below is the listing of all the key conepts with explanations.

Tables

A table is the central object in Maestro. It always corresponds to a
table in BigQuery. Maestro code and documentation use the verb "run"
with respect to tables. To "run a table" means to perform whatever
action is called for in its configuration and store the result in a
BigQuery table.

A table is (in most cases) defined by a BigQuery SQL statement. There
are three kinds of tables in Maestro.

Summary Table

A summary table is produced by executing a BigQuery SQL statement (a
Query job).

Import Table

An import table is produced by executing SQL on an external database
and importing the result into BigQuery. The SQL statement in this case
it intentionally restricted to a primitive which supports only SELECT,
FROM, WHERE and LIMIT. This is so as to discourage the users from
running a complex and taxing query on the database server. The main
reason for this SQL statement is to filter out or transform columns,
any other processing is best done subsequently in BigQuery.

External Table

This is a table whose data comes from GCS. The import is triggered via
the Maestro API. Such tables are generally used when BigQuery data
needs to be processed by an external tool, e.g. SciPy, etc.

Jobs

A job is a BigQuery job. BigQquery has three types of jobs: query,
extract and load. All three types are used in Maestro. These details
are internal but should be familiar to developers.

A BigQquery query job is executed as part of running a table.

A BigQuery extract job is executed as part of running a table, after
the query job is complete. It results in one or more extract files in
GCS. Maestro provides signed URLs to the GCS files so that external
tools require no authentication to access the data. This is also
facilitated via the Maestro pythonlib.

A BigQuery load job is executed as part of running an import table. It
is the last step of the import, after the external database table data
has been copied to GCS.

Runs

A run is a complex process which happens periodically, according to a
frequency. For example if a daily frequency is defined, then Maestro
will construct a run once per day, selecting all tables (including
import tables) assigned to this frequency, computing the dependency
graph and creating jobs for each table. The jobs are then executed in
correct order based on the position in the graph and the number of
workers available. Maestro will also assign priority based on the
number of child dependencies a table has, thus running the most
"important" tables first.

Getting Started

PostgreSQL 9.6 or later is required to run Maestro. Building a
"production" binary, i.e. with all assets included in the binary
itself requires Webpack. Webpack is not necessary for "development"
mode which uses Babel for transpilation.

Download and compile Maestro with "go get
github.com/voxmedia/maestro".  (Note that this will create a
$GOPATH/bin/maestro binary, which is not very useful, you can delete
it). From here cd $GOPATH/src/github.com/voxmedia/maestro and go
build. You should now have a "maestro" binary in this directory.

You can also create a "production" binary by running "make
build". This will combine all the javascript code into a single file
and pack it and all other assets into the maestro binary itself, so
that to deploy you only need the binary and no other files.

Create a PostgreSQL database named "maestro". If you name it something
other than that, you will need to provide that name to Maestro via the
-db-connect flag which defaults to "host=/var/run/postgresql
dbname=maestro sslmode=disable", which should work on most Linux
distros. On MacOS the Postgres socket is likely to be in
"/private/tmp" and one way to address this is to run "ln -s
/private/tmp /var/run/postgresql"


Maestro connects to many services and needs credentials for all of
them. These credentials are stored in the database, all encrypted
using the same shared secret which must be specified on the command
line via the -secret argument. The -secret argument is meant mostly
for development, in production it is much more secure to use the
-secretpath option pointing to the location of a file containing the
secret.

From the Google Cloud perspective, it is best to create a project
entirely dedicated to Maestro, with BigQuery and GCS API's enabled,
then create a Service Account (in IAM) dedicated to Maestro, as well
as OAuth credentials. The service account will need BigQuery Editor,
Job User and Storage Object Admin roles.

Run Maestro like so: ./maestro -secret=whatever where "whatever" is the
shared secret you invent and need to remember.

You should now be able to visit the Maestro UI, by default it is at http://localhost:3000

When you click on the log-in link, since at this point Maestro has no
OAuth configuration, you will be presented with a form asking for the
relevant info, which you will need to provide. You should then be
redirected to the Google OAuth login page. From here on the
configuration is stored in the database in encrypted form.

As the first user of this Maestro instance, you are automatically
marked as "admin", which means you can perform any action. As an
admin, you should see the "Admin" menu in the upper right. Click on it
and select the "Credentials" option.

You now need to populate the credentials. The BigQuery, default
dataset and GCS bucket are required, while Git and Slack are optional,
but highly recommended.

Note that the BigQuery dataset and the GCS bucket are not created by
Maestro, you need to create those manually. The GCS bucket is used for
data exports, and it is generally a good idea to set the data in it to
expire after several days or whatever works for you.

If you need to import data from external databases, you can add those
credentials under the Admin / Databases menu.

You may want to create a frequency (also under Admin menu). A
frequency is how periodic jobs are scheduled in Maestro. It is defined
by a period and an offset. The period is passed to time.Truncate()
function, and if the result is 0, this is when a run is triggered. The
offset is an offset into the period. E.g. to define a frequency that
start a run at 4am UTC, you need to specify a period of 86400 and an
offset of 14400.

Note that Maestro needs to be restarted after these configuration
changes (this will be fixed later).

At this point you should be able to create a summary table with some
simple SQL, e.g. "SELECT 'hello' AS world", save it and run it. If it
executes correctly, you should be able to see this new table in the
BigQuery UI.

*/
package main
