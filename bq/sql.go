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

package bq

import "strings"

// Parse a qualified BigQuery table name, such as
// "[orgname:project.table]" (Legacy) or `orgname.project.table`
// (Standard). Returns three strings: project, dataset and
// table. (Blank if not specified).
func ParseTableSpec(spec string) (proj, ds, table string) {
	if strings.HasPrefix(spec, "[") && strings.HasSuffix(spec, "]") { // legacy

		// remove [ ]
		spec = spec[1 : len(spec)-1]

		// extract the project: part
		if parts := strings.SplitN(spec, ":", 2); len(parts) > 1 {
			proj = parts[0]
			spec = parts[1]
		}
		// extract dataset name
		if parts := strings.SplitN(spec, ".", 2); len(parts) > 1 {
			ds = parts[0]
			table = parts[1]
		}
	} else if strings.HasPrefix(spec, "`") && strings.HasSuffix(spec, "`") { // standard

		// remove ``
		spec = spec[1 : len(spec)-1]

		parts := strings.SplitN(spec, ".", 3)

		if len(parts) == 3 { // proj.dataset.table
			proj = parts[0]
			ds = parts[1]
			table = parts[2]
		} else if len(parts) == 2 { // dataset.table
			ds = parts[0]
			table = parts[1]
		} else {
			ds = ""
			table = parts[0]
		}
	}

	return proj, ds, table
}
