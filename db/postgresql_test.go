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

package db

import (
	"testing"
)

func TestConvertTableRecs(t *testing.T) {
	rec1 := &tableRec{
		Id:            1,
		UserId:        1,
		Name:          "Example",
		Query:         "SELECT * FROM",
		Disposition:   "x",
		LegacySQL:     true,
		Description:   "testing",
		Error:         "",
		Running:       false,
		RawConditions: []byte(`[{"weekdays": [1,2], "months": [1], "hours": [13]}]`),
	}

	table, err := tableFromTableRec(rec1)
	if err != nil {
		t.Errorf("Should not be erroring out in conversion from tableRec to table.")
	}

	tableCond := table.Conditions[0]
	if !tableCond.Weekdays[1] || !tableCond.Weekdays[2] || !tableCond.Months[1] || !tableCond.Hours[13] {
		t.Errorf("Table conditions don't have the right values.")
	}
}
