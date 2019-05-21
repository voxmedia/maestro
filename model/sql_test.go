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

package model

import (
	"reflect"
	"testing"
)

func Test_removeSQLComments(t *testing.T) {

	stmt1 := "SELECT *  /* a comment */\n FROM foo /* another comment */\nWHERE blah\n--whole line comment\nAND foo --trailing\n"
	stmt2 := "SELECT *    FROM foo  WHERE blah AND foo "

	result := removeSQLComments(stmt1)
	if result != stmt2 {
		t.Errorf("Some comments not removed: %q != %q", result, stmt2)
	}
}

func Test_tablesInQuery(t *testing.T) {

	stmt1 := "SELECT *  /* a comment */\n FROM [BQ_style_table] JOIN blah ON bleh /* another comment */\nWHERE blah\n--whole line comment\nAND foo --trailing\n"
	expect := []string{"[BQ_style_table]", "blah"}

	tables := tablesInQuery(stmt1)
	if !reflect.DeepEqual(expect, tables) {
		t.Errorf("tables: %#v expect: %#v", tables, expect)
	}

}

func Test_tablesInQueryWithDataset(t *testing.T) {

	stmt1 := "SELECT *  /* a comment */\n " +
		"FROM [example.com:project-name-123:dataset_123.table_20170731] " +
		"JOIN blah ON bleh /* another comment */\nWHERE blah\n--whole line comment\nAND foo --trailing\n"
	expect := []string{"[example.com:project-name-123:dataset_123.table_20170731]", "blah"}

	tables := tablesInQuery(stmt1)
	if !reflect.DeepEqual(expect, tables) {
		t.Errorf("tables: %#v expect: %#v", tables, expect)
	}

}
