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
	"bufio"
	"regexp"
	"sort"
	"strings"
)

var (
	cCommentRe         = regexp.MustCompile(`/\*[^*]*\*+(?:[^*/][^*]*\*+)*/`)
	wholeLineCommentRe = regexp.MustCompile(`^\s*(--|#)`)
	trailingCommentRe  = regexp.MustCompile(`--|#`)
	tokenRe            = regexp.MustCompile(`[\s)(;,]+`)
)

func removeSQLComments(stmt string) string {
	stmt = cCommentRe.ReplaceAllLiteralString(stmt, "")

	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(stmt))
	for scanner.Scan() {
		line := scanner.Text()
		if !wholeLineCommentRe.MatchString(line) {
			lines = append(lines, trailingCommentRe.Split(line, -1)[0])
		}
	}
	return strings.Join(lines, " ")
}

// Given an SQL query, return a (more or less accurate) list of
// every table name mentioned in it.
//
// It is okay for it to be inaccurate, we only need a function that
// errs by providing us false-positives. We can ignore all the tables
// whose names are not in our database. It cannot tell what
// table_date_range() and similar dynamic table selection functions
// will return, only BigQuery knows it.
func tablesInQuery(query string) []string {

	query = removeSQLComments(query)

	set := make(map[string]bool)
	getNext := false
	for _, tok := range tokenRe.Split(query, -1) {
		ltok := strings.ToLower(tok)
		if getNext {
			if ltok != "" && ltok != "select" && ltok != "table_date_range" {
				set[tok] = true
			}
			getNext = false
		}
		getNext = ltok == "from" || ltok == "join" || ltok == "table_date_range"
	}

	result := make([]string, 0, len(set))
	for name, _ := range set {
		result = append(result, name)
	}

	sort.Strings(result)
	return result
}

// type PrimitiveSelect struct {
// 	Select string
// 	From   string
// 	Where  string
// 	Limit  string
// }

// func (s *PrimitiveSelect) String() string {
// 	cols := s.Select
// 	if cols == "" {
// 		cols = "*"
// 	}
// 	stmt := fmt.Sprintf("SELECT %s \n  FROM %s\n", cols, s.From)
// 	if s.Where != "" {
// 		stmt = fmt.Sprintf("%s WHERE %s\n", stmt, s.Where)
// 	}
// 	if s.Limit != "" {
// 		stmt = fmt.Sprintf("%s LIMIT %s", stmt, s.Limit)
// 	}
// 	return stmt
// }
