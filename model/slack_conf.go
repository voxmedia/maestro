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

// Configuration necessary for Slack posts
type SlackConf struct {
	Url       string `db:"url"`        // Slack hook URL
	UserName  string `db:"username"`   // Username that will show in slack, e.g. "maestro"
	Channel   string `db:"channel"`    // The channel to which to post
	IconEmoji string `db:"iconemoji"`  // An emoji that will show next to user name, e.g. ":violin:"
	UrlPrefix string `db:"url_prefix"` // URL to this instance of Maestro (to make messages clickable)
}
