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

// Package slack contains code to interface with the Slack API.
package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type Config struct {
	Url       string // Slack web hook URL
	UserName  string // Can be anything
	Channel   string // The channel we are posting to
	IconEmoji string // Emoji, e.g. ":violin:"
	UrlPrefix string // The URL to the Maestro instance itself for clickable links
}

// See https://api.slack.com/docs/message-attachments#attachment_structure
type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

type Attachment struct {
	Fallback   *string   `json:"fallback"`
	Color      *string   `json:"color"`
	PreText    *string   `json:"pretext"`
	AuthorName *string   `json:"author_name"`
	AuthorLink *string   `json:"author_link"`
	AuthorIcon *string   `json:"author_icon"`
	Title      *string   `json:"title"`
	TitleLink  *string   `json:"title_link"`
	Text       *string   `json:"text"`
	ImageUrl   *string   `json:"image_url"`
	Fields     []*Field  `json:"fields"`
	Footer     *string   `json:"footer"`
	FooterIcon *string   `json:"footer_icon"`
	Timestamp  *int64    `json:"ts"`
	MarkdownIn *[]string `json:"mrkdwn_in"`
}

type Payload struct {
	Parse       string       `json:"parse,omitempty"`
	Username    string       `json:"username,omitempty"`
	IconUrl     string       `json:"icon_url,omitempty"`
	IconEmoji   string       `json:"icon_emoji,omitempty"`
	Channel     string       `json:"channel,omitempty"`
	Text        string       `json:"text,omitempty"`
	LinkNames   string       `json:"link_names,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
	UnfurlLinks bool         `json:"unfurl_links,omitempty"`
	UnfurlMedia bool         `json:"unfurl_media,omitempty"`
}

func (attachment *Attachment) addField(field Field) *Attachment {
	attachment.Fields = append(attachment.Fields, &field)
	return attachment
}

func send(webhookUrl string, payload Payload) error {

	js, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", webhookUrl, bytes.NewBuffer(js))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body) // Ignore error

	if resp.StatusCode >= 400 {
		return fmt.Errorf("Error sending slack msg. Status: %v", resp.Status)
	}
	return nil
}

// Send a Slack notification contained in msg according to cfg.
func SendNotification(cfg *Config, msg string) error {
	//attachment1 := Attachment{}
	//attachment1.addField(Field{Title: "Author", Value: "Good Bot"}).addField(Field{Title: "Status", Value: "Completed"})
	text := strings.Replace(msg, "{URL_PREFIX}", cfg.UrlPrefix, -1)
	payload := Payload{
		Text:      text,
		Username:  cfg.UserName,
		Channel:   cfg.Channel,
		IconEmoji: cfg.IconEmoji,
		//	Attachments: []Attachment{attachment1},
	}
	return send(cfg.Url, payload)
}
