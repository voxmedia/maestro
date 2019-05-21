// -*- JavaScript -*-

// Copyright 2019 Vox Media, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React from 'react';
import axios from 'axios';
import { Button, Form, FormControl, FormGroup } from 'react-bootstrap';
import {AlertDismissable} from 'js/utils.jsx';

class Creds extends React.Component {
    constructor(props) {
        super(props);

        this.defaultText = "NOT SHOWN";
        this.state = {creds: this.defaultText,
                      bucket: "",
                      dataset: "",
                      repo: "",
                      token: this.defaultText,
                      slack_url: "",
                      slack_username: "maestro",
                      slack_channel: "#maestro-alerts",
                      slack_emoji: ":violin:",
                      slack_prefix: "https://maestro.example.com/",
                      Flash1: "", Flash2: "", Flash3: ""};

        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleBQSaveClick = this.handleBQSaveClick.bind(this);
        this.handleGitSaveClick = this.handleGitSaveClick.bind(this);
        this.handleSlackSaveClick = this.handleSlackSaveClick.bind(this);
    }

    handleInputChange(event) {
        const target = event.target;
        const value = target.type === 'checkbox' ? target.checked : target.value;
        const name = target.name;

        let ns = { [name] : value };
        this.setState(ns);
    }

    handleBQSaveClick(event) {
        let data = {bucket: this.state.bucket, dataset: this.state.dataset};
        if (this.defaultText != this.state.creds) {
            data.creds = this.state.creds;
        }
        axios.post("/admin/bq_config", data)
            .then((result) => {
                console.log("POST OK " + toString(result));
                this.setState({ Flash1: "BQ configuration updated." });
            })
            .catch((error) => {
                console.log("POST ERROR " + toString(error.message));
                this.setState({ Flash1: "Error updating config: "+error.response.data});
            });
    }

    handleGitSaveClick(event) {
        let data = {repo: this.state.repo};
        if (this.defaultText != this.state.token) {
            data.token = this.state.token;
        }
        axios.post("/admin/git_config", data)
            .then((result) => {
                console.log("POST OK " + toString(result));
                this.setState({ Flash2: "Git configuration updated." });
            })
            .catch((error) => {
                console.log("POST ERROR " + toString(error.message));
                this.setState({ Flash2: "Error updating config: "+error.response.data});
            });
    }

    handleSlackSaveClick(event) {
        let data = {slack_url: this.state.slack_url,
                    slack_username: this.state.slack_username,
                    slack_channel: this.state.slack_channel,
                    slack_emoji: this.state.slack_emoji,
                    slack_prefix: this.state.slack_prefix
                   };
        axios.post("/admin/slack_config", data)
            .then((result) => {
                console.log("POST OK " + toString(result));
                this.setState({ Flash3: "Slack configuration updated." });
            })
            .catch((error) => {
                console.log("POST ERROR " + toString(error.message));
                this.setState({ Flash3: "Error updating config: "+error.response.data});
            });
    }

    componentDidMount() {
        this.fetchBQConfig();
        this.fetchGitConfig();
        this.fetchSlackConfig();
    }

    fetchBQConfig() {
        axios.get("/admin/bq_config")
            .then((result) => {
                this.setState(result.data);
            });
    }

    fetchGitConfig() {
        axios.get("/admin/git_config")
            .then((result) => {
                this.setState(result.data);
            });
    }

    fetchSlackConfig() {
        axios.get("/admin/slack_config")
            .then((result) => {
                this.setState(result.data);
            });
    }

    render() {

        return (
            <div>
                <h2>Credentials</h2>

                <p>Note: For reasons of security, credentials are not shown below, but you can paste new values and click Sace to update them.</p>


                <h3>Google</h3>
                <Form inline>
                <FormGroup>
                <label>Credentials JSON:<br/>
                   <textarea name="creds" rows="13" cols="120" onChange={this.handleInputChange}>
                    {this.state["creds"]}
                   </textarea>
                </label>
                <br/>
                <Button onClick={this.handleBQSaveClick} disabled={this.state.creds == this.defaultText}>Save</Button>
                <br/>
                <br/>
                <label>Default dataset:<br/>
                   <FormControl name="dataset" type="text" value={this.state["dataset"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' BigQuery dataset for Maestro output. Not created by Maestro.'}
                <br/>
                <Button onClick={this.handleBQSaveClick}>Save</Button>
                <br/>
                <br/>
                <label>GCS Bucket:<br/>
                   <FormControl name="bucket" type="text" value={this.state["bucket"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' Used for data export/import. Not created by Maestro.'}
                <br/>
                <Button onClick={this.handleBQSaveClick}>Save</Button>
                </FormGroup>
                </Form>
                <AlertDismissable message={this.state.Flash1}
                    show={this.state.Flash1 !== ""}
                    style={this.state.Flash1.indexOf("Error") > -1 ? "danger" : "success"} />

                <hr/>
                <h3>Github</h3>
                <Form>
                <FormGroup>
                <label>Git Repo URL:<br/>
                   <FormControl name="repo" type="text" value={this.state["repo"]} onChange={this.handleInputChange} size="60"/>
                </label>
                {' Table changes will be stored in this repo.'}
                <br/>
                <label>Github Token:<br/>
                   <FormControl name="token" type="text" value={this.state["token"]} onChange={this.handleInputChange} size="40"/>
                </label>
                <br/>
                <Button onClick={this.handleGitSaveClick}>Save</Button>
                </FormGroup>
                </Form>
                <AlertDismissable message={this.state.Flash2}
                    show={this.state.Flash2 !== ""}
                    style={this.state.Flash2.indexOf("Error") > -1 ? "danger" : "success"} />

                <hr/>
                <h3>Slack</h3>
                <Form>
                <FormGroup>
                <label>Hook URL:<br/>
                   <FormControl name="slack_url" type="text" value={this.state["slack_url"]} onChange={this.handleInputChange} size="60"/>
                </label>
                {' The slack post URL (like https://hooks.slack.com/services/T02...).'}
                <br/>
                <label>Post as username:<br/>
                   <FormControl name="slack_username" type="text" value={this.state["slack_username"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' The username under which the posts will appear.'}
                <br/>
                <label>Post in channel:<br/>
                   <FormControl name="slack_channel" type="text" value={this.state["slack_channel"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' Slack channel to post in.'}
                <br/>
                <label>Icon emoji:<br/>
                   <FormControl name="slack_emoji" type="text" value={this.state["slack_emoji"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' Alerts will use this emoji.'}
                <br/>
                <label>Maestro URL:<br/>
                   <FormControl name="slack_prefix" type="text" value={this.state["slack_prefix"]} onChange={this.handleInputChange} size="40"/>
                </label>
                {' This URL will be used to construct links to Maestro.'}
                <br/>
                <Button onClick={this.handleSlackSaveClick}>Save</Button>
                </FormGroup>
                </Form>
                <AlertDismissable message={this.state.Flash3}
                    show={this.state.Flash3 !== ""}
                    style={this.state.Flash3.indexOf("Error") > -1 ? "danger" : "success"} />

            </div>
        );
    }
}

export { Creds };
