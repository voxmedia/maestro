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
import { Button, Checkbox, Col, ControlLabel, Form, FormControl, FormGroup,
         InputGroup, DropdownButton, MenuItem, Modal, Table } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';

import {CodeMirrorEditor} from 'js/code.jsx';
import {Conditions} from 'js/conditions.jsx';
import {toString, isJson, AlertDismissable, findSurrogatePair} from 'js/utils.jsx';

class TableParents extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {

        if (this.props.parents.length == 0) {
            return (
                <div><h3>Table has no Maestro-controlled parents</h3></div>
            );
        } else {
            const parents = this.props.parents.map((parent, i) => {
                return (
                        <div><Link to={"/table/"+parent.Id}>{parent.Name}</Link><br/></div>
                );
            });
            return (
                    <div>
                    <h3>Parents</h3>
                    {parents}
                </div>
            );
        }
    }
}

class GCSExtracts extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {

        if (this.props.urls === null || this.props.urls.length == 0) {
            return (
                <div><h3>No GCS Extracts.</h3></div>
            );
        } else {
            const extracts = this.props.urls.map((url, i) => {
                return (
                        <div><a href={url}>{i}</a><br/></div>
                );
            });
            return (
                    <div>
                    <h3>GCS Extracts</h3>
                    {extracts}
                </div>
            );
        }
    }
}

class BQJobDetail extends React.Component {
    detailRow(row) {
        let val = JSON.stringify(JSON.parse(row.Value), null, 2);
        return (
            <CodeMirrorEditor
               value={val}
               options={{mode: "yaml", readOnly: true}}
            />
        );
    }

    render() {
        if (this.props.data) {
            let obj = this.props.data;
            let items = [];
            for (var key in obj) {
                items.push({ Id: obj[key]+obj.Id, Key: key, Value: obj[key]});
            }
            return (
                    <BootstrapTable data={ items } striped hover expandableRow={(tr) => isJson(tr.Value)} expandComponent={this.detailRow}>
                      <TableHeaderColumn dataField='Id' isKey hidden>Key</TableHeaderColumn>
                      <TableHeaderColumn dataField='Key'>Key</TableHeaderColumn>
                      <TableHeaderColumn dataField='Value'>Value</TableHeaderColumn>
                    </BootstrapTable>
            );
        }
    }
}

class BQJobList extends React.Component {
    constructor(props) {
        super(props);
        this.state = { jobs: [], Page: 0 };

        this.nextClick = this.nextClick.bind(this);
        this.prevClick = this.prevClick.bind(this);
    }

    componentWillReceiveProps(nextProps) {
        if (this.props.table_id !== nextProps.table_id) { // Without this state will be fetched on every keystroke
            this.props.table_id = nextProps.table_id;
            if (this.props.table_id !== 0) {
                this.fetchState()
            }
        }
    }

    componentDidMount() {
        if (this.props.table_id !== 0) {
            this.fetchState()
        }
    }

    fetchState() {
        this.serverRequest =
            axios
            .get("/table/"+this.props.table_id+"/jobs?p="+this.state.Page)
            .then((result) => {
                this.setState({ jobs: result.data });
            });
    }


    detailRow(tr) {
        return ( <BQJobDetail data={tr} /> );
    }

    nextClick() {
        this.state.Page = this.state.Page+1;
        this.fetchState();
    }

    prevClick() {
        if (this.state.Page > 0) {
            this.state.Page = this.state.Page-1;
            this.fetchState();
        }
    }

    render() {
        // compute cost
        const jobs = this.state.jobs.map((job, i) => {
            job.Cost = Math.round((job.TotalBytesBilled * (500.0 / (1024*1024*1024*1024))) * 1000) / 1000;
            return job;
        });

        return (
         <div>
          <BootstrapTable data={jobs} striped hover expandableRow={() => true} expandComponent={this.detailRow}>
            <TableHeaderColumn isKey dataField='BQJobId'>ID</TableHeaderColumn>
            <TableHeaderColumn dataField='EndTime'>End Time</TableHeaderColumn>
            <TableHeaderColumn dataField='TotalBytesBilled'>Bytes Billed</TableHeaderColumn>
            <TableHeaderColumn dataField='Cost'>Cost (cents)</TableHeaderColumn>
            <TableHeaderColumn dataField='Error'>Error</TableHeaderColumn>
          </BootstrapTable>
              <br/>
              <Button onClick={() => this.prevClick()} disabled={this.state.Page === 0}>Prev</Button>
              <Button onClick={() => this.nextClick()}>Next</Button>
         </div>
        );
    }
}

class TableDetail extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            Id: 0,
            GroupId: 0,
            Groups: [],
            GroupName: "None",
            AllGroups: [],
            Writable: false,
            Query: "",
            Name: "",
            DatasetId: 0,
            Dataset: "",
            Datasets: [],
            Disposition: "",
            Partitioned: false,
            Description: "",
            LegacySQL: false,
            Extract: false,
            NotifyExtractUrl: "",
            SheetsExtract: false,
            SheetId: "",
            ExternalTmout: 0,
            ExternalFormat: "",
            GithubUrl: "",
            FreqId: 0,
            FreqName: "None",
            Freqs: [],
            Conditions: [],
            ReimportCond: [],
            IdColumn: "",
            ImportDbId: 0,
            ImportDbName: "None",
            ImportDbs: [],
            ImportSelect: {
                Select: "*"
            },
            Parents: [],
            Error: "",
            Flash: "",
            DeleteModal: false,
            Extracts: {
              Id: 0,
              StartTime: "",
              URLs: []
            },
            TableNameModal: false,
            TableNameValid: false,
            Export: false,
            ExportDbId: 0,
            ExportDbName: "Make a selection",
            ExportTableName: ""
        };

        this.saveTable = this.saveTable.bind(this);
        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleCodeChange = this.handleCodeChange.bind(this);
        this.handleConditionsChange = this.handleConditionsChange.bind(this);
        this.handleReimportCondChange = this.handleReimportCondChange.bind(this);
        this.handleDispositionChange = this.handleDispositionChange.bind(this);
        this.handleFormatChange = this.handleFormatChange.bind(this);
        this.handleSaveClick = this.handleSaveClick.bind(this);
        this.handleRunClick = this.handleRunClick.bind(this);
        this.handleDryRunClick = this.handleDryRunClick.bind(this);
        this.handleReimportClick = this.handleReimportClick.bind(this);
        this.handleDeleteClick = this.handleDeleteClick.bind(this);
        this.handleFreqChange = this.handleFreqChange.bind(this);
        this.handleGroupChange = this.handleGroupChange.bind(this);
        this.handleDatasetChange = this.handleDatasetChange.bind(this);
        this.handleImportDbChange = this.handleImportDbChange.bind(this);
        this.handleImportSelectChange = this.handleImportSelectChange.bind(this);
        this.handleImportNameChange = this.handleImportNameChange.bind(this);
        this.handleExportDbChange = this.handleExportDbChange.bind(this);
        this.handleSummaryNameChange = this.handleSummaryNameChange.bind(this);
        this.checkStatus = this.checkStatus.bind(this);
    }

    componentDidMount() {
        this.fetchState();
    }

    componentWillReceiveProps(nextProps) {
        // See: https://stackoverflow.com/questions/43087007/react-link-vs-a-tag-and-arrow-function/43986829#43986829
        if (this.props.match.params.id !== nextProps.match.params.id) {
            this.fetchState();
        }
    }

    // Promises for freqs, datasets, etc.
    getFreqs() {
        return axios.get("/freqs").then((result) => result);
    }
    getDatasets() {
        return axios.get("/datasets").then((result) => result);
    }
    getImportDbs() {
        return axios.get("/dbs").then((result) => result);
    }
    getUser() {
        return axios.get("/user/").then((result) => result);
    }
    getAllGroups() {
        return axios.get("/groups").then((result) => result);
    }
    getEverything() {
        return Promise.all([this.getFreqs(), this.getDatasets(), this.getImportDbs(), this.getUser(), this.getAllGroups()]);
    }

    fetchState() {
        // This waits for the above promises to be fullfiled
        this.getEverything().then(([freqs, datasets, importdbs, user, allGroups]) => {

            let ns = {}; // new state

            ns.Freqs = freqs.data;
            ns.Datasets = datasets.data;
            ns.ImportDbs = importdbs.data;
            ns.Groups = user.data.Groups;
            ns.AllGroups = allGroups.data;

            if (this.props.match.params.id != "new" && this.props.match.params.id != "new_import" && this.props.match.params.id != "new_external") {
                this.serverRequest =
                    axios
                    .get("/table/"+this.props.match.params.id)
                    .then((result) => {
                        Object.keys(result.data).map((name) => {
                            ns[name] = result.data[name];
                            if (name == "GroupId") {
                                let g = ns.AllGroups.find(g => g.Id == result.data.GroupId);
                                if (g) {
                                    ns.GroupName = g.Name
                                }
                                if (user.Admin) {
                                    ns.Writable = true;
                                } else {
                                    let w = ns.Groups.find(g => g.Id == result.data.GroupId);
                                    if (w) {
                                        ns.Writable = true
                                    }
                                }
                            } else if (name == "FreqId") {
                                let f = ns.Freqs.find(f => f.Id == result.data.FreqId);
                                if (f) {
                                    ns.FreqName = f.Name
                                }
                            } else if (name == "DatasetId") {
                                let d = ns.Datasets.find(d => d.Id == result.data.DatasetId);
                                if (d) {
                                    ns.Dataset = d.Dataset;
                                }
                            } else if (name == "ImportDbId") {
                                let db = ns.ImportDbs.find(db => db.Id == result.data.ImportDbId);
                                if (db) {
                                    ns.ImportDbName = db.Name;
                                }
                            } else if (name == "ExportDbId") {
                                let db = ns.ImportDbs.find(db => db.Id == result.data.ExportDbId);
                                if (db) {
                                    ns.ExportDbName = db.Name;
                                }
                                ns.Export = !!result.data.ExportDbId;
                            }
                        });
                        // fix up the external format
                        if (ns.ExternalTmout != 0 && ns.ExternalFormat == "") {
                            ns.ExternalFormat = "CSV";
                        }
                        // finally set the state at once
                        this.setState( ns );
                    });
            } else {
                if (this.props.match.params.id == "new_import") {
                    ns.Query = toString(this.state.ImportSelect);
                }
                if (this.props.match.params.id == "new" || this.props.match.params.id == "new_external" ) {
                    ns.TableNameModal = true;
                    if ( this.props.match.params.id == "new_external" ) {
                        ns.ExternalTmout = 3600;
                        ns.ExternalFormat = "CSV";
                    }
                    // default to the first dataset
                    if (ns.Datasets.length > 0) {
                        ns.Dataset = ns.Datasets[0].Dataset;
                        ns.DatasetId = ns.Datasets[0].Id;
                    }
                }
                // default to the first group
                if (ns.Groups.length > 0) {
                    ns.GroupName = ns.Groups[0].Name;
                    ns.GroupId = ns.Groups[0].Id;
                }
                ns.Writable = true;
                this.setState( ns );
            }
        });
    }

    checkStatus() {
        axios
            .get("/table/"+this.state.Id +"/status")
            .then((result) => {
                let running = (result.data.Status === "running");
                if (running !== this.state.Running) {  // status change
                    this.fetchState();
                } else {
                    // this will render(), which will trigger checkStatus() again
                    this.setState({ Running: running });
                }
            });
    }

    handleInputChange(event) {
        const target = event.target;
        const value = target.type === 'checkbox' ? target.checked : target.value;
        const name = target.name;
        let ns = { [name] : value };
        if (name == "SheetsExtract") {
            ns.Extract = false;
            ns.Export = false;
        }
        if (name == "Extract") {
            ns.SheetsExtract = false;
            ns.Export = false;
        }
        if (name == "Export") {
            ns.SheetsExtract = false;
            ns.Extract = false;
            if (!value) {
                ns.ExportDbId = 0;
                ns.ExportDbName = "Make a selection";
                ns.ExportTableName = "";
            }
        }
        if (name == "IdColumn") {
            ns.Disposition = value !== "" ? "WRITE_APPEND" : "WRITE_TRUNCATE";
        }
        if (name == "ExternalTmout") {
            ns.ExternalTmout = parseInt(value);
            if (ns.ExternalTmout == 0) {
                ns.ExternalTmout = 3600;
            }
        }
        this.setState(ns);
    }

    handleConditionsChange(conds) {
        this.setState({Conditions: conds});
    }

    handleReimportCondChange(conds) {
        this.setState({ReimportCond: conds});
    }

    handleCodeChange(value) {
        this.setState({Query: value});
    }

    handleDispositionChange(event) {
        this.setState( { Disposition: event.target.value } );
    }

    handleFormatChange(event) {
        this.setState( { ExternalFormat: event.target.value } );
    }

    handleFreqChange(event) {
        this.setState( { FreqId: event } );
        let f = this.state.Freqs.find(f => f.Id == event);
        if (f) {
            this.setState( { FreqName: f.Name } );
        } else {
            this.setState( { FreqName: "None" } );
        }
    }

    handleGroupChange(event) {
        this.setState( { GroupId: event } );
        let g = this.state.Groups.find(g => g.Id == event);
        if (g) {
            this.setState( { GroupName: g.Name } );
        }
    }

    handleDatasetChange(event) {
        this.setState( { DatasetId: event } );
        let d = this.state.Datasets.find(d => d.Id == event);
        if (d) {
            this.setState( { Dataset: d.Dataset } );
        } else {
            this.setState( { Dataset: "ERROR" } );
        }
    }

    handleImportDbChange(event) {
        this.setState( { ImportDbId: event } );
        let db = this.state.ImportDbs.find(db => db.Id == event);
        if (db) {
            this.setState( { ImportDbName: db.Name, DatasetId: db.DatasetId } );
        } else {
            this.setState( { ImportDbName: "Make a selection" } );
        }
    }

    handleExportDbChange(event) {
        this.setState( { ExportDbId: event } );
        let db = this.state.ImportDbs.find(db => db.Id == event);
        if (db) {
            let name = this.state.ExportTableName;
            if (name == "") name = this.state.Name;
            this.setState( { ExportDbName: db.Name, ExportTableName: name } );
        } else {
            this.setState( { ExportDbName: "Make a selection" } );
        }
    }

    handleImportSelectChange(event) {
        const target = event.target;
        const value = target.value;
        const name = target.name;
        var select = this.state.ImportSelect;
        select[name] = value;
        this.setState({ ImportSelect: select,
                        Query: toString(this.state.ImportSelect) });
    }

    handleImportNameChange(event) {
        const value = event.target.value;
        var select = this.state.ImportSelect;
        select.From = value;
        this.setState({ ImportSelect: select,
                        Name: value,
                        Query: toString(select) });
    }

    handleSummaryNameChange(event) {
        let value = event.target.value;
        let re = /^[0-9_a-z]+$/g; // only lowercase and underscores
        this.setState({ Name: event.target.value, TableNameValid: re.test(value) });
    }

    saveTable(action) {
        if (action == "insert") { // POST
            axios.post("/table/", this.state)
                .then((result) => {
                    // We are returned a new Table which now should have an Id
                    // we should update the state with the new data.
                    let ns = {};
                    Object.keys(result.data).map((name) => {
                        ns[name] = result.data[name];
                    });
                    console.log("POST OK " + toString(result));
                    ns.Flash = "Table created.";
                    this.setState(ns);
                    this.props.history.push("/table/"+result.data.Id); // "redirect"
                })
                .catch((error) => {
                    console.log("POST ERROR " + toString(error.message));
                    this.setState({ Flash: "Error creating table: "+error.response.data});
                });
        } else if (action == "update") { // PUT
            axios.put("/table/"+this.props.match.params.id, this.state)
                .then((result) => {
                    console.log("PUT OK " + toString(result));
                    this.setState({ Flash: "Table saved.", Error: "" });
                })
                .catch((error) => {
                    console.log("PUT ERROR " + toString(error.message));
                    this.setState({ Flash: "Error saving table."});
                });
        } else if (action == "delete") { // DELETE
            axios.delete("/table/"+this.props.match.params.id, this.state)
                .then((result) => {
                    console.log("DELETE OK " + toString(result));
                    this.setState({ Flash: "Table marked deleted!" });
                })
                .catch((error) => {
                    console.log("DELETE ERROR " + toString(error.message));
                    this.setState({ Flash: "Error deleting table."});
                });
        }
    }

    handleSaveClick(event) {
        if (this.props.match.params.id == "new" || this.props.match.params.id == "new_import" || this.props.match.params.id == "new_external") { // POST
            this.saveTable("insert");
        } else {
            this.saveTable("update");
        }
    }

    handleRunClick(event) {
        axios
            .get("/table/"+this.state.Id +"/run")
            .then((result) => {
                this.setState({ Running: true });
            });
    }

    handleDryRunClick(event) {
        axios
            .get("/table/"+this.state.Id +"/dryrun")
            .then((result) => {
                let error = result.data.error;
                if (error) {
                    this.setState({ Flash: "", Error: error });
                } else {
                    this.setState({ Flash: "No errors.", Error: "" });
                }
            });
    }

    handleReimportClick(event) {
        axios
            .get("/table/"+this.state.Id +"/reimport")
            .then((result) => {
                this.setState({ Running: true });
            });
    }

    handleDeleteClick(event) {
        this.saveTable("delete");
        setTimeout(() => this.props.history.push("/"), 1000); // "redirect"
    }

    render() {
        if (this.state.Running) {
            setTimeout(this.checkStatus, 3000);
        }
        if (this.props.match.params.id == "new_external" || this.state["ExternalTmout"] != 0) {
            return this.renderExternalTable();
        } else if (this.props.match.params.id == "new_import" || this.state["ImportDbId"] != 0) {
            return this.renderImportTable();
        } else {
            if (this.state.Id == 0 && this.props.match.params.id !== "new") {
                return (<div></div>); // the state has not been loaded
            } else {
                return this.renderSummaryTable();
            }
        }
    }

    renderSummaryTable() {
        return (
        <div className="tableDetail">
          <Modal show={this.state.Running}>
             <Modal.Body>This table is currently running, please wait...</Modal.Body>
          </Modal>

          <Modal show={this.state.TableNameModal}>
             <Modal.Body>
                Please give this table a name and select a dataset. The name and dataset cannot be changed.
                Use lower case and underscores.
                Make the name descriptive, with broader terms such as your team name on the left and more descriptive terms towards the right.
                For scheduled tables, it may be a good idea to include the frequency in the name.
                For example: <tt>eng_nginx_status_400_count_hourly</tt>.
                <Form inline>
                  <label>
                    Name:&nbsp;
                    <FormControl name="Name" type="text" value={this.state["Name"]} onChange={this.handleSummaryNameChange} size="40"/>
                  </label>
                  <label>
                     &nbsp;&nbsp;Dataset:&nbsp;
                     <DropdownButton title={ this.state.Dataset } onSelect={this.handleDatasetChange}>
                      { this.state.Datasets.map((ds) => {
                         return <MenuItem eventKey={ds.Id} active={ds.Id == this.state.DatasetId}>{ds.Dataset}</MenuItem>;
                     })}
                    </DropdownButton>
                  </label>
                  <br/> If you are not sure about the dataset selection, the default value ({this.state.Dataset}) is probably best.
                  <br/>
                  <label>
                    <input name="Partitioned" type="checkbox" checked={this.state.Partitioned} onChange={this.handleInputChange} /> Partitioned&nbsp;&nbsp;
                  </label> (Advanced feature, leave unchecked if unsure).
                </Form>
             </Modal.Body>
             <Modal.Footer>
                <Button onClick={() => this.props.history.push("/summary")}>Cancel</Button>
                <Button disabled={!this.state.TableNameValid} onClick={() => this.setState({ TableNameModal: false})} bsStyle="success">Looks Good</Button>
             </Modal.Footer>
          </Modal>

          <h2>Table Detail (Summary Table)</h2>
                <p>Table id {this.props.match.params.id} <a href={this.state.GithubUrl}>[revision history]</a>
                &nbsp;&nbsp;Created by: {this.state.Email}
                &nbsp;&nbsp;Group:&nbsp;
                <DropdownButton title={ this.state.GroupName } onSelect={this.handleGroupChange} disabled={!this.state.Writable}>
                { this.state.Groups.map((group) => {
                    return <MenuItem eventKey={group.Id} active={group.Id == this.state.GroupId}>{group.Name}</MenuItem>;
                })}
                </DropdownButton>
                </p>

          <Form inline>
            <label>
              Name:&nbsp; { this.state.Dataset + "." + this.state.Name } &nbsp;&nbsp;
            </label>
            <label>
                <input name="Partitioned" readOnly type="checkbox" checked={this.state.Partitioned} disabled={true} /> Partitioned&nbsp;&nbsp;
            </label>
            <br/>
            <label>
              Description:&nbsp;
              <input name="Description" type="text" value={this.state.Description} onChange={this.handleInputChange}
                 disabled={!this.state.Writable} size="70"/>
            </label>
            <br/>
            <label>
                <input name="LegacySQL" type="checkbox" checked={this.state.LegacySQL} onChange={this.handleInputChange}
                   disabled={!this.state.Writable} /> Legacy SQL &nbsp;&nbsp;
            </label>
            <label>
                <input type="radio" value="WRITE_TRUNCATE" checked={this.state.Disposition != "WRITE_APPEND"} onChange={this.handleDispositionChange}
                  disabled={!this.state.Writable} />
                Replace &nbsp;
            </label>
            <label>
              <input type="radio" value="WRITE_APPEND" checked={this.state.Disposition == "WRITE_APPEND"} onChange={this.handleDispositionChange}
                disabled={!this.state.Writable} /> Append
            </label>
            <br/>
             <label>
                &nbsp;&nbsp;Include in run:&nbsp;
                <DropdownButton title={ this.state.FreqName } onSelect={this.handleFreqChange} disabled={!this.state.Writable}>
                { this.state.Freqs.map((freq) => {
                    return <MenuItem eventKey={freq.Id} active={freq.Id == this.state.FreqId}>{freq.Name}</MenuItem>;
                })}
                <MenuItem eventKey={0} active={this.state.FreqId == -1}>None</MenuItem>
                </DropdownButton>
            </label>
            <label>
              &nbsp;&nbsp;
                <Conditions value={this.state.Conditions} visible={this.state.FreqId > 0} onChange={this.handleConditionsChange} weekdayslabel="Only on:"
                  disabled={!this.state.Writable} />
            </label>
            <br/><br/>

            <CodeMirrorEditor
               ref="editor"
               name="Query"
               value={this.state.Query}
               onChange={this.handleCodeChange}
               options={{mode: "text/x-bigquery", lineNumbers: true, readOnly: this.state.Writable ? false : "nocursor"}}
               autoFocus={true}
            />

            </Form>

            <br/>
            <Form horizontal>
            <FormGroup>
                <Col sm={2}>
                  <Checkbox name="Extract" checked={this.state.Extract} onChange={this.handleInputChange} disabled={!this.state.Writable}>
                    <b>Make GCS Extract</b>
                  </Checkbox>
                </Col>
                <Col componentClass={ControlLabel} sm={2} hidden={!this.state.Extract}>Notify URL (optional):</Col>
                <Col sm={4} hidden={!this.state.Extract}>
                  <FormControl name="NotifyExtractUrl" type="text" value={this.state.NotifyExtractUrl} onChange={this.handleInputChange}
                     disabled={!this.state.Writable}/>
                </Col>
            </FormGroup>
            </Form>

            <Form horizontal>
            <FormGroup>
                <Col sm={2}>
                  <Checkbox name="SheetsExtract" checked={this.state.SheetsExtract} onChange={this.handleInputChange} disabled={!this.state.Writable}>
                    <b>Export to Google Sheets</b>
                  </Checkbox>
                </Col>
                <Col sm={6} hidden={(!this.state.SheetsExtract) || (this.state.SheetId === '')}>
                  <a href={'https://docs.google.com/spreadsheets/d/'+this.state.SheetId} target="_blank">
                   {'https://docs.google.com/spreadsheets/d/'+this.state.SheetId}
                  </a> {"\u2197"}
                </Col>
            </FormGroup>
            </Form>

            <Form horizontal>
            <FormGroup>
                <Col sm={2}>
                  <Checkbox name="Export" checked={this.state.Export} onChange={this.handleInputChange} disabled={!this.state.Writable}>
                    <b>Export to database</b>
                  </Checkbox>
                </Col>
                <Col sm={2} hidden={!this.state.Export}>
                    <b>Database:&nbsp;&nbsp;</b>
                    <DropdownButton title={ this.state.ExportDbName } onSelect={this.handleExportDbChange} disabled={!this.state.Writable}>
                    { this.state.ImportDbs.filter(db => db.Export).map((db) => {
                        return <MenuItem eventKey={db.Id} active={db.Id == this.state.ExportDbId}>{db.Name}</MenuItem>;
                    })}
                    </DropdownButton>
                </Col>
                <Col componentClass={ControlLabel} sm={2} hidden={!this.state.Export}>As Table Name:</Col>
                <Col sm={2} hidden={!this.state.Export}>
                    <FormControl name="ExportTableName" type="text" value={this.state.ExportTableName} onChange={this.handleInputChange}
                       disabled={!this.state.Writable} />
                </Col>
            </FormGroup>
            </Form>

            <br/>
            <br/>

            <Button onClick={this.handleSaveClick} disabled={!this.state.Writable || (this.state.Export && this.state.ExportDbId == 0)}>Save</Button>
            <Button onClick={this.handleDryRunClick} disabled={!this.state.Writable}>Dry Run</Button>
            <Button onClick={this.handleRunClick} disabled={!this.state.Writable || this.state.Id == 0}>
                {this.state.Running ? 'Running...' : 'Run Now'}
            </Button>
            <Button onClick={() => this.setState({ DeleteModal: true })} disabled={!this.state.Writable || this.state.Id == 0}>Delete</Button>

            <AlertDismissable message={this.state.Flash} show={this.state.Flash !== ""} />
            <AlertDismissable message={'Last error: "'+this.state.Error+'" (Saving table clears last error).'} show={this.state.Error !== ""} style="danger" />

            <Modal show={this.state.DeleteModal}>
                <Modal.Body>Are you sure?</Modal.Body>
                <Modal.Footer>
                    <Button onClick={() => this.setState({ DeleteModal: false })}>Cancel</Button>
                    <Button onClick={this.handleDeleteClick} bsStyle="danger">Yes, delete!</Button>
                </Modal.Footer>
            </Modal>

         <br/>

          <TableParents parents={this.state.Parents} />

          <GCSExtracts urls={this.state.Extracts.URLs} />

          <BQJobList table_id={this.state.Id} />

        </div>
        );
    }

    renderImportTable() {
        return (
        <div className="tableDetail">
          <Modal show={this.state.Running}>
             <Modal.Body>This table is currently running, please wait...</Modal.Body>
          </Modal>

          <h2>Table Detail (Import Table)</h2>
                <p>Table id {this.props.match.params.id} <a href={this.state.GithubUrl}>[revision history]</a>
                &nbsp;&nbsp;Created by: {this.state.Email}
                &nbsp;&nbsp;Group:&nbsp;
                <DropdownButton title={ this.state.GroupName } onSelect={this.handleGroupChange} disabled={!this.state.Writable}>
                { this.state.Groups.map((group) => {
                    return <MenuItem eventKey={group.Id} active={group.Id == this.state.GroupId}>{group.Name}</MenuItem>;
                })}
                </DropdownButton>
                </p>

          <Form inline>
            <label>
              Name:
              <FormControl name="Name" type="text" value={this.state["Name"]} onChange={this.handleImportNameChange} size="40"
                disabled={!this.state.Writable} />
            </label>
             <label>
                &nbsp;&nbsp;Import from DB:&nbsp;
                <DropdownButton title={ this.state.ImportDbName } onSelect={this.handleImportDbChange} disabled={!this.state.Writable}>
                { this.state.ImportDbs.map((db) => {
                    return <MenuItem eventKey={db.Id} active={db.Id == this.state.ImportDbId}>{db.Name}</MenuItem>;
                })}
                </DropdownButton>
            </label>
            <br/>
            <label>
              Description:
              <input name="Description" type="text" value={this.state.Description} onChange={this.handleInputChange} size="70"
                disabled={!this.state.Writable} />
            </label>
             <br/>
             <label>
                Include in run:&nbsp;
                <DropdownButton title={ this.state.FreqName } onSelect={this.handleFreqChange} disabled={!this.state.Writable}>
                { this.state.Freqs.map((freq) => {
                    return <MenuItem eventKey={freq.Id} active={freq.Id == this.state.FreqId}>{freq.Name}</MenuItem>;
                })}
                <MenuItem eventKey={0} active={this.state.FreqId == -1}>None</MenuItem>
                </DropdownButton>
            </label>
            <label>
              &nbsp;&nbsp;
                <Conditions value={this.state.Conditions} visible={this.state.FreqId > 0} onChange={this.handleConditionsChange} weekdayslabel="Only on:"
                  disabled={!this.state.Writable} />
            </label>
            <br/>
            <label>
                Incremental Id Column:&nbsp;&nbsp;
                <input name="IdColumn" type="text" value={this.state.IdColumn} onChange={this.handleInputChange} disabled={!this.state.Writable} />
            </label>
                &nbsp;&nbsp;(blank = table is replaced)
            <br/>
            <label>
                {this.state.IdColumn !== "" ? "Reimport on: " : ""}
                <Conditions value={this.state.ReimportCond} visible={this.state.IdColumn !== ""} onChange={this.handleReimportCondChange}
                   disabled={!this.state.Writable} />
            </label>
            <br/><br/>

            <br/>
            <table><tbody>
            <tr>
              <th>Select:</th>
              <td>
                <input name="Select" type="text" value={this.state.ImportSelect.Select} onChange={this.handleImportSelectChange} size="70"
                  disabled={!this.state.Writable} />
              </td>
            </tr>
            <tr>
              <th>From:</th>
              <td>
                <input name="From" type="text" value={this.state.ImportSelect.From} onChange={this.handleImportSelectChange} size="70"
                  disabled={!this.state.Writable} />
              </td>
            </tr>
            <tr>
              <th>Where:</th>
              <td>
                <input name="Where" type="text" value={this.state.ImportSelect.Where} onChange={this.handleImportSelectChange} size="70"
                  disabled={!this.state.Writable} />
              </td>
            </tr>
            <tr>
              <th>Limit:</th>
              <td>
                <input name="Limit" type="text" value={this.state.ImportSelect.Limit} onChange={this.handleImportSelectChange} size="70"
                  disabled={!this.state.Writable} />
              </td>
            </tr>
            </tbody></table>

            <br/>

            <Button onClick={this.handleSaveClick} disabled={!this.state.Writable}>Save</Button>
            <Button onClick={this.handleRunClick} disabled={!this.state.Writable || this.state.Id == 0}>
                {this.state.Running ? 'Running...' : 'Run Now'}
            </Button>
            <Button onClick={this.handleReimportClick} disabled={!this.state.Writable || this.state.Id == 0}>Reimport</Button>
            <Button onClick={() => this.setState({ DeleteModal: true })} disabled={!this.state.Writable || this.state.Id == 0}>Delete</Button>

            <AlertDismissable message={this.state.Flash} show={this.state.Flash !== ""} />
            <AlertDismissable message={'Last error: "'+this.state.Error+'" (Saving table clears last error).'} show={this.state.Error !== ""} style="danger" />

            <Modal show={this.state.DeleteModal}>
                <Modal.Body>Are you sure?</Modal.Body>
                <Modal.Footer>
                    <Button onClick={() => this.setState({ DeleteModal: false })}>Cancel</Button>
                    <Button onClick={this.handleDeleteClick} bsStyle="danger">Yes, delete!</Button>
                </Modal.Footer>
            </Modal>

         </Form>

         <br/>

          <TableParents parents={this.state.Parents} />

          <BQJobList table_id={this.state.Id} />

        </div>
        );
    }

    renderExternalTable() {
        return (
        <div className="tableDetail">
          <Modal show={this.state.Running}>
             <Modal.Body>This table is currently running, please wait...</Modal.Body>
          </Modal>

          <Modal show={this.state.TableNameModal}>
             <Modal.Body>
                Please give this table a name and select a dataset. The name and dataset cannot be changed.
                Use lower case and underscores.
                Make the name descriptive, with broader terms such as your team name on the left and more descriptive terms towards the right.
                For scheduled tables, it may be a good idea to include the frequency in the name.
                For example: <tt>eng_nginx_status_400_count_hourly</tt>.
                <Form inline>
                  <label>
                    Name:&nbsp;
                    <FormControl name="Name" type="text" value={this.state["Name"]} onChange={this.handleSummaryNameChange} size="40"/>
                  </label>
                  <label>
                     &nbsp;&nbsp;Dataset:&nbsp;
                     <DropdownButton title={ this.state.Dataset } onSelect={this.handleDatasetChange}>
                      { this.state.Datasets.map((ds) => {
                         return <MenuItem eventKey={ds.Id} active={ds.Id == this.state.DatasetId}>{ds.Dataset}</MenuItem>;
                     })}
                    </DropdownButton>
                  </label>
                  <br/> If you are not sure about the dataset selection, the default value ({this.state.Dataset}) is probably best.
                  <br/>
                </Form>
             </Modal.Body>
             <Modal.Footer>
                <Button onClick={() => this.props.history.push("/summary")}>Cancel</Button>
                <Button disabled={!this.state.TableNameValid} onClick={() => this.setState({ TableNameModal: false})} bsStyle="success">Looks Good</Button>
             </Modal.Footer>
          </Modal>

          <h2>Table Detail (External Table)</h2>
                <p>Table id {this.props.match.params.id} <a href={this.state.GithubUrl}>[revision history]</a>
                &nbsp;&nbsp;Created by: {this.state.Email}
                &nbsp;&nbsp;Group:&nbsp;
                <DropdownButton title={ this.state.GroupName } onSelect={this.handleGroupChange} disabled={!this.state.Writable}>
                { this.state.Groups.map((group) => {
                    return <MenuItem eventKey={group.Id} active={group.Id == this.state.GroupId}>{group.Name}</MenuItem>;
                })}
                </DropdownButton>
                </p>

          <Form inline>
            <label>
              Name:&nbsp; { this.state.Dataset + "." + this.state.Name } &nbsp;&nbsp;
            </label>
            <br/>
            <label>
              Description:&nbsp;
              <input name="Description" type="text" value={this.state.Description} onChange={this.handleInputChange} size="70"
                disabled={!this.state.Writable} />
            </label>
            <br/>
            <label>
                Disposition:&nbsp;
                <input type="radio" value="WRITE_TRUNCATE" checked={this.state.Disposition != "WRITE_APPEND"} onChange={this.handleDispositionChange}
                  disabled={!this.state.Writable} />
                Replace &nbsp;
            </label>
            <label>
              <input type="radio" value="WRITE_APPEND" checked={this.state.Disposition == "WRITE_APPEND"} onChange={this.handleDispositionChange}
                disabled={!this.state.Writable} /> Append
            </label>
            <br/>
             <label>
                &nbsp;&nbsp;Include in run:&nbsp;
                <DropdownButton title={ this.state.FreqName } onSelect={this.handleFreqChange} disabled={!this.state.Writable}>
                { this.state.Freqs.map((freq) => {
                    return <MenuItem eventKey={freq.Id} active={freq.Id == this.state.FreqId}>{freq.Name}</MenuItem>;
                })}
                <MenuItem eventKey={0} active={this.state.FreqId == -1}>None</MenuItem>
                </DropdownButton>
            </label>
            <label>
              &nbsp;&nbsp;
                <Conditions value={this.state.Conditions} visible={this.state.FreqId > 0} onChange={this.handleConditionsChange} weekdayslabel="Only on:"
                  disabled={!this.state.Writable} />
            </label>
            <br/><br/>
            <label>
              External process timeout:&nbsp;
              <input name="ExternalTmout" type="number" min={60} max={10800}value={this.state.ExternalTmout} onChange={this.handleInputChange} size="10"
                disabled={!this.state.Writable} /> Seconds
            </label>
            <br/>
            <label>
                Format:&nbsp;
                <input type="radio" value="CSV" checked={this.state.ExternalFormat != "NEWLINE_DELIMITED_JSON"} onChange={this.handleFormatChange}
                  disabled={!this.state.Writable} />
                &nbsp;CSV &nbsp;
            </label>
            <label>
              <input type="radio" value="NEWLINE_DELIMITED_JSON" checked={this.state.ExternalFormat == "NEWLINE_DELIMITED_JSON"}
               onChange={this.handleFormatChange} disabled={!this.state.Writable} /> Newline-delimited JSON
            </label>
            <br/>
          </Form>

          <h4>Note: external tables can only be ran by outside processes via API.</h4>
          <br/>

            <Button onClick={this.handleSaveClick} disabled={!this.state.Writable}>Save</Button>
            <Button onClick={() => this.setState({ DeleteModal: true })} disabled={!this.state.Writable || this.state.Id == 0}>Delete</Button>

            <AlertDismissable message={this.state.Flash} show={this.state.Flash !== ""} />
            <AlertDismissable message={'Last error: "'+this.state.Error+'" (Saving table clears last error).'} show={this.state.Error !== ""} style="danger" />

            <Modal show={this.state.DeleteModal}>
                <Modal.Body>Are you sure?</Modal.Body>
                <Modal.Footer>
                    <Button onClick={() => this.setState({ DeleteModal: false })}>Cancel</Button>
                    <Button onClick={this.handleDeleteClick} bsStyle="danger">Yes, delete!</Button>
                </Modal.Footer>
            </Modal>

         <br/>

          <BQJobList table_id={this.state.Id} />

        </div>
        );
    }

}

// NB: exports *must* be after declaration, or webpack build will fail
export {TableDetail};
