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
import { Collapse, Button, Table } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { toString, findSurrogatePair } from 'js/utils.jsx';

class TableStatus extends React.Component {
    constructor(props) {
        super(props);
        this.state = { running: false, error: "" };
        this.runClick = this.runClick.bind(this);
        this.reimportClick = this.reimportClick.bind(this);
        this.checkStatus = this.checkStatus.bind(this);
    }

    checkStatus() {
        let table = this.props.table;
        axios
            .get("/table/"+table.Id +"/status")
            .then((result) => {
                if (result.data.Status == "running") {
                    this.setState({ running: true });
                    setTimeout(this.checkStatus, 3000);
                } else {
                    this.setState({ running: false });
                    if (result.data.Status == "error") {
                        this.setState({ "error": result.data.Error });
                    }
                }
            })
            .catch((result) => {
                // This is not supposed to happen
                console.log("Error getting status: " + toString(result));
            });
    }

    runClick(event) {
        let table = this.props.table;
        event.preventDefault();
        axios
            .get("/table/"+table.Id +"/run")
            .then((result) => {
                this.setState({ running: true });
                setTimeout(this.checkStatus, 3000);
            });
    }

    reimportClick(event) {
        let table = this.props.table;
        event.preventDefault();
        axios
            .get("/table/"+table.Id +"/reimport")
            .then((result) => {
                this.setState({ running: true });
                setTimeout(this.checkStatus, 3000);
            });
    }

    render() {
        let table = this.props.table;
        if (this.state.error != "") {
            document.getElementById("errorStatus_"+table.Id).innerHTML = "<em>"+this.state.error+"</em>";
        }
        return (
        <div>
            {table.Running || this.state.running ? (
                    <div>&nbsp;[ {findSurrogatePair(0x1F680)} ]</div>
                ) : (
                    table.Error != "" || this.state.error != "" ?  (
                    <div>&nbsp;[ err ]</div>
                ) : (
                  <div>
                    <Link to={"/table/"+table.Id +"/run"} onClick={this.runClick}>&nbsp;[ run ]</Link>
                    { table.IdColumn != "" ?
                    ( <Link to={"/table/"+table.Id +"/reimport"} onClick={this.reimportClick}>&nbsp;[ reimport ]</Link> ) : "" }
                  </div>
                )
            )}
        </div>
        );
    }
}

class TableItem extends React.Component {
  constructor(props) {
    super(props);

    this.ListLinkStyle = {padding: "0 20px 0 5px"};
    this.ListColStyle = {paddingRight: "20px"};
    this.ListErrorStyle = {columnWidth: "250px", color: "#C92323"};
  }

  formatConditionWeekdays(conds) {
    if (conds.length > 0 && conds[0].weekdays.length > 0) {
        let weekdays = conds[0].weekdays;
        let names = ['Su','Mo','Tu','We','Th','Fr','Sa'];
        let result = [];
        for (var i = 0; i < weekdays.length; i++) {
            result.push(names[weekdays[i]]);
        }
        return '(' + result.join(',') + ')';
    }
    return '';
  }
}

class SummaryTableItem extends TableItem {
  constructor(props) {
    super(props)
  }

  render() {
    let table = this.props.table;
    return (
      <tr class="striped">
        <td style={this.ListLinkStyle}><Link to={"/table/"+table.Id}>{table.Id}</Link></td>
        <td style={this.ListColStyle}>{table.Name}</td>
        <td style={this.ListColStyle}>{table.Dataset}</td>
        <td style={this.ListColStyle}>{table.Description}</td>
        <td style={this.ListColStyle}>{table.FreqName} {this.formatConditionWeekdays(table.Conditions)}</td>
        <td style={this.ListColStyle}>{table.DispLabel}</td>
        <td style={this.ListColStyle}><TableStatus table={table} /></td>
        <td style={this.ListErrorStyle} id={"errorStatus_"+table.Id}><em> {table.Error} </em></td>
      </tr>
    );
  }
}

class ImportTableItem extends TableItem {
  constructor(props) {
    super(props)
  }

  render() {
    let table = this.props.table;
      return (
        <tr class="striped">
          <td style={this.ListLinkStyle}><Link to={"/table/"+table.Id}>{table.Id}</Link></td>
          <td style={this.ListColStyle}>{table.Name}</td>
          <td style={this.ListColStyle}>{table.FreqName}</td>
          <td style={this.ListColStyle}>{table.DispLabel} {this.formatConditionWeekdays(table.ReimportCond)}</td>
          <td style={this.ListColStyle}><TableStatus table={table} /></td>
          <td style={this.ListErrorStyle} id={"errorStatus_"+table.Id}><em> {table.Error} </em></td>
        </tr>
      );
  }
}

class ExternalTableItem extends TableItem {
  constructor(props) {
    super(props)
  }

  render() {
    let table = this.props.table;
    return (
      <tr class="striped">
        <td style={this.ListLinkStyle}><Link to={"/table/"+table.Id}>{table.Id}</Link></td>
        <td style={this.ListColStyle}>{table.Name}</td>
        <td style={this.ListColStyle}>{table.Dataset}</td>
        <td style={this.ListColStyle}>{table.Description}</td>
        <td style={this.ListColStyle}>{table.FreqName} {this.formatConditionWeekdays(table.Conditions)}</td>
        <td style={this.ListColStyle}>{table.DispLabel}</td>
        <td style={this.ListColStyle}><TableStatus table={table} /></td>
        <td style={this.ListErrorStyle} id={"errorStatus_"+table.Id}><em> {table.Error} </em></td>
      </tr>
    );
  }
}

class ImportDataset extends React.Component {
  constructor(props) {
    super(props)
    this.state = {open: false};
    this.HeaderStyle = {padding: "0 10px 0 0"};
  }

  render() {
    return (
      <table>
        <tbody>
          <tr>
            <th style={this.HeaderStyle} colSpan={2}>
              <Button onClick={() => this.setState({ open: !this.state.open })}>
              {this.state.open ? "⊖" : "⊕" }
              </Button>
              &nbsp;&nbsp;{this.props.dataset}
            </th> <td/> <td/> <td/> <td/> <td/> <td/>
          </tr>
          </tbody>
          <Collapse in={this.state.open}>
           <tbody>
             <tr onClick={this.props.sorter}>
               <th style={this.HeaderStyle}><a data-column="id">ID</a></th>
               <th style={this.HeaderStyle}><a data-column="name">Name</a></th>
               <th style={this.HeaderStyle}>Frequency</th>
               <th style={this.HeaderStyle}>Disposition</th>
               <th style={this.HeaderStyle}>Status</th>
               <th>Error</th>
             </tr>
           {this.props.tables}
           </tbody>
          </Collapse>
       </table>
    );
  }
}

class TableList extends React.Component {
  constructor(props) {
    super(props);
    this.sort = this.sort.bind(this);
    this.getData = this.getData.bind(this);
    this.state = { tables: [], sortColumn: "id", refresh: false };
    this.HeaderStyle = {padding: "0 10px 0 0"};
  }

  componentDidMount() {
    this.state.refresh = true;
    this.getData(10000);
  }

  componentWillUnmount() {
    this.state.refresh = false;
  }

  getFreqs() {
    return axios.get("/freqs").then((result) => result);
  }

  getTables() {
    return axios.get(`/tables?sort=${this.state.sortColumn}&order=asc&filter=${this.props.filter}`).then((result) => result);
  }

  getEverything() {
    return Promise.all([this.getFreqs(), this.getTables()]);
  }

  getData(again=0) {
      this.getEverything().then(([freqs, tables]) => {
          let ns = {};
          ns.Freqs = freqs.data;
          ns.tables = tables.data;
          this.setState(ns);
          if (this.state.refresh && again > 0) {
            setTimeout(this.getData, again, again);
          }
      });
  }

  sort(event) {
    if (!event.target.matches('a[data-column]')) { return; }
    let column = event.target.getAttribute('data-column');
    this.state.sortColumn = column;
    this.getData();
  }
}

class SummaryTableList extends TableList {
  constructor(props) {
    super(props)
  }

  render() {
    const tables = this.state.tables.map((table, i) => {
      let f = this.state.Freqs.find(f => f.Id == table.FreqId);
      table.FreqName = "Not Set";
      if (f) {
        table.FreqName = f.Name;
      }
      table.DispLabel = 'Replace';
      if (table.Disposition == 'WRITE_APPEND') {
        table.DispLabel = 'Append';
      }
      return (
        <SummaryTableItem key={i} table={table} />
      );
    });

    return (
      <div>
        <table><tbody>
          <tr onClick={this.sort} class="striped">
            <th style={this.HeaderStyle}><a data-column="id">ID</a></th>
            <th style={this.HeaderStyle}><a data-column="name">Name</a></th>
            <th style={this.HeaderStyle}>Dataset</th>
            <th style={this.HeaderStyle}>Description</th>
            <th style={this.HeaderStyle}>Frequency</th>
            <th style={this.HeaderStyle}>Disposition</th>
            <th style={this.HeaderStyle}>Status</th>
            <th>Error</th>
          </tr>
          {tables}
        </tbody></table>
      </div>
    );
  }
}

class ImportTableList extends TableList {
  constructor(props) {
    super(props);
  }

  render() {

    let datasets = [];
    let i = 0;
    let d = 0;
    let currentDataset = { dataset: "" };

    this.state.tables.map((table) => {
      i++;

      if (currentDataset.dataset !== table.Dataset) {
        if (currentDataset.dataset !== "") {
          d++;
          datasets.push(
            <ImportDataset key={d} dataset={currentDataset.dataset} tables={currentDataset.tables} sorter={this.sort}/>
          );
        }
        currentDataset = { dataset: table.Dataset, tables: [] };
      }

      let f = this.state.Freqs.find(f => f.Id == table.FreqId);
      table.FreqName = "Not Set";
      if (f) {
        table.FreqName = f.Name;
      }
      table.DispLabel = 'Full';
      if (table.IdColumn !== '') {
        table.DispLabel = 'Incremental';
      }

      currentDataset.tables.push (
        <ImportTableItem key={i} table={table} />
      );

    });

    // push the last dataset
    if (currentDataset.dataset !== "") {
      d++;
      datasets.push(
        <ImportDataset key={d} dataset={currentDataset.dataset} tables={currentDataset.tables} sorter={this.sort} />
      );
    }

    return (
      <div>
          {datasets}
      </div>
    );
  }
}

class ExternalTableList extends TableList {
  constructor(props) {
    super(props)
  }

  render() {
    const tables = this.state.tables.map((table, i) => {
      let f = this.state.Freqs.find(f => f.Id == table.FreqId);
      table.FreqName = "Not Set";
      if (f) {
        table.FreqName = f.Name;
      }
      table.DispLabel = 'Replace';
      if (table.Disposition == 'WRITE_APPEND') {
        table.DispLabel = 'Append';
      }
      return (
        <ExternalTableItem key={i} table={table} />
      );
    });

    return (
      <div>
        <table><tbody>
          <tr onClick={this.sort}>
            <th style={this.HeaderStyle}><a data-column="id">ID</a></th>
            <th style={this.HeaderStyle}><a data-column="name">Name</a></th>
            <th style={this.HeaderStyle}>Dataset</th>
            <th style={this.HeaderStyle}>Description</th>
            <th style={this.HeaderStyle}>Frequency</th>
            <th style={this.HeaderStyle}>Disposition</th>
            <th style={this.HeaderStyle}>Status</th>
            <th>Error</th>
          </tr>
          {tables}
        </tbody></table>
      </div>
    );
  }
}

// NB: exports *must* be at the end, or webpack build will fail (TODO why?)
export {SummaryTableList, ImportTableList, ExternalTableList};
