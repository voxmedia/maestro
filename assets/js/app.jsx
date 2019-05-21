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
import ReactDOM from 'react-dom';
import { Button, Nav, Navbar, NavDropdown, MenuItem, NavItem } from 'react-bootstrap';
import { LinkContainer } from 'react-router-bootstrap';
import { HashRouter, Link, Route, Switch } from "react-router-dom";

import { SummaryTableList, ImportTableList, ExternalTableList } from 'js/table_list.jsx';
import { RunList } from 'js/run_list.jsx';
import { TableDetail } from 'js/table_detail.jsx';
import { UserDetail } from 'js/user_detail.jsx';
import { UserList } from 'js/user_list.jsx';
import { Home } from 'js/home.jsx';
import { Creds } from 'js/creds.jsx';
import { DbList } from 'js/db_list.jsx';
import { FreqList } from 'js/freq_list.jsx';

import {toString, findSurrogatePair} from 'js/utils.jsx';

class SummaryTablePanel extends React.Component {
    render() {
        return (
        <div>
          <Button href="/#/table/new" bsStyle="primary">Create New Summary Table</Button>
          <h2>Summary Table List</h2>
          <SummaryTableList filter="bq"/>
        </div>
        );
    }
}

class ImportDatasetPanel extends React.Component {
    render() {
        return (
        <div>
          <Button href="/#/table/new_import" bsStyle="primary">Create New Import Table</Button>
          <h2>Import Table List</h2>
          <ImportTableList filter="import"/>
        </div>
        );
    }
}

class ExternalDatasetPanel extends React.Component {
    render() {
        return (
        <div>
          <Button href="/#/table/new_external" bsStyle="primary">Create New External Table</Button>
          <h2>External Table List</h2>
          <ExternalTableList filter="external"/>
        </div>
        );
    }
}

class RunsPanel extends React.Component {
    render() {
        return (
        <div>
          <h2>Recent Run List</h2>
          <RunList />
        </div>
        );
    }
}

class AdminUsersPanel extends React.Component {
    render() {
        return (
        <div>
          <h2>User Administration</h2>
          <UserList />
        </div>
        );
    }
}

class AdminCredsPanel extends React.Component {
    render() {
        return (
        <div>
          <Creds/>
        </div>
        );
    }
}

class AdminDbsPanel extends React.Component {
    render() {
        return (
        <div>
          <DbList/>
        </div>
        );
    }
}

class AdminFreqsPanel extends React.Component {
    render() {
        return (
        <div>
          <FreqList/>
        </div>
        );
    }
}

class Help extends React.Component {
    render() {
        return (
        <div>
          <h2>Help</h2>
          <p>
                See <a href="https://godoc.org/github.com/voxmedia/maestro">Godoc</a> for documentation.
          </p>
        </div>
        );
    }
}

class Header extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            User: {}
        };
    }

    componentDidMount() {
        axios.get("/user/").then((resp) => {
            this.setState({ User: resp.data });
        });
    }

    render() {
        return (
          <Navbar fluid collapseOnSelect>
            <Navbar.Header>
              <Navbar.Brand>
                <Link to="/">{findSurrogatePair(0x1F3BB)}</Link>
              </Navbar.Brand>
              <Navbar.Toggle />
            </Navbar.Header>
            <Navbar.Collapse>
              <Nav>
                <LinkContainer to="/summary">
                  <NavItem>Summary Tables</NavItem>
                </LinkContainer>
                <LinkContainer to="/imports">
                  <NavItem>Import Tables</NavItem>
                </LinkContainer>
                <LinkContainer to="/externals">
                  <NavItem>External Tables</NavItem>
                </LinkContainer>
              </Nav>
              <Nav pullRight>
                <LinkContainer to="/runs">
                  <NavItem>Runs</NavItem>
                </LinkContainer>
             { this.state.User.Admin ?
                <NavDropdown title="Admin">
                  <LinkContainer to="/admin/users">
                    <MenuItem>Users</MenuItem>
                  </LinkContainer>
                  <LinkContainer to="/admin/creds">
                    <MenuItem>Credentials</MenuItem>
                  </LinkContainer>
                  <LinkContainer to="/admin/dbs">
                    <MenuItem>Databases</MenuItem>
                  </LinkContainer>
                  <LinkContainer to="/admin/freqs">
                    <MenuItem>Frequencies</MenuItem>
                  </LinkContainer>
                </NavDropdown>
               : '' }
                <LinkContainer to="/help">
                  <NavItem>Help</NavItem>
                </LinkContainer>
                <LinkContainer to="/user">
                  <NavItem>{this.state.User.Email}</NavItem>
                </LinkContainer>
              </Nav>
          </Navbar.Collapse>
        </Navbar>
        );
    }
}

class Routes extends React.Component {
    render() {
        return (
        <Switch>
          <Route path="/" exact component={Home} />
          <Route path="/summary" component={SummaryTablePanel} />
          <Route path="/table/:id" component={TableDetail}/>
          <Route path="/imports" component={ImportDatasetPanel}/>
          <Route path="/externals" component={ExternalDatasetPanel}/>
          <Route path="/runs" component={RunsPanel}/>
          <Route path="/help" component={Help}/>
          <Route path="/user" component={UserDetail}/>
          <Route path="/admin/users" component={AdminUsersPanel}/>
          <Route path="/admin/creds" component={AdminCredsPanel}/>
          <Route path="/admin/dbs" component={AdminDbsPanel}/>
          <Route path="/admin/freqs" component={AdminFreqsPanel}/>
        </Switch>
        );
    }
}

class App extends React.Component {
    render() {
        return (
      <HashRouter>
        <div>
          <Header/>
          <Routes/>
        </div>
      </HashRouter>
   );
  }
}

// Start the app
ReactDOM.render( <App/>, document.querySelector("#root"));
