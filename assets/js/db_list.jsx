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
import { Button, Form, FormGroup, FormControl, Table, Modal } from 'react-bootstrap';

class DbEdit extends React.Component {
    constructor(props) {
        super(props);
        let db = {};
        if (props.db) {
            db = props.db;
        }
        this.defaultText = "NOT SHOWN";
        this.state = {show: false,
                      Id: db.Id,
                      Name: db.Name,
                      Dataset: db.Dataset,
                      Driver: db.Driver,
                      Export: db.Export,
                      ConnectStr: db.ConnectStr,
                      Secret: this.defaultText
                      };

        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleSaveClick = this.handleSaveClick.bind(this);
    }

    componentWillReceiveProps(nextProps) {
        this.setState({show: nextProps.show});
    }

    handleInputChange(event) {
        const target = event.target;
        const value = target.type === 'checkbox' ? target.checked : target.value;
        const name = target.name;
        let ns = { [name] : value };
        this.setState(ns);
    }

    handleSaveClick(event) {
        let data = {Id: this.state.Id,
                    Name: this.state.Name,
                    Driver: this.state.Driver,
                    Export: this.state.Export,
                    ConnectStr: this.state.ConnectStr,
                    Dataset: this.state.Dataset,
                    Secret: ""};

        if (this.defaultText != this.state.Secret) {
            data.secret = this.state.Secret;
        }
        axios.post("/admin/db_config", data)
            .then((result) => {
                console.log("POST OK " + toString(result));
                setTimeout(this.props.refresh, 1000);
                this.setState({show: false});
            })
            .catch((error) => {
                console.log("POST ERROR " + toString(error.message));
            });
    }

    render() {
        return (
          <Modal show={this.state.show}>
             <Modal.Body>

            <Form inline>
            <FormGroup>
                <Table>
                  <tr>
                    <th>Name:</th>
                    <td>
                    <FormControl name="Name" type="text" value={this.state.Name} onChange={this.handleInputChange} size="40"/>
                    </td>
                  </tr>
                  <tr>
                    <th>Dataset:</th>
                    <td>
                    <FormControl name="Dataset" type="text" value={this.state.Dataset} onChange={this.handleInputChange} size="40"/>
                    </td>
                  </tr>
                  <tr>
                    <th>Driver:</th>
                    <td>
                    <FormControl name="Driver" type="text" value={this.state.Driver} onChange={this.handleInputChange} size="40"/>
                    </td>
                  </tr>
                  <tr>
                    <td colspan={2}>
                    (must be 'postgres' or 'mysql')
                    </td>
                  </tr>
                  <tr>
                    <th>Export:</th>
                    <td>
                    <input name="Export" type="checkbox" checked={this.state.Export} onChange={this.handleInputChange} />
                    </td>
                  </tr>
                  <tr>
                    <th>Connection&nbsp;String:&nbsp;</th>
                    <td>
                    <FormControl name="ConnectStr" type="text" value={this.state.ConnectStr} onChange={this.handleInputChange} size="50"/>
                    </td>
                  </tr>
                  <tr>
                    <td colspan={2}>
                    ('%s' will be interpolated with the password below)
                    </td>
                  </tr>
                  <tr>
                    <th>Password:</th>
                    <td>
                    <FormControl name="Secret" type="text" value={this.state.Secret} onChange={this.handleInputChange} size="40"/>
                    </td>
                  </tr>
                  <tr>
                    <td colspan={2}>
                    (stored encrypted)
                    </td>
                  </tr>
                </Table>
            </FormGroup>
            </Form>


             </Modal.Body>
             <Modal.Footer>
                <Button onClick={this.handleSaveClick}>Save</Button>
                <Button onClick={() => this.setState({show:false})}>Cancel</Button>
             </Modal.Footer>
          </Modal>
        );
    }
}

class DbItem extends React.Component {
    constructor(props) {
        super(props);
        this.state = {Db: props.db, ShowEdit: false};
    }

    componentWillReceiveProps(nextProps) {
        this.setState({Db: nextProps.db, ShowEdit: false});
    }

    showEdit() {
        this.setState({ShowEdit: true});
    }

    render() {
        let db = this.state.Db;
        return (
       <tr>
        <td>{db.Id}</td>
        <td>{db.Name}</td>
        <td>{db.Dataset}</td>
        <td>{db.Driver}</td>
        <td>{db.Export ? 'true' : 'false'}</td>
        <td>
              <Button onClick={() => this.showEdit()}>Edit</Button>
        </td>

        <DbEdit show={this.state.ShowEdit} db={db} refresh={this.props.refresh}/>
      </tr>
        );
    }
}

class DbList extends React.Component {

    constructor(props) {
        super(props);
        this.state ={Dbs: [], ShowEdit: false};

        this.getData = this.getData.bind(this);
        this.showEdit = this.showEdit.bind(this);
    }

    componentDidMount() {
        this.getData();
    }

    getData() {
        axios.get('/dbs').then((result) => {
            this.setState({Dbs: result.data, ShowEdit: false});
        });
    }

    showEdit() {
        this.setState({ShowEdit: true});
        this.state.ShowEdit = false;
    }

    render() {
        const dbs = this.state.Dbs.map((db, i) => {
            return (
                <DbItem key={i} db={db} refresh={this.getData}/>
            );
        });
        return (
            <div>
                <Table striped bordered hover>
                <thead>
                <tr>
                  <th>Id</th>
                  <th>Name</th>
                  <th>Dataset</th>
                  <th>Driver</th>
                  <th>Export</th>
                </tr>
                </thead>
                <tbody>
                {dbs}
                </tbody>
                </Table>
                <Button onClick={this.showEdit}>Create</Button>
                <br/>
                <DbEdit show={this.state.ShowEdit} refresh={this.getData}/>
            </div>
        );
    }
}

// NB: exports *must* be at the end, or webpack build will fail (TODO why?)
export { DbList };
