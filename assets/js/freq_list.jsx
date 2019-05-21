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

class FreqEdit extends React.Component {
    constructor(props) {
        super(props);
        let freq = {};
        if (props.freq) {
            freq = props.freq;
        }
        this.defaultText = "NOT SHOWN";
        this.state = {show: false,
                      Id: freq.Id,
                      Name: freq.Name,
                      Period: freq.Period/1000000000,
                      Offset: freq.Offset/1000000000,
                      Active: freq.Active
                      };

        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleSaveClick = this.handleSaveClick.bind(this);
    }

    componentWillReceiveProps(nextProps) {
        let ns = {show: nextProps.show};
        this.setState(ns);
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
                    Period: parseInt(this.state.Period, 10),
                    Offset: parseInt(this.state.Offset, 10),
                    Active: this.state.Active
                    };

        axios.post("/admin/freq", data)
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
                    <th>Period:</th>
                    <td>
                    <FormControl name="Period" type="text" value={this.state.Period} onChange={this.handleInputChange} size="40" type="number" step="1"/>
                    </td>
                  </tr>
                  <tr>
                    <th>Offset:</th>
                    <td>
                    <FormControl name="Offset" type="text" value={this.state.Offset} onChange={this.handleInputChange} size="40" type="number" step="1"/>
                    </td>
                  </tr>
                  <tr>
                    <td colspan={2}>
                    (Period and Offset are in seconds)
                    </td>
                  </tr>
                  <tr>
                    <th>Active:</th>
                    <td>
                    <input name="Active" type="checkbox" checked={this.state.Active} onChange={this.handleInputChange} />
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

class FreqItem extends React.Component {
    constructor(props) {
        super(props);
        this.state = {Freq: props.freq, ShowEdit: false};
    }

    componentWillReceiveProps(nextProps) {
        this.setState({Freq: nextProps.freq, ShowEdit: false});
    }

    showEdit() {
        this.setState({ShowEdit: true});
    }

    render() {
        let f = this.state.Freq;
        return (
       <tr>
        <td>{f.Id}</td>
        <td>{f.Name}</td>
        <td>{f.Period/1000000000}</td>
        <td>{f.Offset/1000000000}</td>
        <td>{f.Active ? 'true' : 'false'}</td>
        <td>
              <Button onClick={() => this.showEdit()}>Edit</Button>
        </td>

        <FreqEdit show={this.state.ShowEdit} freq={f} refresh={this.props.refresh}/>
      </tr>
        );
    }
}

class FreqList extends React.Component {

    constructor(props) {
        super(props);
        this.state ={Freqs: [], ShowEdit: false};

        this.getData = this.getData.bind(this);
        this.showEdit = this.showEdit.bind(this);
    }

    componentDidMount() {
        this.getData();
    }

    getData() {
        axios.get('/freqs').then((result) => {
            this.setState({Freqs: result.data, ShowEdit: false});
        });
    }

    showEdit() {
        this.setState({ShowEdit: true});
        this.state.ShowEdit = false;
    }

    render() {
        const freqs = this.state.Freqs.map((f, i) => {
            return (
                <FreqItem key={i} freq={f} refresh={this.getData}/>
            );
        });
        return (
            <div>
                <Table striped bordered hover>
                <thead>
                <tr>
                  <th>Id</th>
                  <th>Name</th>
                  <th>Period</th>
                  <th>Offset</th>
                  <th>Active</th>
                </tr>
                </thead>
                <tbody>
                {freqs}
                </tbody>
                </Table>
                <Button onClick={this.showEdit}>Create</Button>
                <br/>
                <FreqEdit show={this.state.ShowEdit} refresh={this.getData}/>
            </div>
        );
    }
}

// NB: exports *must* be at the end, or webpack build will fail (TODO why?)
export { FreqList };
