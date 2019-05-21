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

import { Button, Modal } from 'react-bootstrap';

class UserDetail extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            User: {},
            Token: "",
            TokenWarnModal: false,
            TokenShowModal: false
        };

        this.handleTokenShowClick = this.handleTokenShowClick.bind(this);
    }

    handleTokenShowClick() {
        axios.get("/user/token").then((resp) => {
            this.setState( { TokenShowModal: true, Token: resp.data.api_token } );
        });
        this.setState({ TokenWarnModal: false });
    }

    componentDidMount() {
        axios.get("/user/").then((resp) => {
            this.setState({ User: resp.data });
        });
    }

    render() {
        return (
        <div>
          <h2>User detail for {this.state.User.Email}</h2>

          <p>An API token provides access to Maestro API without OAuth authentication via a bearer token.</p>

          <p>Requesting an API token will trigger an alert to the administrators.</p>

          <p>The token MUST be passed in as <tt>X-Api-Token</tt> HTTP header. Example usage:
             <pre>curl -v -H 'X-Api-Token: token_value_here' https://maestro.example.com/table/1</pre>
          </p>

          <Button onClick={() => this.setState({ TokenWarnModal: true })}>Request or see your API Token</Button>

          <Modal show={this.state.TokenWarnModal}>
             <Modal.Body>This will produce an alert to the administrators, are you sure?</Modal.Body>
             <Modal.Footer>
                <Button onClick={() => this.setState({ TokenWarnModal: false })}>Cancel</Button>
                <Button onClick={this.handleTokenShowClick} bsStyle="danger">Proceed</Button>
             </Modal.Footer>
          </Modal>


          <Modal show={this.state.TokenShowModal}>
             <Modal.Body>
                This is your API token, keep it secure and never share it with anyone!
                <p/>
                <pre>{this.state.Token}</pre>
            </Modal.Body>
             <Modal.Footer>
                <Button onClick={() => this.setState({ TokenShowModal: false })}>OK</Button>
             </Modal.Footer>
          </Modal>



        </div>
        );
    }
}

// NB: exports *must* be after declaration, or webpack build will fail
export {UserDetail};
