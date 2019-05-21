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
import { Button, DropdownButton, Form, FormControl, MenuItem, Modal, Table } from 'react-bootstrap';
import {toString, AlertDismissable} from 'js/utils.jsx';


class UserItem extends React.Component {
    constructor(props) {
        super(props);
        this.state = {User: props.user, Groups: props.groups};

        this.handleDisabled = this.handleDisabled.bind(this);
        this.handleAdmin = this.handleAdmin.bind(this);
        this.handleGroup = this.handleGroup.bind(this);
    }

    componentWillReceiveProps(nextProps) {
        this.state.Groups = nextProps.groups;
    }

    handleDisabled() {
        let user = this.state.User;
        user.Disabled = !user.Disabled;
        this.saveUser();
    }

    handleAdmin() {
        let user = this.state.User;
        user.Admin = !user.Admin;
        this.saveUser();
    }

    handleGroup(event) {
        const target = event.target;
        const checked = target.checked;
        const id = parseInt(target.name);
        if (checked) { // We adding a group
            this.state.User.Groups.push({Id: id});
        } else { // We are removing a group
            let groups = this.state.User.Groups;
            for (var i = 0; i < groups.length; i++) {
                if (groups[i].Id == id) {
                    this.state.User.Groups.splice(i, 1);
                }
            }
        }
        this.saveUser();
    }

    saveUser() {
        axios.put("/admin/user", this.state.User)
            .then((result) => {
                let user = JSON.parse(result.config.data);
                this.setState({User: result.data});
            })
            .catch((error) => {
                console.log("PUT ERROR " + JSON.stringify(error.message));
            });
    }

    groupContains(group, groups) {
        for (var i = 0; i < groups.length; i++) {
            if (groups[i].Id == group.Id) {
                return true;
            }
        }
        return false;
    }

    render() {
        const groups = this.state.Groups.map((group) => {
            return (
            <td>
            <Form inline>
            <label>
                <input name={group.Id}  type="checkbox" onChange={this.handleGroup} checked={this.groupContains(group, this.state.User.Groups)} />
            </label>
            </Form>
            </td>
            );
        });

        let user = this.state.User;
        let self = (this.props.self == user.Email);
        return (
       <tr>
        <td>{user.Id}</td>
        <td>{user.Email}</td>
        <td>
            <Form inline>
            <label>
                <input name="Disabled" disabled={self} type="checkbox" onChange={this.handleDisabled} checked={user.Disabled} />
            </label>
            </Form>
        </td>
        <td>
            <Form inline>
            <label>
                <input name="Admin" disabled={self} type="checkbox" onChange={this.handleAdmin} checked={user.Admin} />
            </label>
            </Form>
        </td>
        {groups}
      </tr>
        );
    }
}

class UserList extends React.Component {

    constructor(props) {
        super(props);
        this.state ={
            Users: [],
            User: {},
            Groups: [],
            Flash: "",
            DeleteModal: "",
            GroupModal: false,
            GroupName: "",
            GroupAdminId: null,
            GroupAdminEmail: "Make a Selection",
            GroupId: null};

        this.handleDeleteClick = this.handleDeleteClick.bind(this);
        this.handleGroupAdminSelect = this.handleGroupAdminSelect.bind(this);
        this.editGroup = this.editGroup.bind(this);
        this.newGroupModal = this.newGroupModal.bind(this);
    }

    componentDidMount() {
        this.getData();
        axios.get("/user/").then((resp) => {
            this.setState({ User: resp.data });
        });
    }

    getUsers() {
        return axios.get("/admin/users").then((result) => result);
    }
    getGroups() {
        return axios.get("/groups").then((result) => result);
    }
    getEverything() {
        return Promise.all([this.getUsers(), this.getGroups()])
    }

    getData() {
        this.getEverything().then(([users, groups]) => {
            let ns = {}; // new state
            ns.Users = users.data;
            ns.Groups = groups.data;
            this.setState(ns);
        });
    }

    handleDeleteClick(id) {
        this.setState({ Flash: "" });
        axios.delete("/admin/group/"+id)
            .then((result) => {
                console.log("DELETE OK " + toString(result));
                this.setState({ Flash: "Group deleted.", DeleteModal: "" });
                setTimeout(() => this.getData(), 1000);
            })
            .catch((error) => {
                if (error.response && error.response.data && error.response.data.error) {
                    console.log("DELETE ERROR " + error.response.data.error);
                    this.setState({ Flash: "Error: " + error.response.data.error, DeleteModal: ""});
                } else {
                    console.log("DELETE ERROR " + toString(error.message));
                    this.setState({ Flash: "Error deleting group", DeleteModal: ""});
                }
            });


    }

    handleGroupAdminSelect(e) {
        let ns = {GroupAdminId: e};

        let users = this.state.Users;
        for (var i = 0; i < users.length; i++) {
            if (users[i].Id == e) {
                ns.GroupAdminEmail = users[i].Email;
                break;
            }
        }

        this.setState(ns);
    }

    handleSaveGroup() {

        if (this.state.GroupId != null) { // Existing group
            let group = {Name: this.state.GroupName, AdminUserId: this.state.GroupAdminId, Id: this.state.GroupId};
            axios.put("/admin/group", group)
                .then((result) => {
                    console.log("PUT OK " + toString(result));
                this.getData();
                    this.setState({Flash: "Group updated.", GroupModal: false});
                })
                .catch((error) => {
                    console.log("PUT ERROR " + toString(error.message));
                    this.setState({ Flash: "Error updating group: "+error.response.data});
                });
        } else { // New group
            let group = {Name: this.state.GroupName, AdminUserId: this.state.GroupAdminId};
            axios.post("/admin/group", group)
                .then((result) => {
                    console.log("POST OK " + toString(result));
                this.getData();
                    this.setState({Flash: "Group created.", GroupModal: false});
                })
                .catch((error) => {
                    console.log("POST ERROR " + toString(error.message));
                    this.setState({ Flash: "Error creating group: "+error.response.data});
                });
        }
    }

    editGroup(group) {

        let users = this.state.Users;
        for (var i = 0; i < users.length; i++) {
            if (users[i].Id == group.AdminUserId) {
                this.state.GroupAdminEmail = users[i].Email;
            }
        }
        this.setState({ GroupModal: true,
                        GroupName: group.Name,
                        GroupAdminId: group.AdminUserId,
                        GroupId: group.Id });
    }

    newGroupModal() {
        this.setState({ GroupModal: true,
                        Flash: "",
                        GroupName: "",
                        GroupAdminUserId: null,
                        GroupAdminEmail: "Make a selection" });

    }

    render() {
        const users = this.state.Users.map((user, i) => {
            return (
                <UserItem key={i} user={user} self={this.state.User.Email} groups={this.state.Groups}/>
            );
        });
        const groups = this.state.Groups.map((group, i) => {
            return (
                    <th>{group.Name}
                    &nbsp;&nbsp;&nbsp;<Button onClick={() => this.setState({DeleteModal: group.Name})}>Delete Group</Button>
                    &nbsp;&nbsp;&nbsp;<Button onClick={() => this.editGroup(group)}>Edit Group</Button>

                      <Modal show={this.state.DeleteModal == group.Name}>
                      <Modal.Body>Deleting group &quot;{group.Name}&quot;. Are you sure?</Modal.Body>
                      <Modal.Footer>
                        <Button onClick={() => this.setState({ DeleteModal: "" })}>Cancel</Button>
                        <Button onClick={() => this.handleDeleteClick(group.Id)} bsStyle="danger">Yes, delete!</Button>
                      </Modal.Footer>
                      </Modal>
                     </th>
                   );
        });
        return (
            <div>
                <AlertDismissable message={this.state.Flash} show={this.state.Flash !== ""} />

          <Modal show={this.state.GroupModal}>
             <Modal.Body>
                <h4>Group detail:</h4>
                <Form inline>
                  <label>
                    Name:&nbsp;
                    <FormControl name="Name" type="text" value={this.state.GroupName} onChange={(e) => {this.setState({GroupName: event.target.value})}} size="40"/>
                  </label>
                <br/>
                  <label>
                     Group Admin:&nbsp;
                     <DropdownButton title={this.state.GroupAdminEmail} onSelect={this.handleGroupAdminSelect}>
                      { this.state.Users.map((u) => {
                         return <MenuItem eventKey={u.Id} active={this.state.GroupAdminId == u.Id}>{u.Email}</MenuItem>;
                     })}
                    </DropdownButton>
                  </label>
                  <br/>
                  (Group Admin should be able to add or remove users from the group, but it is not yet implemented. TODO)
                </Form>
             </Modal.Body>
             <Modal.Footer>
                <Button onClick={() => this.setState({GroupModal: false})}>Cancel</Button>
                <Button disabled={this.state.GroupName == ""} onClick={() => this.handleSaveGroup()} bsStyle="success">Save Group</Button>
             </Modal.Footer>
          </Modal>

                <Table striped bordered hover>
                <thead>
                <tr>
                  <th>Id</th>
                  <th>Email</th>
                  <th>Disabled</th>
                  <th>Admin</th>
                  <th colspan={this.state.Groups.length}>Groups
                    &nbsp;&nbsp;&nbsp;<Button onClick={this.newGroupModal}>Create New Group</Button>
                  </th>
                </tr>
                <tr>
                  <td colspan={4}></td>
                  {groups}
                </tr>
                </thead>
                <tbody>
                {users}
                </tbody>
                </Table>
            </div>
        );
    }
}

// NB: exports *must* be at the end, or webpack build will fail (TODO why?)
export { UserList };
