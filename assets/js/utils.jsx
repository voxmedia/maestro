// -*- Javascript -*-

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
import { Alert, Button } from 'react-bootstrap';

// A stringify helper which can deal with cyclic refs
// Super useful in console.log(toStr(whatever));
let toString = function(obj) {
    let seen = [];
    return JSON.stringify(obj, function(key, val) {
        if (val != null && typeof val == "object") {
            if (seen.indexOf(val) >= 0) {
                return;
            }
            seen.push(val);
        }
        return val;
    });
}

// To make emoji work, see
// http://stackoverflow.com/questions/35142493/how-can-i-write-emoji-characters-to-a-textarea
let findSurrogatePair = function(point) {
  var offset = point - 0x10000,
      lead = 0xd800 + (offset >> 10),
      trail = 0xdc00 + (offset & 0x3ff);
  return String.fromCharCode(lead) + String.fromCharCode(trail);
}


// Dismissable Alert
class AlertDismissable extends React.Component {
    constructor(props, context) {
        super(props, context);

        this.handleDismiss = this.handleDismiss.bind(this);

        this.state = {
            show: this.props.show
        };
    }

    handleDismiss() {
        this.setState({ show: false });
    }

    componentWillReceiveProps(nextProps) {
        this.setState({ show: nextProps.show });
    }

    render() {
        let style = this.props.style
        if (style == null || style === "") {
            style = (this.props.message.includes("rror ") || this.props.message.includes("rror:")) ? "danger" : "success";
        }
        if (this.state.show) {
            return (
        <Alert bsStyle={style} onDismiss={this.handleDismiss}>
          <h4>
          {this.props.message}
          </h4>
        </Alert>
            );
        }
        return ( <span/> );
    }
}


function isJson(item) {
    item = typeof item !== "string"
        ? JSON.stringify(item)
        : item;

    try {
        item = JSON.parse(item);
    } catch (e) {
        return false;
    }

    if (typeof item === "object" && item !== null) {
        return true;
    }

    return false;
}

export {toString, isJson, findSurrogatePair, AlertDismissable};
