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
import { Button, Checkbox, Collapse, Panel, Well } from 'react-bootstrap';

import {toString} from 'js/utils.jsx';

class Weekdays extends React.Component {

    constructor(props) {
        super(props);
        this.handleChange = this.handleChange.bind(this);
    }

    handleChange(weekday) {
        let days = this.props.value.slice();
        let i = days.indexOf(weekday);
        if (i == -1) {
            days.push(weekday); // add
        } else {
            days.splice(i, 1);  // remove
        }
        days.sort();
        this.props.onChange(days);
    }

    render() {
        return (
            <span>
                <Checkbox inline checked={this.props.value.includes(1)} onChange={() => this.handleChange(1)} disabled={this.props.disabled}>Mon</Checkbox>
                <Checkbox inline checked={this.props.value.includes(2)} onChange={() => this.handleChange(2)} disabled={this.props.disabled}>Tue</Checkbox>
                <Checkbox inline checked={this.props.value.includes(3)} onChange={() => this.handleChange(3)} disabled={this.props.disabled}>Wed</Checkbox>
                <Checkbox inline checked={this.props.value.includes(4)} onChange={() => this.handleChange(4)} disabled={this.props.disabled}>Thu</Checkbox>
                <Checkbox inline checked={this.props.value.includes(5)} onChange={() => this.handleChange(5)} disabled={this.props.disabled}>Fri</Checkbox>
                <Checkbox inline checked={this.props.value.includes(6)} onChange={() => this.handleChange(6)} disabled={this.props.disabled}>Sat</Checkbox>
                <Checkbox inline checked={this.props.value.includes(0)} onChange={() => this.handleChange(0)} disabled={this.props.disabled}>Sun</Checkbox>
            </span>
        );
    }
}

class Conditions extends React.Component {

    // TODO This component is a hack, it only deals with a single
    // condition. The right way would be to have a Condition
    // (singular) component.

    constructor(props) {
        super(props)
        this.state = {
            Conditions: [{days:[], hours:[], months:[], weekdays:[]}]
        };
        this.handleWeekdaysChange = this.handleWeekdaysChange.bind(this);
    }

    handleWeekdaysChange(days) {
        this.state.Conditions[0].weekdays = days;
        this.props.onChange(this.state.Conditions);
    }

    render() {
        if (this.props.visible) {
            let cond = this.props.value[0];
            if (typeof cond === 'undefined') {
                cond = { weekdays: [] }
            }
            return (
                    <span>
                    {this.props.weekdayslabel} <Weekdays value={cond.weekdays} onChange={this.handleWeekdaysChange} disabled={this.props.disabled}/>
                    </span>
            );
        } else {
            return ( <span></span> );
        }
    }
}

export {Conditions};
