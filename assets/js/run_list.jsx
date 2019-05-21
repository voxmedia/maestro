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
import { DropdownButton, MenuItem, Collapse, Button, Table } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { withRouter } from 'react-router'
import { toString, findSurrogatePair } from 'js/utils.jsx';

import { Graph } from 'js/react-graph-vis.js';

class RunItem extends React.Component {
    constructor(props) {
        super(props)
        this.state = {
            Id: this.props.run_id,
            Data: null
        }

        this.doubleClickTime = 0;
        this.threshold = 200;

        this.clusterOptionsByLevel = {
            joinCondition:function(childOptions) {
                return childOptions.level == 0;
            },
            clusterNodeProperties: {
                id:'levelCluster',
                shape:'box',
                level: 0,
                widthConstraint: { maximum: 150 },
                label: 'unconnected tables'}
        };

        this.onClick = this.onClick.bind(this);
        this.doOnClick = this.doOnClick.bind(this);
        this.onDoubleClick = this.onDoubleClick.bind(this);
        this.getNetwork = this.getNetwork.bind(this);
    }

    componentDidMount() {
        this.getData()
    }

    fixupNodes(nodes) {
        for (var i = 0; i < nodes.length; i++) {
            let node = nodes[i];
            node.shadow = true;
            node.widthConstraint = { maximum: 150 };
            node.label = node.dataset + ' ' + node.table;
            let color = 'gold';
            let title = node.dataset + '.' + node.table;
            if (node.status == 'DONE') {
                color = 'lightgreen';
            }
            if (node.error !== '') {
                color = 'orangered';
                title += " <em style='color:red;'>"+node.error+"</em>";
            }
            node.title = title;
            node.color = color;

            if (node.orphan == false) {
                node.level = node.score + 1;
            } else {
                node.level = node.score;
            }
        }
    }

    getData() {
        axios.
            get("/run/"+this.state.Id+"/graph").then((result) => {
                let ns = {};
                ns.Data = result.data;
                this.fixupNodes(ns.Data.nodes);
                this.setState(ns);
            });
    }

    getNetwork(network) {
        this.network = network;
        network.cluster(this.clusterOptionsByLevel);
        network.fit();
        return network;
    }

    onClick(event) {
        // Delay the click by threshold. If in the meantime a
        // doubleClick happens, then do not do anything.
        var now = new Date();
        if (now - this.doubleClickTime > this.threshold) {
            setTimeout((event) => {
                if (now - this.doubleClickTime > this.threshold) {
                    this.doOnClick(event);
                }
            }, this.threshold, event);
        }
    }

    doOnClick(event) {
        var { nodes, edges } = event;

        if (nodes.length == 1) {
            if (this.network.isCluster(nodes[0]) == true) {
                this.network.openCluster(nodes[0]);
                this.network.fit();
            } else {
                let node = this.network.findNode(nodes[0]);
                this.setState(this.state); // re-render, as clustered
            }
        }
    }

    onDoubleClick(event) {
        this.doubleClickTime = new Date();
        this.props.history.push("/table/"+event.nodes[0]); // "redirect"
    }

    render () {

        const options = {
            interaction: {
                hover:true,
                navigationButtons: true,
                zoomView: false // this messes with the scroll
            },
            layout: {
                hierarchical: {
                    direction: 'DU',
                    sortMethod: 'directed',
                    parentCentralization: true
                }
            },
            edges: {
                color: "#000000",
                arrows: "to",
                smooth: {
                    type: "cubicBezier"
                }
            }
        };

        const events = {
            click: this.onClick,
            doubleClick: this.onDoubleClick
        };

        if (this.state.Data === null) {
            return ( <div>empty</div> );
        } else {

            return (
                 <div>
                  <small><em>double-click to open tables</em></small>
                  <Graph
                    graph={this.state.Data}
                    options={options}
                    events={events}
                    style={{ height: "400px" }}
                    getNetwork={this.getNetwork}/>
                </div>
            );
        }
    }
}

const RunItemWithRouter = withRouter(RunItem)

class RunList extends React.Component {
    constructor(props) {
        super(props);
        this.state = { Runs: [], Page: 0, refresh: false };
        this.expandedRows = {};

        this.getData = this.getData.bind(this);
        this.detailRow = this.detailRow.bind(this);
        this.handleExpand = this.handleExpand.bind(this);
        this.nextClick = this.nextClick.bind(this);
        this.prevClick = this.prevClick.bind(this);
        this.resumeClick = this.resumeClick.bind(this);
        this.endTimeFormatter = this.endTimeFormatter.bind(this);
    }

    componentDidMount() {
        this.state.refresh = true;
        this.getData(10000);
    }

    componentWillUnmount() {
        this.state.refresh = false;
    }

    getData(again=0) {
        axios.
            get("/runs?p="+this.state.Page).then((result) => {
                let ns = {};
                ns.Runs = result.data;
                this.setState(ns);
                if (this.state.refresh && again > 0) {
                    setTimeout(this.getData, again, again);
                }
            });
    }

    resumeRun(id) {
        axios.
            get("/run/"+id+"/resume").then(() => {
                this.getData()
            });
    }

    handleExpand(rowKey, isExpand) {
        this.expandedRows[rowKey] = isExpand;
    }

    detailRow(tr) {
        if (this.expandedRows[tr.Id]) {
            return ( <RunItemWithRouter run_id={tr.Id} /> );
        }
        return ( <div/> );
    }

    nextClick() {
        this.state.Page = this.state.Page+1;
        this.getData();
    }

    prevClick() {
        if (this.state.Page > 0) {
            this.state.Page = this.state.Page-1;
            this.getData();
        }
    }

    resumeClick(e, id) {
        e.stopPropagation();
        this.resumeRun(id);
    }

    endTimeFormatter(t, row) {
        if (t) {
            if (row.Error) {
                return(
                <div>
                    {t}<br/>
                    <Button onClick={(e) => this.resumeClick(e, row.Id)}>Resume</Button>
                </div>
                );
            }
            return t;
        }
        return ( <div>Running...</div> );
    }

    render() {

        const tableOptions = {
            expandRowBgColor: '#f9f9fc',
            expandBy: 'column',
            onExpand: this.handleExpand
        };

        const runs = this.state.Runs.map((run, i) => {
            run.Cost = Math.round((run.TotalBytes * (5.0 / (1024*1024*1024*1024))) * 1000) / 1000;
            return run;
        });

        return (
            <div>
              <BootstrapTable data={runs} striped hover options={tableOptions} expandableRow={() => true} expandComponent={this.detailRow}>
                <TableHeaderColumn isKey dataField='Id'>ID</TableHeaderColumn>
                <TableHeaderColumn dataField='FreqName'>Frequency</TableHeaderColumn>
                <TableHeaderColumn dataField='StartTime'>Start Time</TableHeaderColumn>
                <TableHeaderColumn dataField='EndTime' dataFormat={this.endTimeFormatter}>End Time</TableHeaderColumn>
                <TableHeaderColumn dataField='Cost'>Cost (USD)</TableHeaderColumn>
                <TableHeaderColumn dataField='Error'>Error</TableHeaderColumn>
              </BootstrapTable>
              <br/>
              <Button onClick={() => this.prevClick()} disabled={this.state.Page === 0}>Prev</Button>
              <Button onClick={() => this.nextClick()}>Next</Button>
            </div>
        );
    }
}

// NB: exports *must* be at the end, or webpack build will fail (TODO why?)
export { RunList };
