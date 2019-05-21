/* Copyright 2019 Vox Media, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

package scheduler

import (
	"reflect"
	"testing"
)

// dummy table for testing
type table struct {
	name string
}

func (t *table) GetName() string {
	return t.name
}

func Test_newNode(t *testing.T) {
	tb := new(table)
	n := newNode(tb)
	if n.parents == nil || n.children == nil || n.Item != tb {
		t.Error(`n.parents == nil || n.children == nil || n.Item != tb`)
	}
}

func Test_node_addParent(t *testing.T) {
	tb1 := &table{name: "foo"}
	tb2 := &table{name: "bar"}
	child := newNode(tb1)
	parent := newNode(tb2)
	child.addParent(parent)
	if child.parents[tb2.GetName()] != parent {
		t.Error(`child.parents[tb2.GetName()] != parent`)
	}
}

func Test_node_addChild(t *testing.T) {
	tb1 := &table{name: "foo"}
	tb2 := &table{name: "bar"}
	child := newNode(tb1)
	parent := newNode(tb2)
	parent.addChild(child)
	if parent.children[tb1.GetName()] != child {
		t.Error(`parent.children[tb1.GetName()] != child`)
	}
}

func Test_graph_addItem(t *testing.T) {
	g := make(Graph)
	tb1 := &table{name: "foo"}
	g.addItem(tb1)
	if g[tb1.GetName()].Item != tb1 {
		t.Error(`g[tb1.GetName()].Item != tb1`)
	}
	tb2 := &table{name: "foo"}
	g.addItem(tb2) // should be a noop
	if g["foo"].Item != tb1 {
		t.Error(`g["foo"].Item != tb1`)
	}
}

func Test_graph_Relate(t *testing.T) {
	g := make(Graph)
	parent := &table{name: "parent"}
	child := &table{name: "child"}
	g.Relate(parent, child)
	if g["child"].parents["parent"].Item != parent {
		t.Error(`g["child"].parents["parent"].Item != parent`)
	}
	if g["parent"].children["child"].Item != child {
		t.Error(`g["parent"].children["child"].Item != child`)
	}
}

func makeGraph() Graph {
	//       g00   g01
	//        ^     ^
	//        |     |
	//        +-g10-+-g11
	//           ^     ^-------
	//           |            |
	//       g20 + g21 + g22  |
	//        ^      ^   ^    |
	//       g30      g31     |
	//        ^        ^      |
	//        +----+---+------+
	//             ^
	//          child sentinel

	// How many tables depend on me? (aka "score"):
	// {"g21":1, "g22":1, "g10":5, "g11":0, "g00":6, "g01":7, "g30":0, "g31":0, "g20":1}

	g00 := &table{name: "g00"} // generation 0
	g01 := &table{name: "g01"}
	g10 := &table{name: "g10"} // generation 1
	g11 := &table{name: "g11"}
	g20 := &table{name: "g20"} // generation 2
	g21 := &table{name: "g21"}
	g22 := &table{name: "g22"}
	g30 := &table{name: "g30"} // generation 3
	g31 := &table{name: "g31"}

	g := NewGraph()
	g.Relate(g00, g10)
	g.Relate(g01, g10)
	g.Relate(g01, g11)
	g.Relate(g10, g20)
	g.Relate(g10, g21)
	g.Relate(g10, g22)
	g.Relate(g20, g30)
	g.Relate(g21, g31)
	g.Relate(g22, g31)

	return g
}

func Test_graph_traversal(t *testing.T) {

	g := makeGraph()

	var bftup []string
	g.bftUp(g.getItem("g31"), func(t Item) { bftup = append(bftup, t.GetName()) })
	if !reflect.DeepEqual(bftup, []string{"g31", "g21", "g22", "g10", "g00", "g01"}) {
		t.Error("Incorrect BFT UP:", bftup)
	}

	var dftup []string
	g.dftUp(g.getItem("g30"), func(t Item) { dftup = append(dftup, t.GetName()) })
	if !reflect.DeepEqual(dftup, []string{"g30", "g20", "g10", "g00", "g01"}) {
		t.Error("Incorrect DFT UP:", dftup)
	}

	var bftdown []string
	g.bftDown(g.getItem("g00"), func(t Item) { bftdown = append(bftdown, t.GetName()) })
	if !reflect.DeepEqual(bftdown, []string{"g00", "g10", "g20", "g21", "g22", "g30", "g31"}) {
		t.Error("Incorrect BFT DOWN:", bftdown)
	}

	cs := g.childSentinel()
	if cs.parents["g30"] == nil || cs.parents["g31"] == nil {
		t.Errorf(`cs.parents["g30"] == nil || cs.parents["g31"] == nil`)
	}

	// Lets make a cycle - it should still work because "seen" takes care of it
	g.Relate(g["g31"].Item, g["g00"].Item)

	var cycle []string
	g.bftDown(g.getItem("g00"), func(t Item) { cycle = append(cycle, t.GetName()) })
	if !reflect.DeepEqual(cycle, []string{"g00", "g10", "g20", "g21", "g22", "g30", "g31", ""}) {
		t.Error("Incorrect cycle traversal:", cycle)
	}
}

func Test_graph_scores(t *testing.T) {
	g := makeGraph()
	scores := g.scores()
	expect := []scoreName{{7, "g01"}, {6, "g00"}, {5, "g10"}, {1, "g20"}, {1, "g21"}, {1, "g22"}, {0, "g11"}, {0, "g30"}, {0, "g31"}}
	if !reflect.DeepEqual(scores, expect) {
		t.Errorf("Incorrect scores: %v", scores)
	}
}

func Test_graph_readyItems(t *testing.T) {
	g := makeGraph()
	var result []string
	ready, _ := g.ReadyItems()
	for _, t := range ready {
		result = append(result, t.GetName())
	}
	if !reflect.DeepEqual(result, []string{"g01", "g00"}) {
		t.Errorf("Incorrect ready tables: %v", result)
	}
}

func Test_graph_removeItem(t *testing.T) {
	g := makeGraph()

	if err := g.RemoveItem("g30"); err == nil {
		t.Errorf("Removing nodes with parents should not be allowed")
	}

	if err := g.RemoveItem("g01"); err != nil {
		t.Errorf("Removing g01 should be ok.")
	} else {
		var bftup []string
		g.bftUp(g.getItem("g31"), func(t Item) { bftup = append(bftup, t.GetName()) })
		if !reflect.DeepEqual(bftup, []string{"g31", "g21", "g22", "g10", "g00"}) {
			t.Error("Incorrect BFT UP after removal:", bftup)
		}
	}
}

// A cycle can only be detected by following the whole run: eventually
// we will run into a condition when the graph has tables, but none
// are runnable.
func Test_graph_cycle(t *testing.T) {
	g := makeGraph()
	// make a cycle
	g.Relate(g["g31"].Item, g["g00"].Item)
	ready, err := g.ReadyItems()
	for ; len(ready) > 0 && err == nil; ready, err = g.ReadyItems() {
		//fmt.Printf("-- Starting next cycle, scores: %v\n", g.scores())
		for _, t := range ready {
			//fmt.Printf("Running %#v\n", t.Name)
			g.RemoveItem(t.GetName())
		}
	}
	if err == nil {
		t.Error("Cycle not detected")
	}
}
