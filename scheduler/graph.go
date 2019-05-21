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
	"fmt"
	"sort"
)

type Item interface {
	GetName() string
}

type node struct {
	Item
	children, parents map[string]*node
	Score             int
}

func (n *node) Parents() map[string]*node  { return n.parents }
func (n *node) Children() map[string]*node { return n.children }

func newNode(t Item) *node {
	return &node{
		Item:     t,
		children: make(map[string]*node),
		parents:  make(map[string]*node),
	}
}

func (n *node) addParent(p *node) {
	if n.parents[p.GetName()] == nil {
		n.parents[p.GetName()] = p
	}
}

func (n *node) addChild(c *node) {
	if n.children[c.GetName()] == nil {
		n.children[c.GetName()] = c
	}
}

// A graph. Implemented as a map of nodes, keyed by string
// representing the name of this node. A node maintains maps of its
// parents and children, as well as has an integer Score which
// represents the "importance" of this node based on how many
// dependents it has.
type Graph map[string]*node

func NewGraph() Graph {
	return make(Graph)
}

func (g Graph) addItem(t Item) *node {
	if g[t.GetName()] == nil {
		g[t.GetName()] = newNode(t)
	}
	return g[t.GetName()]
}

func (g Graph) getItem(name string) *node {
	return g[name]
}

// Remove a node given its name.
func (g Graph) RemoveItem(name string) error {
	if node, ok := g[name]; ok {
		if len(node.parents) > 0 {
			return fmt.Errorf("Node has parents, cannot be removed.")
		}
		for _, child := range node.children {
			delete(child.parents, name)
		}
		delete(g, name)
	}
	return nil
}

// Add a parent-child pair to the graph. A nil parent means this
// child has no parent.
func (g Graph) Relate(parent, child Item) {
	c := g.addItem(child)
	if parent != nil {
		p := g.addItem(parent)
		c.addParent(p)
		p.addChild(c)
	}
}

// Breadth-first (pre-order) up traversal
func (g Graph) bftUp(start *node, action func(Item)) {
	queue := make(nodeStack, 0)
	seen := make(map[string]bool)

	queue.enqueue(start)
	for len(queue) > 0 {
		if n := queue.pop(); n != nil && !seen[n.GetName()] {
			nn := g.getItem(n.GetName())
			action(nn.Item)
			seen[nn.GetName()] = true
			for _, p := range sortedNodes(nn.parents) {
				queue.enqueue(p)
			}
		}
	}
}

// Depth-first (pre-order) up traversal
// TODO: Do we need it?
func (g Graph) dftUp(start *node, action func(Item)) {
	stack := make(nodeStack, 0)
	seen := make(map[string]bool)

	stack.push(start)
	for len(stack) > 0 {
		if n := stack.pop(); n != nil && !seen[n.GetName()] {
			nn := g.getItem(n.GetName())
			action(nn.Item)
			seen[nn.GetName()] = true
			for _, p := range sortedNodesReverse(nn.parents) {
				stack.push(p)
			}
		}
	}
}

// Breadth-first (pre-order) down traversal
func (g Graph) bftDown(start *node, action func(Item)) {
	queue := make(nodeStack, 0)
	seen := make(map[string]bool)

	queue.enqueue(start)
	for len(queue) > 0 {
		if n := queue.pop(); n != nil && !seen[n.GetName()] {
			nn := g.getItem(n.GetName())
			action(nn.Item)
			seen[nn.GetName()] = true
			for _, p := range sortedNodes(nn.children) {
				queue.enqueue(p)
			}
		}
	}
}

const csName = ""

type sentinel struct{}

func (*sentinel) GetName() string { return csName }

func (g Graph) childSentinel() *node {
	if _, ok := g[csName]; !ok {
		ct := &sentinel{}
		for name, node := range g {
			if name != csName && len(node.children) == 0 {
				g.Relate(node.Item, ct)
			}
		}
	}
	return g[csName]
}

type scoreName struct {
	Score int
	Name  string
}

// Traverse (breadth-first) the entire graph bottom up, stopping at
// every node to traverse (breadth-first) from that node down to count
// its progeny. The count is then the score. Scores array is ordered
// by score.
func (g Graph) Scores() []scoreName {
	result := make([]scoreName, 0, len(g))
	g.bftUp(g.childSentinel(), func(t Item) {
		n := -2 // self + sentinel == 2
		g.bftDown(g.getItem(t.GetName()), func(_ Item) { n += 1 })
		if t.GetName() != csName {
			result = append(result, scoreName{n, t.GetName()})
		}
	})
	sort.SliceStable(result, func(i, j int) bool { return result[i].Score > result[j].Score })
	return result
}

// Return a slice of items which have no parents, meaning these items
// are not waiting on the parents to finish executing and are ready to
// run.
func (g Graph) ReadyItems() (result []Item, err error) {
	for _, sn := range g.Scores() {
		node := g.getItem(sn.Name)
		if len(node.parents) == 0 {
			result = append(result, node.Item)
		}
	}
	// Note: there may still be a cycle, as well as runnable tables,
	// in which case it is not detected until the runnable tables
	// are removed.
	if len(g) > 1 && len(result) == 0 { // 1 for sentinel
		err = fmt.Errorf("Cycle detected.")
	}
	return
}

func sortedNodes(nmap map[string]*node) []*node {
	names := make([]string, len(nmap))
	for name, _ := range nmap {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]*node, len(names))
	for _, name := range names {
		result = append(result, nmap[name])
	}
	return result
}

func sortedNodesReverse(nmap map[string]*node) []*node {
	names := make([]string, len(nmap))
	for name, _ := range nmap {
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	result := make([]*node, len(names))
	for _, name := range names {
		result = append(result, nmap[name])
	}
	return result
}

type nodeStack []*node

// stack push
func (q *nodeStack) push(n *node) {
	*q = append(nodeStack{n}, *q...)
}

// queue push
func (q *nodeStack) enqueue(n *node) {
	*q = append(*q, n)
}

func (q *nodeStack) pop() (n *node) {
	if len(*q) == 0 {
		return nil
	}
	n, *q = (*q)[0], (*q)[1:]
	return n
}
