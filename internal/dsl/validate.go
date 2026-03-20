package dsl

import (
	"errors"
	"fmt"
)

type Validated struct {
	NodeByID map[string]Node
	Adj      map[string][]string
	Indegree map[string]int
	Topo     []string
}

func Validate(d DAG) (Validated, error) {
	if len(d.Nodes) == 0 {
		return Validated{}, errors.New("dag.nodes empty")
	}

	nodeByID := map[string]Node{}
	for _, n := range d.Nodes {
		if n.ID == "" || n.Type == "" {
			return Validated{}, fmt.Errorf("node id/type empty: %+v", n)
		}
		if _, ok := nodeByID[n.ID]; ok {
			return Validated{}, fmt.Errorf("duplicate node id: %s", n.ID)
		}
		if n.Params == nil {
			n.Params = map[string]any{}
		}
		nodeByID[n.ID] = n
	}

	adj := map[string][]string{}
	indeg := map[string]int{}
	for id := range nodeByID {
		adj[id] = []string{}
		indeg[id] = 0
	}

	type edgeKey struct {
		From string
		To   string
	}
	seenEdges := map[edgeKey]struct{}{}

	for _, e := range d.Edges {
		if e.From == "" || e.To == "" {
			return Validated{}, fmt.Errorf("edge from/to empty: %+v", e)
		}
		if _, ok := nodeByID[e.From]; !ok {
			return Validated{}, fmt.Errorf("edge from missing node: %s", e.From)
		}
		if _, ok := nodeByID[e.To]; !ok {
			return Validated{}, fmt.Errorf("edge to missing node: %s", e.To)
		}

		k := edgeKey{From: e.From, To: e.To}
		if _, ok := seenEdges[k]; ok {
			return Validated{}, fmt.Errorf("duplicate edge: %s -> %s", e.From, e.To)
		}
		seenEdges[k] = struct{}{}
		
		adj[e.From] = append(adj[e.From], e.To)
		indeg[e.To]++
	}

	// Kahn topo sort
	q := make([]string, 0, len(indeg))
	for id, deg := range indeg {
		if deg == 0 {
			q = append(q, id)
		}
	}
	topo := make([]string, 0, len(indeg))
	for len(q) > 0 {
		id := q[0]
		q = q[1:]
		topo = append(topo, id)
		for _, to := range adj[id] {
			indeg[to]--
			if indeg[to] == 0 {
				q = append(q, to)
			}
		}
	}

	if len(topo) != len(nodeByID) {
		return Validated{}, errors.New("cycle detected in dag")
	}

	// recompute indegree (topo mutated indeg to 0)
	indeg2 := map[string]int{}
	for id := range nodeByID {
		indeg2[id] = 0
	}
	for _, e := range d.Edges {
		indeg2[e.To]++
	}

	return Validated{
		NodeByID: nodeByID,
		Adj:      adj,
		Indegree: indeg2,
		Topo:     topo,
	}, nil
}