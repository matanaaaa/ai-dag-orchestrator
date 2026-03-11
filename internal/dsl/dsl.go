package dsl

import (
	"fmt"
	"strings"
)

type DAG struct {
	DAGVersion string `json:"dag_version"`
	Nodes      []Node `json:"nodes"`
	Edges      []Edge `json:"edges"`
}

type Node struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Params map[string]any         `json:"params"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (d *DAG) ToDOT() string {
    var b strings.Builder

    b.WriteString("digraph DAG {\n")

    // nodes
    for _, n := range d.Nodes {
        fmt.Fprintf(&b, `  %s [label="%s\n(%s)"];`+"\n",
            n.ID, n.ID, n.Type)
    }

    // edges
    for _, e := range d.Edges {
        fmt.Fprintf(&b, "  %s -> %s;\n", e.From, e.To)
    }

    b.WriteString("}\n")

    return b.String()
}