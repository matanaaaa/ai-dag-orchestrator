package planner

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type strictDAG struct {
	DAGVersion *string      `json:"dag_version"`
	Nodes      []strictNode `json:"nodes"`
	Edges      []strictEdge `json:"edges"`
}

type strictNode struct {
	ID     *string         `json:"id"`
	Type   *string         `json:"type"`
	Params *map[string]any `json:"params"`
}

type strictEdge struct {
	From *string `json:"from"`
	To   *string `json:"to"`
}

func ValidateJSONSchema(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var in strictDAG
	if err := dec.Decode(&in); err != nil {
		return fmt.Errorf("dag schema validate: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("dag schema validate: extra trailing content")
	}

	if in.DAGVersion == nil {
		return fmt.Errorf("dag schema validate: missing required field dag_version")
	}
	if len(*in.DAGVersion) == 0 {
		return fmt.Errorf("dag schema validate: dag_version must be non-empty")
	}
	if len(in.Nodes) == 0 {
		return fmt.Errorf("dag schema validate: nodes must contain at least 1 item")
	}

	for i, n := range in.Nodes {
		if n.ID == nil {
			return fmt.Errorf("dag schema validate: nodes[%d].id is required", i)
		}
		if *n.ID == "" {
			return fmt.Errorf("dag schema validate: nodes[%d].id must be non-empty", i)
		}
		if n.Type == nil {
			return fmt.Errorf("dag schema validate: nodes[%d].type is required", i)
		}
		if *n.Type == "" {
			return fmt.Errorf("dag schema validate: nodes[%d].type must be non-empty", i)
		}
		if n.Params == nil {
			return fmt.Errorf("dag schema validate: nodes[%d].params is required", i)
		}
	}

	for i, e := range in.Edges {
		if e.From == nil {
			return fmt.Errorf("dag schema validate: edges[%d].from is required", i)
		}
		if *e.From == "" {
			return fmt.Errorf("dag schema validate: edges[%d].from must be non-empty", i)
		}
		if e.To == nil {
			return fmt.Errorf("dag schema validate: edges[%d].to is required", i)
		}
		if *e.To == "" {
			return fmt.Errorf("dag schema validate: edges[%d].to must be non-empty", i)
		}
	}

	return nil
}
