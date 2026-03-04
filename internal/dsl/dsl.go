package dsl

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