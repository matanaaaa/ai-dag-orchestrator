package planner

import "encoding/json"

type OperatorCatalog struct {
	Version   string `json:"version"`
	Operators []struct {
		Type        string         `json:"type"`
		Description string         `json:"description"`
		Params      map[string]any `json:"params"`
	} `json:"operators"`
}

func BuildDeveloperPrompt(cat OperatorCatalog) string {
	b, _ := json.MarshalIndent(cat, "", "  ")
	return "You are a planner that converts a user request into a DAG JSON.\n" +
		"Rules:\n" +
		"1) Output MUST be valid JSON and MUST match the provided JSON schema strictly.\n" +
		"2) node.type MUST be one of the allowed operators from this catalog (do not invent types).\n" +
		"3) Use minimal nodes/edges needed.\n" +
		"4) Nodes must have unique ids. Edges must reference existing node ids.\n\n" +
		"Operator catalog (whitelist):\n" + string(b)
}