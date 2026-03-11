package planner

import (
	"fmt"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
)

func BuildAllowedOperatorSet(cat OperatorCatalog) map[string]struct{} {
	out := make(map[string]struct{}, len(cat.Operators))
	for _, op := range cat.Operators {
		out[op.Type] = struct{}{}
	}
	return out
}

func ValidateOperatorTypes(d dsl.DAG, allowed map[string]struct{}) error {
	for _, n := range d.Nodes {
		if _, ok := allowed[n.Type]; !ok {
			return fmt.Errorf("node %s uses unsupported type %s", n.ID, n.Type)
		}
	}
	return nil
}
