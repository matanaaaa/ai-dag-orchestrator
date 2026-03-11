package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
)

type Planner struct {
	LLM       LLMProvider
	MaxRepair int
	Model     string
	SchemaAny any
	DevPrompt string
	AllowedOperatorTypes map[string]struct{}
}

// Plan：调用 LLM 生成 JSON -> 解析为 DAG -> 语义校验（无环/引用）
// 失败则把错误 + 上次输出喂回去修复
func (p *Planner) Plan(ctx context.Context, userRequest string) (dsl.DAG, PlanMeta, error) {
	var lastErr error
	var lastOut string
	var meta PlanMeta

	for round := 0; round <= p.MaxRepair; round++ {
		userPrompt := userRequest
		if round > 0 {
			userPrompt = buildRepairUserPrompt(userRequest, lastOut, lastErr)
		}

		out, m, err := p.LLM.GenerateDAG(ctx, p.DevPrompt, userPrompt, p.SchemaAny, p.Model)
		meta = m
		meta.RepairRounds = round

		if err != nil {
			lastErr = err
			continue
		}
		lastOut = out

		var dag dsl.DAG
		clean := sanitizeToJSON(out)
		lastOut = clean
		if err := json.Unmarshal([]byte(clean), &dag); err != nil {
			lastErr = fmt.Errorf("dag json unmarshal: %w", err)
			continue
		}

		if _, err := dsl.Validate(dag); err != nil {
			lastErr = fmt.Errorf("dag semantic validate: %w", err)
			continue
		}
		if len(p.AllowedOperatorTypes) > 0 {
			if err := ValidateOperatorTypes(dag, p.AllowedOperatorTypes); err != nil {
				lastErr = fmt.Errorf("dag operator validate: %w", err)
				continue
			}
		}
		return dag, meta, nil
	}

	return dsl.DAG{}, meta, lastErr
}

func buildRepairUserPrompt(userRequest, lastOut string, lastErr error) string {
	hint := repairHint(lastErr)
	if hint != "" {
		hint = "\n" + hint + "\n"
	}

	return userRequest + "\n\n" +
		"Your previous DAG JSON is invalid.\n" +
		"Validation error:\n" + lastErr.Error() + hint + "\n\n" +
		"Previous output:\n" + lastOut + "\n\n" +
		"Please output a corrected DAG JSON that strictly matches the schema and uses only allowed operators."
}

func repairHint(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "duplicate node id"):
		return "Hint: rename one of the duplicated node.id values and update any edges that reference it."
	case strings.Contains(s, "edge from missing node"):
		return "Hint: either add the missing source node, or change edge.from to an existing node id."
	case strings.Contains(s, "edge to missing node"):
		return "Hint: either add the missing target node, or change edge.to to an existing node id."
	case strings.Contains(s, "cycle detected"):
		return "Hint: remove or redirect one edge to break the cycle while keeping the workflow intent."
	default:
		return ""
	}
}

func sanitizeToJSON(s string) string {
	// 常见情况：```json ... ``` 或 ``` ... ```
	trim := strings.TrimSpace(s)

	if strings.HasPrefix(trim, "```") {
		trim = strings.TrimPrefix(trim, "```")
		trim = strings.TrimSpace(trim)

		// 去掉可能的 "json"
		if strings.HasPrefix(strings.ToLower(trim), "json") {
			trim = strings.TrimSpace(trim[4:])
		}

		// 找到结尾 ```
		if idx := strings.LastIndex(trim, "```"); idx >= 0 {
			trim = strings.TrimSpace(trim[:idx])
		}
	}

	// 再做一层兜底：截取第一个 '{' 到最后一个 '}'（适配前后有解释文字）
	if i := strings.Index(trim, "{"); i >= 0 {
		if j := strings.LastIndex(trim, "}"); j > i {
			return strings.TrimSpace(trim[i : j+1])
		}
	}

	return trim
}