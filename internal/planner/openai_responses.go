package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
	"strings"
)

type PlanMeta struct {
	Model        string
	LatencyMs    int64
	RepairRounds int
}

type LLMProvider interface {
	GenerateDAG(ctx context.Context, developerPrompt string, userPrompt string, schema any, model string) (dagJSON string, meta PlanMeta, err error)
}

type OpenAIResponsesProvider struct {
	BaseURL string
	HTTP    *http.Client
}

func NewOpenAIResponsesProvider(baseURL string) *OpenAIResponsesProvider {
	return &OpenAIResponsesProvider{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 40 * time.Second},
	}
}



func (p *OpenAIResponsesProvider) GenerateDAG(ctx context.Context, developerPrompt string, userPrompt string, schema any, model string) (string, PlanMeta, error) {
	start := time.Now()

	reqBody := map[string]any{
		"model": model,
		"input": []map[string]any{
			{"role": "developer", "content": developerPrompt},
			{"role": "user", "content": userPrompt},
		},
		"text": map[string]any{
			"format": map[string]any{
				"type":   "json_schema",
				"name":   "dag_v1",
				"strict": true,
				"schema": schema,
			},
		},
	}

	b, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.HTTP.Do(req)
	if err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		// 把错误 body 带上，方便你 debug
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, errors.New("openai http " + resp.Status + " body=" + string(body))
	}

	// Responses API 返回结构较丰富；这里我们优先读 output_text（很多响应会有这个字段）
	var out struct {
		OutputText string `json:"output_text"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	if out.OutputText == "" {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, errors.New("openai empty output_text")
	}
	return out.OutputText, PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, nil
}