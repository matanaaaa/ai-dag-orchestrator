package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

type OpenAICompatibleResponsesProvider struct {
	Endpoint string
	APIKey   string
	HTTP     *http.Client
}

func NewOpenAICompatibleResponsesProvider(endpoint, apiKey string) *OpenAICompatibleResponsesProvider {
	return &OpenAICompatibleResponsesProvider{
		Endpoint: endpoint,
		APIKey:   apiKey,
		HTTP:     &http.Client{Timeout: 40 * time.Second},
	}
}

func (p *OpenAICompatibleResponsesProvider) GenerateDAG(ctx context.Context, developerPrompt string, userPrompt string, schema any, model string) (string, PlanMeta, error) {
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
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(b))
	if p.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.HTTP.Do(req)
	if err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, errors.New("llm http " + resp.Status + " body=" + string(body))
	}

	var out struct {
		OutputText string `json:"output_text"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	if out.OutputText == "" {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, errors.New("llm empty output_text")
	}
	return out.OutputText, PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, nil
}
