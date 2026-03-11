package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

type OpenAICompatibleChatCompletionsProvider struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func NewOpenAICompatibleChatCompletionsProvider(baseURL, apiKey string) *OpenAICompatibleChatCompletionsProvider {
	return &OpenAICompatibleChatCompletionsProvider{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 40 * time.Second},
	}
}

func (p *OpenAICompatibleChatCompletionsProvider) GenerateDAG(
	ctx context.Context,
	developerPrompt string,
	userPrompt string,
	schema any,
	model string,
) (string, PlanMeta, error) {
	start := time.Now()

	// DashScope OpenAI-compatible uses /chat/completions. :contentReference[oaicite:1]{index=1}
	url := p.BaseURL + "/chat/completions"

	// 注意：为了最大兼容（包括千问），developer prompt 用 role=system 更稳。
	// （你在 Responses 里用的 developer role，这里改 system）
	reqBody := map[string]any{
		"model": model,
		"messages": []map[string]any{
			{"role": "system", "content": developerPrompt},
			{"role": "user", "content": userPrompt},
		},
		// 结构化输出（JSON Schema）用 response_format。:contentReference[oaicite:2]{index=2}
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "dag_v1",
				"strict": true,
				"schema": schema,
			},
		},
	}

	b, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.HTTP.Do(req)
	if err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()},
			errors.New("compatible http " + resp.Status + " body=" + string(body))
	}

	// Chat Completions: choices[0].message.content
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, err
	}
	if len(out.Choices) == 0 || out.Choices[0].Message.Content == "" {
		return "", PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()},
			errors.New("compatible empty choices[0].message.content")
	}

	return out.Choices[0].Message.Content, PlanMeta{Model: model, LatencyMs: time.Since(start).Milliseconds()}, nil
}