package operators

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type OpContext struct {
	JobID   string
	NodeID  string
	DataDir string
	Upstream map[string]string // node_id -> output_text
}

type Operator interface {
	Type() string
	Run(ctx context.Context, params map[string]any, opCtx OpContext) (outputText string, artifactURI string, err error)
}

type Registry struct {
	m map[string]Operator
}

func NewRegistry(ops ...Operator) *Registry {
	m := map[string]Operator{}
	for _, op := range ops {
		m[op.Type()] = op
	}
	return &Registry{m: m}
}

func (r *Registry) Get(t string) (Operator, bool) {
	op, ok := r.m[t]
	return op, ok
}

// --- download ---
type Download struct{}

func (Download) Type() string { return "download" }
func (Download) Run(ctx context.Context, params map[string]any, opCtx OpContext) (string, string, error) {
	if v, ok := params["text"].(string); ok {
		return v, "", nil
	}
	return "hello world", "", nil
}

// --- transform_upper ---
type TransformUpper struct{}

func (TransformUpper) Type() string { return "transform_upper" }
func (TransformUpper) Run(ctx context.Context, params map[string]any, opCtx OpContext) (string, string, error) {
	// 取任一上游输出（P0 简化：只有一个上游）
	for _, v := range opCtx.Upstream {
		return strings.ToUpper(v), "", nil
	}
	return "", "", errors.New("no upstream output")
}

// --- upload_local ---
type UploadLocal struct{}

func (UploadLocal) Type() string { return "upload_local" }
func (UploadLocal) Run(ctx context.Context, params map[string]any, opCtx OpContext) (string, string, error) {
	name, _ := params["filename"].(string)
	if name == "" {
		name = fmt.Sprintf("%s_%s.txt", opCtx.JobID, opCtx.NodeID)
	}
	outDir := filepath.Join(opCtx.DataDir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", err
	}
	path := filepath.Join(outDir, name)

	// 同样取任一上游输出
	var content string
	for _, v := range opCtx.Upstream {
		content = v
		break
	}
	if content == "" {
		return "", "", errors.New("no upstream output")
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", "", err
	}
	return content, "file://" + path, nil
}