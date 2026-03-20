package operators

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrInvalidParams    = errors.New("invalid operator params")
	ErrNoUpstreamOutput = errors.New("no upstream output")
	ErrAmbiguousInput   = errors.New("ambiguous upstream input")
	ErrUnsafeFilename   = errors.New("unsafe filename")
)

type OpContext struct {
	JobID    string
	NodeID   string
	Attempt  int
	WorkerID string
	DataDir  string

	// Upstream carries already-materialized upstream outputs.
	// Node execution is protected by orchestrator/worker claim + attempt checks,
	// so operators should treat this context as immutable input for one claimed attempt.
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
		t := strings.TrimSpace(op.Type())
		if t == "" {
			panic("operator type is empty")
		}
		if _, exists := m[t]; exists {
			panic("duplicate operator type: " + t)
		}
		m[t] = op
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
	if err := checkContext(ctx); err != nil {
		return "", "", err
	}
	if err := validateAllowedParams(params, "text"); err != nil {
		return "", "", err
	}

	v, ok := getOptionalString(params, "text")
	if !ok {
		return "", "", fmt.Errorf("%w: text must be string", ErrInvalidParams)
	}
	if v == "" {
		return "hello world", "", nil
	}
	return v, "", nil
}

// --- transform_upper ---
type TransformUpper struct{}

func (TransformUpper) Type() string { return "transform_upper" }
func (TransformUpper) Run(ctx context.Context, params map[string]any, opCtx OpContext) (string, string, error) {
	if err := checkContext(ctx); err != nil {
		return "", "", err
	}
	if err := validateAllowedParams(params, "input_from"); err != nil {
		return "", "", err
	}
	v, err := resolveInputText(params, opCtx.Upstream)
	if err != nil {
		return "", "", err
	}
	return strings.ToUpper(v), "", nil
}

// --- upload_local ---
type UploadLocal struct{}

func (UploadLocal) Type() string { return "upload_local" }
func (UploadLocal) Run(ctx context.Context, params map[string]any, opCtx OpContext) (string, string, error) {
	if err := checkContext(ctx); err != nil {
		return "", "", err
	}
	if err := validateAllowedParams(params, "filename", "input_from"); err != nil {
		return "", "", err
	}
	name, ok := getOptionalString(params, "filename")
	if !ok {
		return "", "", fmt.Errorf("%w: filename must be string", ErrInvalidParams)
	}
	if name == "" {
		name = fmt.Sprintf("%s_%s_attempt_%d.txt", opCtx.JobID, opCtx.NodeID, opCtx.Attempt)
	}
	name, err := sanitizeFilename(name)
	if err != nil {
		return "", "", err
	}
	outDir := filepath.Join(opCtx.DataDir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", err
	}
	path := filepath.Join(outDir, name)

	content, err := resolveInputText(params, opCtx.Upstream)
	if err != nil {
		return "", "", err
	}
	if err := checkContext(ctx); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", "", err
	}
	return content, "file://" + path, nil
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func getOptionalString(params map[string]any, key string) (string, bool) {
	if params == nil {
		return "", true
	}
	v, ok := params[key]
	if !ok {
		return "", true
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(s), true
}

func firstUpstreamOutput(upstream map[string]string) (string, error) {
	if len(upstream) == 0 {
		return "", ErrNoUpstreamOutput
	}
	keys := make([]string, 0, len(upstream))
	for k := range upstream {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if upstream[k] != "" {
			return upstream[k], nil
		}
	}
	return "", ErrNoUpstreamOutput
}

func resolveInputText(params map[string]any, upstream map[string]string) (string, error) {
	inputFrom, ok := getOptionalString(params, "input_from")
	if !ok {
		return "", fmt.Errorf("%w: input_from must be string", ErrInvalidParams)
	}
	if inputFrom != "" {
		v, exists := upstream[inputFrom]
		if !exists || v == "" {
			return "", fmt.Errorf("%w: upstream %q not found", ErrNoUpstreamOutput, inputFrom)
		}
		return v, nil
	}
	if len(upstream) == 1 {
		return firstUpstreamOutput(upstream)
	}
	if len(upstream) == 0 {
		return "", ErrNoUpstreamOutput
	}
	return "", fmt.Errorf("%w: specify input_from when multiple upstream outputs exist", ErrAmbiguousInput)
}

func validateAllowedParams(params map[string]any, allowed ...string) error {
	if len(params) == 0 {
		return nil
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, k := range allowed {
		allow[k] = struct{}{}
	}
	for k := range params {
		if _, ok := allow[k]; !ok {
			return fmt.Errorf("%w: unexpected param %q", ErrInvalidParams, k)
		}
	}
	return nil
}

func sanitizeFilename(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: empty filename", ErrUnsafeFilename)
	}
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("%w: absolute path not allowed", ErrUnsafeFilename)
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: path traversal not allowed", ErrUnsafeFilename)
	}
	if strings.ContainsAny(clean, `/\:`) {
		return "", fmt.Errorf("%w: nested path not allowed", ErrUnsafeFilename)
	}
	upper := strings.ToUpper(clean)
	base := strings.TrimSuffix(upper, filepath.Ext(upper))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return "", fmt.Errorf("%w: reserved filename", ErrUnsafeFilename)
	}
	return clean, nil
}
