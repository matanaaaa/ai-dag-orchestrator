package kafka

import "github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"

type Envelope[T any] struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	TSMs    int64  `json:"ts_ms"`
	TraceID string `json:"trace_id"`

	JobID   string `json:"job_id"`
	NodeID  string `json:"node_id,omitempty"`
	Attempt int    `json:"attempt,omitempty"`

	Payload T `json:"payload"`
}

const (
	TypeJobSubmit     = "JOB_SUBMIT"
	TypePlanResult    = "PLAN_RESULT"
	TypeNodeDispatch  = "NODE_DISPATCH"
	TypeNodeStatus    = "NODE_STATUS"
	TypeNodeRetry     = "NODE_RETRY"
	TypeNodeDLQ       = "NODE_DLQ"
)

type JobSubmitPayload struct {
	UserRequest    string `json:"user_request"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type PlanResultPayload struct {
	OK  bool   `json:"ok"`
	DAG dsl.DAG `json:"dag"`
	Err string `json:"err,omitempty"`

	PlannerModel     string `json:"planner_model,omitempty"`
    PlannerLatencyMs int64  `json:"planner_latency_ms,omitempty"`
    RepairRounds     int    `json:"repair_rounds,omitempty"`
}

type NodeDispatchPayload struct {
	Node dsl.Node `json:"node"`
	// P0：把上游输出直接带上，避免 worker 查 DB
	Context struct {
		UpstreamOutputs map[string]string `json:"upstream_outputs,omitempty"` // node_id -> output_text
	} `json:"context"`
}

type NodeStatusPayload struct {
	Status string `json:"status"` // STARTED/SUCCEEDED/FAILED
	Worker struct {
		ID   string `json:"id"`
		Host string `json:"host,omitempty"`
	} `json:"worker"`
	OutputText  string `json:"output_text,omitempty"`
	ArtifactURI string `json:"artifact_uri,omitempty"`
	ErrorMsg    string `json:"error_msg,omitempty"`
}

type NodeRetryPayload struct {
	Node        dsl.Node `json:"node"`
	NotBeforeMs int64    `json:"not_before_ms"`
	Reason      string   `json:"reason,omitempty"`
	Context     struct {
		UpstreamOutputs map[string]string `json:"upstream_outputs,omitempty"`
	} `json:"context"`
}

type NodeDLQPayload struct {
	Node         dsl.Node `json:"node"`
	FinalAttempt int      `json:"final_attempt"`
	Reason       string   `json:"reason"`
	Context      struct {
		UpstreamOutputs map[string]string `json:"upstream_outputs,omitempty"`
	} `json:"context"`
}
