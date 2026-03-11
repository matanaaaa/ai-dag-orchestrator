package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"strings"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/observability"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
)

type Engine struct {
	Store          *store.Store
	Prod           *kafka.Producer
	Topics         kafka.Topics
	MaxRetry       int
	RetryBackoffMs int64
	Metrics        *observability.Registry
}

func (e *Engine) HandlePlanResult(ctx context.Context, msgValue []byte) error {
	var env kafka.Envelope[kafka.PlanResultPayload]
	if err := json.Unmarshal(msgValue, &env); err != nil {
		return err
	}

	pm := env.Payload.PlannerModel
	pl := env.Payload.PlannerLatencyMs
	pr := env.Payload.RepairRounds

	if !env.Payload.OK {
		_ = e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "planner failed", FailedReason: env.Payload.Err,
			PlannerModel: pm, PlannerLatencyMs: pl, RepairRounds: pr,
		})
		return nil
	}

	v, err := dsl.Validate(env.Payload.DAG)
	if err != nil {
		_ = e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "invalid dag", FailedReason: err.Error(),
			PlannerModel: pm, PlannerLatencyMs: pl, RepairRounds: pr,
		})
		return nil
	}

	_ = e.Store.InsertJob(ctx, store.Job{
		JobID: env.JobID, TraceID: env.TraceID, Status: "RUNNING", UserRequest: "(from submit)",
		PlannerModel: pm, PlannerLatencyMs: pl, RepairRounds: pr,
	})

	for id, n := range v.NodeByID {
		deg := v.Indegree[id]
		_ = e.Store.InsertNode(ctx, env.JobID, n.ID, n.Type, n.Params, "PENDING", deg)
	}
	for _, ed := range env.Payload.DAG.Edges {
		_ = e.Store.InsertEdge(ctx, env.JobID, ed.From, ed.To)
	}

	ready, err := e.Store.ListReadyNodes(ctx, env.JobID)
	if err != nil {
		return err
	}
	for _, nodeID := range ready {
		if err := e.dispatchNode(ctx, env.JobID, env.TraceID, nodeID); err != nil {
			return err
		}
	}

	_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
		EventID: util.NewID("ev"),
		JobID:   env.JobID,
		Status:  "PLAN_ACCEPTED",
		TSMs:    time.Now().UnixMilli(),
	})
	return nil
}

func (e *Engine) HandleNodeStatus(ctx context.Context, msgValue []byte) error {
	var env kafka.Envelope[kafka.NodeStatusPayload]
	if err := json.Unmarshal(msgValue, &env); err != nil {
		return err
	}
	if env.Attempt <= 0 {
		env.Attempt = 1
	}

	switch env.Payload.Status {
	case "STARTED":
		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "RUNNING")
		_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
			EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "STARTED",
			Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID,
		})
		return nil

	case "SUCCEEDED":
		current, _ := e.Store.GetNodeStatus(ctx, env.JobID, env.NodeID)
		if current == "SUCCEEDED" {
			return nil
		}

		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "SUCCEEDED")
		_ = e.Store.UpdateNodeAttemptStatus(ctx, env.JobID, env.NodeID, env.Attempt, "SUCCEEDED", "")
		_ = e.Store.UpsertNodeResult(ctx, env.JobID, env.NodeID, env.Payload.OutputText, env.Payload.ArtifactURI, "")
		_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
			EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "SUCCEEDED",
			Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID,
			OutputText: env.Payload.OutputText, ArtifactURI: env.Payload.ArtifactURI,
		})

		down, err := e.Store.ListDownstream(ctx, env.JobID, env.NodeID)
		if err != nil {
			return err
		}
		for _, to := range down {
			justReady, err := e.Store.UnlockDownstreamEdge(ctx, env.JobID, env.NodeID, to)
			if err != nil {
				return err
			}
			if justReady {
				if err := e.dispatchNode(ctx, env.JobID, env.TraceID, to); err != nil {
					return err
				}
			}
		}

		total, err := e.Store.CountAllNodes(ctx, env.JobID)
		if err != nil {
			return err
		}
		succ, err := e.Store.CountNodesByStatus(ctx, env.JobID, "SUCCEEDED")
		if err != nil {
			return err
		}
		if total > 0 && succ == total {
			_ = e.Store.UpdateJobStatus(ctx, env.JobID, "SUCCEEDED")
			_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
				EventID: util.NewID("ev"),
				JobID:   env.JobID,
				Status:  "JOB_SUCCEEDED",
				TSMs:    time.Now().UnixMilli(),
			})
		}
		return nil

	case "FAILED":
		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "FAILED")
		_ = e.Store.UpdateNodeAttemptStatus(ctx, env.JobID, env.NodeID, env.Attempt, "FAILED", env.Payload.ErrorMsg)
		_ = e.Store.UpsertNodeResult(ctx, env.JobID, env.NodeID, "", "", env.Payload.ErrorMsg)
		_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
			EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "FAILED",
			Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID, ErrorMsg: env.Payload.ErrorMsg,
		})

		if env.Attempt < e.MaxRetry {
			nextAttempt := env.Attempt + 1
			retryPayload, err := e.buildRetryPayload(ctx, env.JobID, env.NodeID)
			if err != nil {
				return err
			}
			retryPayload.NotBeforeMs = time.Now().Add(time.Duration(e.RetryBackoffMs) * time.Millisecond).UnixMilli()
			retryPayload.Reason = env.Payload.ErrorMsg

			retryEnv := kafka.Envelope[kafka.NodeRetryPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeRetry,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
				Attempt: nextAttempt,
				Payload: retryPayload,
			}
			if err := e.Prod.Publish(e.Topics.Retry, env.JobID, retryEnv); err != nil {
				return err
			}
			if e.Metrics != nil {
				e.Metrics.IncCounter("worker_retry_total", nil, 1)
			}
			_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
				EventID: util.NewID("ev"), JobID: env.JobID, NodeID: env.NodeID,
				Status: "RETRY_SCHEDULED", Attempt: nextAttempt, TSMs: time.Now().UnixMilli(), ErrorMsg: env.Payload.ErrorMsg,
			})
			return nil
		}
		// terminal failure -> trigger replanning as a child job (agent loop: observe -> replan)
		failedNodeType, _, _ := e.Store.GetNodeTypeAndParams(ctx, env.JobID, env.NodeID)
		childJobID, replanErr := e.triggerReplanAsChildJob(ctx, env.JobID, env.TraceID, env.NodeID, failedNodeType, env.Payload.ErrorMsg)
		dlqPayload, err := e.buildDLQPayload(ctx, env.JobID, env.NodeID, env.Attempt, env.Payload.ErrorMsg)
		if err == nil {
			dlqEnv := kafka.Envelope[kafka.NodeDLQPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeDLQ,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
				Attempt: env.Attempt,
				Payload: dlqPayload,
			}
			_ = e.Prod.Publish(e.Topics.DLQ, env.JobID, dlqEnv)
		}

		if replanErr != nil {
			_ = e.Store.UpdateJobFailed(ctx, env.JobID, env.Payload.ErrorMsg+" | replan_error="+replanErr.Error())
			_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
				EventID:  util.NewID("ev"),
				JobID:    env.JobID,
				Status:   "JOB_FAILED",
				TSMs:     time.Now().UnixMilli(),
				ErrorMsg: env.Payload.ErrorMsg + " | replan_error=" + replanErr.Error(),
			})
			return nil
		}
		if childJobID == "" {
			// idempotent path: replan already triggered before
			return nil
		}
		return nil
	}
	return fmt.Errorf("unknown status: %s", env.Payload.Status)
}

func (e *Engine) dispatchNode(ctx context.Context, jobID, traceID, nodeID string) error {
	claimed, err := e.Store.TryMarkNodeReady(ctx, jobID, nodeID)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	payload, err := e.buildDispatchPayload(ctx, jobID, nodeID)
	if err != nil {
		return err
	}

	ev := kafka.Envelope[kafka.NodeDispatchPayload]{
		EventID: util.NewID("ev"),
		Type:    kafka.TypeNodeDispatch,
		TSMs:    time.Now().UnixMilli(),
		TraceID: traceID,
		JobID:   jobID,
		NodeID:  nodeID,
		Attempt: 1,
		Payload: payload,
	}
	if err := e.Prod.Publish(e.Topics.Dispatch, jobID, ev); err != nil {
		return err
	}
	if e.Metrics != nil {
		e.Metrics.IncCounter("orchestrator_dispatch_total", nil, 1)
	}
	_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
		EventID: ev.EventID, JobID: jobID, NodeID: nodeID, Status: "DISPATCHED", Attempt: 1, TSMs: ev.TSMs,
	})
	return nil
}

func (e *Engine) buildDispatchPayload(ctx context.Context, jobID, nodeID string) (kafka.NodeDispatchPayload, error) {
	nodeType, params, err := e.Store.GetNodeTypeAndParams(ctx, jobID, nodeID)
	if err != nil {
		return kafka.NodeDispatchPayload{}, err
	}

	upOut := map[string]string{}
	rows, err := e.Store.DB.QueryContext(ctx,
		`SELECT from_node FROM job_edges WHERE job_id=? AND to_node=?`, jobID, nodeID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var from string
			if err := rows.Scan(&from); err == nil {
				if txt, err := e.Store.GetNodeOutputText(ctx, jobID, from); err == nil && txt != "" {
					upOut[from] = txt
				}
			}
		}
	}

	payload := kafka.NodeDispatchPayload{
		Node: dsl.Node{ID: nodeID, Type: nodeType, Params: params},
	}
	payload.Context.UpstreamOutputs = upOut
	return payload, nil
}

func (e *Engine) buildRetryPayload(ctx context.Context, jobID, nodeID string) (kafka.NodeRetryPayload, error) {
	dispatchPayload, err := e.buildDispatchPayload(ctx, jobID, nodeID)
	if err != nil {
		return kafka.NodeRetryPayload{}, err
	}
	var p kafka.NodeRetryPayload
	p.Node = dispatchPayload.Node
	p.Context.UpstreamOutputs = dispatchPayload.Context.UpstreamOutputs
	return p, nil
}

func (e *Engine) buildDLQPayload(ctx context.Context, jobID, nodeID string, attempt int, reason string) (kafka.NodeDLQPayload, error) {
	dispatchPayload, err := e.buildDispatchPayload(ctx, jobID, nodeID)
	if err != nil {
		return kafka.NodeDLQPayload{}, err
	}
	var p kafka.NodeDLQPayload
	p.Node = dispatchPayload.Node
	p.FinalAttempt = attempt
	p.Reason = reason
	p.Context.UpstreamOutputs = dispatchPayload.Context.UpstreamOutputs
	return p, nil
}

func (e *Engine) triggerReplanAsChildJob(ctx context.Context, parentJobID, parentTraceID, failedNodeID, failedNodeType, failedErr string) (string, error) {
	j, err := e.Store.GetJob(ctx, parentJobID)
	if err == nil {
		if j.Status == "NEEDS_REPLAN" || j.Status == "REPLANNED" || strings.Contains(j.FailedReason, "replanned_to_job=") {
			return "", nil
		}
	}

	origReq := ""
	if err == nil && strings.TrimSpace(j.UserRequest) != "" && j.UserRequest != "(from submit)" {
		origReq = j.UserRequest
	} else {
		origReq = "unknown (please check jobs.user_request from submit)"
	}

	upOut := map[string]string{}
	rows, qerr := e.Store.DB.QueryContext(ctx,
		`SELECT from_node FROM job_edges WHERE job_id=? AND to_node=?`, parentJobID, failedNodeID)
	if qerr == nil {
		defer rows.Close()
		for rows.Next() {
			var from string
			if err := rows.Scan(&from); err == nil {
				if txt, err := e.Store.GetNodeOutputText(ctx, parentJobID, from); err == nil && txt != "" {
					upOut[from] = txt
				}
			}
		}
	}

	var b strings.Builder
	b.WriteString("You are replanning a workflow after execution failure.\n\n")
	b.WriteString("Original request:\n")
	b.WriteString(origReq)
	b.WriteString("\n\nExecution failure:\n")
	b.WriteString("- parent_job_id: " + parentJobID + "\n")
	b.WriteString("- failed_node_id: " + failedNodeID + "\n")
	b.WriteString("- failed_node_type: " + failedNodeType + "\n")
	b.WriteString("- error_msg: " + failedErr + "\n\n")
	if len(upOut) > 0 {
		b.WriteString("Upstream outputs (may help repair):\n")
		for k, v := range upOut {
			b.WriteString("- " + k + ": " + v + "\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("Constraints:\n")
	b.WriteString("- Output must be valid DAG JSON strictly matching schema.\n")
	b.WriteString("- node.type must be from allowed operator catalog.\n")
	b.WriteString("- Prefer minimal changes to achieve the same intent.\n")
	b.WriteString("- If failure is related to filename/path, choose a safe filename like \"" + parentJobID + "_fixed.txt\".\n")
	b.WriteString("- Avoid Windows reserved names (CON, PRN, AUX, NUL, COM1...).\n\n")
	b.WriteString("Return only strict JSON.\n")
	replanUserRequest := b.String()

	_ = e.Store.UpdateJobStatusWithReason(ctx, parentJobID, "NEEDS_REPLAN", failedErr)

	childJobID := util.NewID("job")
	childTraceID := util.NewID("trace")
	_ = e.Store.InsertJob(ctx, store.Job{
		JobID:       childJobID,
		TraceID:     childTraceID,
		Status:      "PENDING",
		UserRequest: replanUserRequest,
		ParentJobID: parentJobID,
	})

	ev := kafka.Envelope[kafka.JobSubmitPayload]{
		EventID: util.NewID("ev"),
		Type:    kafka.TypeJobSubmit,
		TSMs:    time.Now().UnixMilli(),
		TraceID: childTraceID,
		JobID:   childJobID,
		Payload: kafka.JobSubmitPayload{UserRequest: replanUserRequest},
	}
	if err := e.Prod.Publish(e.Topics.Submit, childJobID, ev); err != nil {
		_ = e.Store.UpdateJobFailed(ctx, childJobID, "replan submit publish failed: "+err.Error())
		_ = e.Store.UpdateJobFailed(ctx, parentJobID, failedErr+" | replan submit publish failed: "+err.Error())
		return "", err
	}

	_ = e.Store.UpdateJobStatusWithReason(ctx, parentJobID, "REPLANNED", failedErr+" | replanned_to_job="+childJobID)
	_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
		EventID:  util.NewID("ev"),
		JobID:    parentJobID,
		Status:   "REPLAN_TRIGGERED",
		TSMs:     time.Now().UnixMilli(),
		ErrorMsg: "replanned_to_job=" + childJobID,
	})
	return childJobID, nil
}
