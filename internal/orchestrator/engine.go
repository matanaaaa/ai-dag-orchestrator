package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
		if err := e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "planner failed", FailedReason: env.Payload.Err,
			PlannerModel: pm, PlannerLatencyMs: pl, RepairRounds: pr,
		}); err != nil {
			return err
		}
		return nil
	}

	v, err := dsl.Validate(env.Payload.DAG)
	if err != nil {
		if err := e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "invalid dag", FailedReason: err.Error(),
			PlannerModel: pm, PlannerLatencyMs: pl, RepairRounds: pr,
		}); err != nil {
			return err
		}
		return nil
	}

	shouldDispatch := true
	if err := e.Store.WithTx(ctx, func(q store.Querier) error {
		existing, err := e.Store.GetJobTx(ctx, q, env.JobID)
		switch {
		case err == nil:
			if existing.Status == "SUCCEEDED" || existing.Status == "FAILED" || existing.Status == "REPLANNED" {
				shouldDispatch = false
				return nil
			}
		case err != nil && err != sql.ErrNoRows:
			return err
		}

		claimed := false
		switch {
		case err == sql.ErrNoRows:
			if err := e.Store.InsertJobTx(ctx, q, store.Job{
				JobID:            env.JobID,
				TraceID:          env.TraceID,
				Status:           "RUNNING",
				UserRequest:      "(from submit)",
				PlannerModel:     pm,
				PlannerLatencyMs: pl,
				RepairRounds:     pr,
			}); err != nil {
				return err
			}
			claimed = true
		case err == nil:
			claimed, err = e.Store.TryStartJobPlanInitialization(ctx, q, store.Job{
				JobID:            env.JobID,
				TraceID:          env.TraceID,
				PlannerModel:     pm,
				PlannerLatencyMs: pl,
				RepairRounds:     pr,
			})
			if err != nil {
				return err
			}
		default:
			return err
		}
		if !claimed {
			shouldDispatch = false
			return nil
		}

		for id, n := range v.NodeByID {
			deg := v.Indegree[id]
			if err := e.Store.InsertNodeTx(ctx, q, env.JobID, n.ID, n.Type, n.Params, "PENDING", deg); err != nil {
				return err
			}
		}
		for _, ed := range env.Payload.DAG.Edges {
			if err := e.Store.InsertEdgeTx(ctx, q, env.JobID, ed.From, ed.To); err != nil {
				return err
			}
		}
		return e.Store.InsertNodeEventTx(ctx, q, store.NodeEvent{
			EventID: env.EventID,
			JobID:   env.JobID,
			Status:  "PLAN_ACCEPTED",
			TSMs:    time.Now().UnixMilli(),
		})
	}); err != nil {
		return err
	}
	if !shouldDispatch {
		return nil
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
		if err := e.Store.WithTx(ctx, func(q store.Querier) error {
			started, err := e.Store.TryMarkNodeRunningAttempt(ctx, q, env.JobID, env.NodeID, env.Attempt)
			if err != nil {
				return err
			}
			if !started {
				return nil
			}
			return e.Store.InsertNodeEventTx(ctx, q, store.NodeEvent{
				EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "STARTED",
				Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID,
			})
		}); err != nil {
			return err
		}
		return nil

	case "SUCCEEDED":
		finalized := false
		if err := e.Store.WithTx(ctx, func(q store.Querier) error {
			var err error
			finalized, err = e.Store.TryFinalizeNodeSucceeded(ctx, q, store.NodeEvent{
				EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "SUCCEEDED",
				Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID,
				OutputText: env.Payload.OutputText, ArtifactURI: env.Payload.ArtifactURI,
			})
			return err
		}); err != nil {
			return err
		}
		if !finalized {
			return nil
		}

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

		jobDone := false
		if err := e.Store.WithTx(ctx, func(q store.Querier) error {
			var err error
			jobDone, err = e.Store.TryMarkJobSucceeded(ctx, q, env.JobID)
			if err != nil || !jobDone {
				return err
			}
			return e.Store.InsertNodeEventTx(ctx, q, store.NodeEvent{
				EventID: util.NewID("ev"),
				JobID:   env.JobID,
				Status:  "JOB_SUCCEEDED",
				TSMs:    time.Now().UnixMilli(),
			})
		}); err != nil {
			return err
		}
		return nil

	case "FAILED":
		if env.Attempt < e.MaxRetry {
			nextAttempt := env.Attempt + 1
			notBeforeMs := time.Now().Add(time.Duration(e.RetryBackoffMs) * time.Millisecond).UnixMilli()
			scheduled := false
			if err := e.Store.WithTx(ctx, func(q store.Querier) error {
				var err error
				scheduled, err = e.Store.TryScheduleNodeRetry(ctx, q, store.NodeEvent{
					EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "FAILED",
					Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID, ErrorMsg: env.Payload.ErrorMsg,
				}, nextAttempt, notBeforeMs)
				if err != nil || !scheduled {
					return err
				}
				return e.Store.InsertNodeEventTx(ctx, q, store.NodeEvent{
					EventID:  util.NewID("ev"),
					JobID:    env.JobID,
					NodeID:   env.NodeID,
					Status:   "RETRY_SCHEDULED",
					Attempt:  nextAttempt,
					TSMs:     time.Now().UnixMilli(),
					ErrorMsg: env.Payload.ErrorMsg,
				})
			}); err != nil {
				return err
			}
			if !scheduled {
				return nil
			}
			if e.Metrics != nil {
				e.Metrics.IncCounter("worker_retry_total", nil, 1)
			}
			return nil
		}

		finalized := false
		if err := e.Store.WithTx(ctx, func(q store.Querier) error {
			var err error
			finalized, err = e.Store.TryFinalizeNodeFailed(ctx, q, store.NodeEvent{
				EventID: env.EventID, JobID: env.JobID, NodeID: env.NodeID, Status: "FAILED",
				Attempt: env.Attempt, TSMs: env.TSMs, WorkerID: env.Payload.Worker.ID, ErrorMsg: env.Payload.ErrorMsg,
			})
			return err
		}); err != nil {
			return err
		}
		if !finalized {
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
			if err := e.Prod.Publish(e.Topics.DLQ, env.JobID, dlqEnv); err != nil {
				return err
			}
		}

		if replanErr != nil {
			if err := e.Store.UpdateJobFailed(ctx, env.JobID, env.Payload.ErrorMsg+" | replan_error="+replanErr.Error()); err != nil {
				return err
			}
			if err := e.Store.InsertNodeEvent(ctx, store.NodeEvent{
				EventID:  util.NewID("ev"),
				JobID:    env.JobID,
				Status:   "JOB_FAILED",
				TSMs:     time.Now().UnixMilli(),
				ErrorMsg: env.Payload.ErrorMsg + " | replan_error=" + replanErr.Error(),
			}); err != nil {
				return err
			}
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
	claimed, err := e.Store.TryStartNodeDispatch(ctx, jobID, nodeID)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	return e.publishNodeDispatch(ctx, jobID, traceID, nodeID, 1)
}

func (e *Engine) publishNodeDispatch(ctx context.Context, jobID, traceID, nodeID string, attempt int) error {
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
		Attempt: attempt,
		Payload: payload,
	}
	if err := e.Prod.Publish(e.Topics.Dispatch, jobID, ev); err != nil {
		_ = e.Store.InsertNodeEvent(ctx, store.NodeEvent{
			EventID:  util.NewID("ev"),
			JobID:    jobID,
			NodeID:   nodeID,
			Status:   "DISPATCH_FAILED",
			Attempt:  attempt,
			TSMs:     time.Now().UnixMilli(),
			ErrorMsg: err.Error(),
		})
		return fmt.Errorf("publish dispatch failed: %w", err)
	}
	if e.Metrics != nil {
		e.Metrics.IncCounter("orchestrator_dispatch_total", nil, 1)
	}
	if err := e.Store.InsertNodeEvent(ctx, store.NodeEvent{
		EventID: ev.EventID, JobID: jobID, NodeID: nodeID, Status: "DISPATCHED", Attempt: attempt, TSMs: ev.TSMs,
	}); err != nil {
		return err
	}
	return nil
}

func (e *Engine) RecoverPendingDispatches(ctx context.Context, limit int) error {
	nowMs := time.Now().UnixMilli()
	candidates, err := e.Store.ListRecoverableDispatches(ctx, nowMs-5000, nowMs, limit)
	if err != nil {
		return err
	}
	var firstErr error
	for _, c := range candidates {
		job, err := e.Store.GetJob(ctx, c.JobID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := e.publishNodeDispatch(ctx, c.JobID, job.TraceID, c.NodeID, c.CurrentAttempt); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	return firstErr
}

func (e *Engine) buildDispatchPayload(ctx context.Context, jobID, nodeID string) (kafka.NodeDispatchPayload, error) {
	nodeType, params, err := e.Store.GetNodeTypeAndParams(ctx, jobID, nodeID)
	if err != nil {
		return kafka.NodeDispatchPayload{}, err
	}

	upOut := map[string]string{}
	rows, err := e.Store.DB.QueryContext(ctx,
		`SELECT from_node FROM job_edges WHERE job_id=? AND to_node=?`, jobID, nodeID)
	if err != nil {
		return kafka.NodeDispatchPayload{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var from string
		if err := rows.Scan(&from); err != nil {
			return kafka.NodeDispatchPayload{}, err
		}
		txt, err := e.Store.GetNodeOutputText(ctx, jobID, from)
		if err != nil {
			return kafka.NodeDispatchPayload{}, err
		}
		if txt != "" {
			upOut[from] = txt
		}
	}
	if err := rows.Err(); err != nil {
		return kafka.NodeDispatchPayload{}, err
	}

	payload := kafka.NodeDispatchPayload{
		Node: dsl.Node{ID: nodeID, Type: nodeType, Params: params},
	}
	payload.Context.UpstreamOutputs = upOut
	return payload, nil
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
	if qerr != nil {
		return "", qerr
	}
	defer rows.Close()
	for rows.Next() {
		var from string
		if err := rows.Scan(&from); err != nil {
			return "", err
		}
		txt, err := e.Store.GetNodeOutputText(ctx, parentJobID, from)
		if err != nil {
			return "", err
		}
		if txt != "" {
			upOut[from] = txt
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
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

	childJobID := util.NewID("job")
	childTraceID := util.NewID("trace")
	claimed := false
	if err := e.Store.WithTx(ctx, func(q store.Querier) error {
		var err error
		claimed, err = e.Store.TryClaimJobForReplan(ctx, q, parentJobID, failedErr)
		if err != nil || !claimed {
			return err
		}
		if err := e.Store.InsertJobTx(ctx, q, store.Job{
			JobID:       childJobID,
			TraceID:     childTraceID,
			Status:      "PENDING",
			UserRequest: replanUserRequest,
			ParentJobID: parentJobID,
		}); err != nil {
			return err
		}
		return e.Store.InsertNodeEventTx(ctx, q, store.NodeEvent{
			EventID:  util.NewID("ev"),
			JobID:    parentJobID,
			Status:   "REPLAN_TRIGGERED",
			TSMs:     time.Now().UnixMilli(),
			ErrorMsg: "replanned_to_job=" + childJobID,
		})
	}); err != nil {
		return "", err
	}
	if !claimed {
		return "", nil
	}

	ev := kafka.Envelope[kafka.JobSubmitPayload]{
		EventID: util.NewID("ev"),
		Type:    kafka.TypeJobSubmit,
		TSMs:    time.Now().UnixMilli(),
		TraceID: childTraceID,
		JobID:   childJobID,
		Payload: kafka.JobSubmitPayload{UserRequest: replanUserRequest},
	}
	if err := e.Prod.Publish(e.Topics.Submit, childJobID, ev); err != nil {
		if updateErr := e.Store.UpdateJobFailed(ctx, childJobID, "replan submit publish failed: "+err.Error()); updateErr != nil {
			return "", updateErr
		}
		if updateErr := e.Store.UpdateJobFailed(ctx, parentJobID, failedErr+" | replan submit publish failed: "+err.Error()); updateErr != nil {
			return "", updateErr
		}
		return "", err
	}

	if _, err := e.Store.TryMarkJobReplannedNow(ctx, parentJobID, failedErr+" | replanned_to_job="+childJobID); err != nil {
		return "", err
	}
	return childJobID, nil
}
