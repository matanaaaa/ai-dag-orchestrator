package orchestrator

import (
	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Engine struct {
	Store  *store.Store
	Prod   *kafka.Producer
	Topics kafka.Topics
}

func (e *Engine) HandlePlanResult(ctx context.Context, msgValue []byte) error {
	var env kafka.Envelope[kafka.PlanResultPayload]
	if err := json.Unmarshal(msgValue, &env); err != nil {
		return err
	}
	if !env.Payload.OK {
		// P0：直接失败
		_ = e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "planner failed",
		})
		return nil
	}

	v, err := dsl.Validate(env.Payload.DAG)
	if err != nil {
		_ = e.Store.InsertJob(ctx, store.Job{
			JobID: env.JobID, TraceID: env.TraceID, Status: "FAILED", UserRequest: "invalid dag: " + err.Error(),
		})
		return nil
	}

	// Insert job
	if err := e.Store.InsertJob(ctx, store.Job{
		JobID: env.JobID, TraceID: env.TraceID, Status: "RUNNING", UserRequest: "(from submit)",
	}); err != nil {
		// job 可能已存在（重复消息），P0 先忽略
	}

	// Insert nodes
	for id, n := range v.NodeByID {
		deg := v.Indegree[id]
		if err := e.Store.InsertNode(ctx, env.JobID, n.ID, n.Type, n.Params, "PENDING", deg); err != nil {
			// ignore duplicates in P0
		}
	}
	// Insert edges
	for _, ed := range env.Payload.DAG.Edges {
		_ = e.Store.InsertEdge(ctx, env.JobID, ed.From, ed.To)
	}

	// Dispatch indegree=0 nodes
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

	switch env.Payload.Status {
	case "STARTED":
		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "RUNNING")
		return nil

	case "SUCCEEDED":
		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "SUCCEEDED")
		_ = e.Store.UpsertNodeResult(ctx, env.JobID, env.NodeID, env.Payload.OutputText, env.Payload.ArtifactURI, "")

		down, err := e.Store.ListDownstream(ctx, env.JobID, env.NodeID)
		if err != nil {
			return err
		}
		for _, to := range down {
			_ = e.Store.DecrementIndegree(ctx, env.JobID, to)
			// 如果变成 ready，就 dispatch
			ready, err := e.Store.ListReadyNodes(ctx, env.JobID)
			if err != nil {
				return err
			}
			for _, nid := range ready {
				if err := e.dispatchNode(ctx, env.JobID, env.TraceID, nid); err != nil {
					return err
				}
			}
		}

		// job 完成判定
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
		}
		return nil

	case "FAILED":
		_ = e.Store.MarkNodeStatus(ctx, env.JobID, env.NodeID, "FAILED")
		_ = e.Store.UpsertNodeResult(ctx, env.JobID, env.NodeID, "", "", env.Payload.ErrorMsg)
		_ = e.Store.UpdateJobStatus(ctx, env.JobID, "FAILED")
		return nil
	default:
		return fmt.Errorf("unknown status: %s", env.Payload.Status)
	}
}

func (e *Engine) dispatchNode(ctx context.Context, jobID, traceID, nodeID string) error {
	// 标 READY（P0 简化：直接更新）
	_ = e.Store.MarkNodeStatus(ctx, jobID, nodeID, "READY")

	nodeType, params, err := e.Store.GetNodeTypeAndParams(ctx, jobID, nodeID)
	if err != nil {
		return err
	}

	// 组装上游输出（P0 用 edges 表反查上游）
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

	ev := kafka.Envelope[kafka.NodeDispatchPayload]{
		EventID: util.NewID("ev"),
		Type:    kafka.TypeNodeDispatch,
		TSMs:    time.Now().UnixMilli(),
		TraceID: traceID,
		JobID:   jobID,
		NodeID:  nodeID,
		Payload: payload,
	}

	return e.Prod.Publish(e.Topics.Dispatch, jobID, ev)
}