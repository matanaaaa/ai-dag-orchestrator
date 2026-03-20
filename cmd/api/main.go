package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
)

type submitReq struct {
	UserRequest string `json:"user_request"`
}

func main() {
	cfg := config.Load()
	cfg.MustValidate()

	st, err := store.Open(cfg.MySQLDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	prod, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()

	topics := kafka.Topics{
		Submit:     cfg.TopicSubmit,
		PlanResult: cfg.TopicPlanResult,
		Dispatch:   cfg.TopicDispatch,
		NodeStatus: cfg.TopicNodeStatus,
		DLQ:        cfg.TopicNodeDLQ,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var req submitReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserRequest) == "" {
			http.Error(w, "invalid body", 400)
			return
		}

		jobID := util.NewID("job")
		traceID := util.NewID("trace")
		ev := kafka.Envelope[kafka.JobSubmitPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeJobSubmit,
			TSMs:    time.Now().UnixMilli(),
			TraceID: traceID,
			JobID:   jobID,
			Payload: kafka.JobSubmitPayload{UserRequest: req.UserRequest},
		}
		_ = st.InsertJob(r.Context(), store.Job{
			JobID:       jobID,
			TraceID:     traceID,
			Status:      "PENDING",
			UserRequest: req.UserRequest,
		})
		if err := prod.Publish(topics.Submit, jobID, ev); err != nil {
			_ = st.UpdateJobFailed(r.Context(), jobID, "submit publish failed: "+err.Error())
			http.Error(w, "kafka publish failed: "+err.Error(), 500)
			return
		}

		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"job_id": jobID})
	})

	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/jobs/")
		if path == "" {
			http.Error(w, "missing job_id", 400)
			return
		}

		if strings.HasSuffix(path, "/events") {
			jobID := strings.TrimSuffix(path, "/events")
			jobID = strings.TrimSuffix(jobID, "/")
			serveJobEventsSSE(w, r, st, jobID)
			return
		}

		if strings.HasSuffix(path, "/dag.dot") {
			jobID := strings.TrimSuffix(path, "/dag.dot")
			jobID = strings.TrimSuffix(jobID, "/")
			serveJobDAGDot(w, r, st, jobID)
			return
		}

		jobID := strings.TrimSuffix(path, "/")
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		j, err := st.GetJob(ctx, jobID)
		if err != nil {
			http.Error(w, "job not found: "+err.Error(), 404)
			return
		}
		childIDs, _ := st.ListChildJobs(ctx, jobID)
		replannedToJobID := extractReplannedToJobID(j.FailedReason, childIDs)

		rows, err := st.DB.QueryContext(ctx,
			`SELECT n.node_id,n.node_type,n.status,n.indegree_remaining,r.output_text,r.artifact_uri,r.error_msg
			 FROM job_nodes n
			 LEFT JOIN node_results r ON n.job_id=r.job_id AND n.node_id=r.node_id
			 WHERE n.job_id=? ORDER BY n.node_id`, jobID)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), 500)
			return
		}
		defer rows.Close()

		type nodeResp struct {
			NodeID   string `json:"node_id"`
			Type     string `json:"type"`
			Status   string `json:"status"`
			Indegree int    `json:"indegree_remaining"`
			Output   string `json:"output_text,omitempty"`
			Artifact string `json:"artifact_uri,omitempty"`
			Error    string `json:"error_msg,omitempty"`
		}
		var nodes []nodeResp
		for rows.Next() {
			var nr nodeResp
			var out, art, er sql.NullString
			if err := rows.Scan(&nr.NodeID, &nr.Type, &nr.Status, &nr.Indegree, &out, &art, &er); err != nil {
				http.Error(w, "scan failed: "+err.Error(), 500)
				return
			}
			if out.Valid {
				nr.Output = out.String
			}
			if art.Valid {
				nr.Artifact = art.String
			}
			if er.Valid {
				nr.Error = er.String
			}
			nodes = append(nodes, nr)
		}

		resp := map[string]any{
			"job": map[string]any{
				"job_id":              j.JobID,
				"parent_job_id":       j.ParentJobID,
				"trace_id":            j.TraceID,
				"status":              j.Status,
				"user_request":        j.UserRequest,
				"failed_reason":       j.FailedReason,
				"replanned_to_job_id": replannedToJobID,
			},
			"planner": map[string]any{
				"model":         j.PlannerModel,
				"latency_ms":    j.PlannerLatencyMs,
				"repair_rounds": j.RepairRounds,
			},
			"nodes":         nodes,
			"child_job_ids": childIDs,
		}

		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	log.Printf("[api] listening on %s", cfg.HTTPAddr)
	log.Fatal(srv.ListenAndServe())
}

func extractReplannedToJobID(failedReason string, childIDs []string) string {
	const marker = "replanned_to_job="
	if i := strings.Index(failedReason, marker); i >= 0 {
		v := failedReason[i+len(marker):]
		if j := strings.IndexAny(v, " |\n\t\r"); j >= 0 {
			v = v[:j]
		}
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	if len(childIDs) > 0 {
		return childIDs[len(childIDs)-1]
	}
	return ""
}

func serveJobEventsSSE(w http.ResponseWriter, r *http.Request, st *store.Store, jobID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	if jobID == "" {
		http.Error(w, "missing job_id", 400)
		return
	}

	lastID := int64(0)
	if v := r.URL.Query().Get("last_id"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			lastID = n
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", 500)
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	send := func(name string, v any, id int64) bool {
		b, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, name, string(b)); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	_ = send("hello", map[string]any{"job_id": jobID, "last_id": lastID}, lastID)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			events, err := st.ListNodeEventsAfter(ctx, jobID, lastID, 200)
			cancel()
			if err != nil {
				_ = send("error", map[string]any{"error": err.Error()}, lastID)
				return
			}

			for _, e := range events {
				payload := map[string]any{
					"id":           e.ID,
					"event_id":     e.EventID,
					"job_id":       e.JobID,
					"node_id":      e.NodeID,
					"status":       e.Status,
					"attempt":      e.Attempt,
					"ts_ms":        e.TSMs,
					"worker_id":    e.WorkerID,
					"output_text":  e.OutputText,
					"artifact_uri": e.ArtifactURI,
					"error_msg":    e.ErrorMsg,
				}
				if !send("node_event", payload, e.ID) {
					return
				}
				lastID = e.ID
			}
			_, _ = fmt.Fprintf(w, ": keepalive %d\n\n", time.Now().UnixMilli())
			flusher.Flush()
		}
	}
}

func serveJobDAGDot(w http.ResponseWriter, r *http.Request, st *store.Store, jobID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	if jobID == "" {
		http.Error(w, "missing job_id", 400)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	// 确认 job 存在（顺便拿 version/model 也行）
	if _, err := st.GetJob(ctx, jobID); err != nil {
		http.Error(w, "job not found: "+err.Error(), 500)
		return
	}

	// 1) 从 job_nodes 拿节点 id/type
	nrows, err := st.DB.QueryContext(ctx,
		`SELECT node_id, node_type
		 FROM job_nodes
		 WHERE job_id=?
		 ORDER BY node_id`, jobID)
	if err != nil {
		http.Error(w, "query nodes failed: "+err.Error(), 500)
		return
	}
	defer nrows.Close()

	nodes := make([]dsl.Node, 0, 16)
	for nrows.Next() {
		var id, typ string
		if err := nrows.Scan(&id, &typ); err != nil {
			http.Error(w, "scan nodes failed: "+err.Error(), 500)
			return
		}
		nodes = append(nodes, dsl.Node{
			ID:     id,
			Type:   typ,
			Params: map[string]any{}, // DOT 不用 params，给空即可
		})
	}

	// 2) 从 job_edges 拿边
	erows, err := st.DB.QueryContext(ctx,
		`SELECT from_node, to_node
		FROM job_edges
		WHERE job_id=?
		ORDER BY from_node, to_node`, jobID)
	if err != nil {
		http.Error(w, "query edges failed: "+err.Error(), 500)
		return
	}
	defer erows.Close()

	edges := make([]dsl.Edge, 0, 16)
	for erows.Next() {
		var from, to string
		if err := erows.Scan(&from, &to); err != nil {
			http.Error(w, "scan edges failed: "+err.Error(), 500)
			return
		}
		edges = append(edges, dsl.Edge{From: from, To: to})
	}

	dag := dsl.DAG{
		DAGVersion: "v1",
		Nodes:      nodes,
		Edges:      edges,
	}

	dot := dag.ToDOT()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(dot))
}
