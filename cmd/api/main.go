package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
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

		// P0：API 不落 jobs 表，交给 orchestrator 在 plan.result 时落库
		if err := prod.Publish(topics.Submit, jobID, ev); err != nil {
			http.Error(w, "kafka publish failed: "+err.Error(), 500)
			return
		}

		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"job_id": jobID})
	})

	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
		if jobID == "" {
			http.Error(w, "missing job_id", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		j, err := st.GetJob(ctx, jobID)
		if err != nil {
			http.Error(w, "job not found: "+err.Error(), 404)
			return
		}

		// 简化：直接查 nodes + results 用 2 次查询（P0 省事）
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
				"job_id":       j.JobID,
				"trace_id":     j.TraceID,
				"status":       j.Status,
				"user_request": j.UserRequest,
			},
			"nodes": nodes,
		}

		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	log.Printf("[api] listening on %s", cfg.HTTPAddr)
	log.Fatal(srv.ListenAndServe())
}
