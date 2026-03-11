package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/observability"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/worker/operators"
)

var opDurationBuckets = []float64{10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000}

func main() {
	cfg := config.Load()
	cfg.MustValidate()

	reg := observability.NewRegistry()
	go serveMetrics(cfg.MetricsAddr, reg)

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
		Retry:      cfg.TopicNodeRetry,
		DLQ:        cfg.TopicNodeDLQ,
	}

	registry := operators.NewRegistry(
		operators.Download{},
		operators.TransformUpper{},
		operators.UploadLocal{},
	)

	workerID := util.NewID("worker")
	host, _ := os.Hostname()

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var env kafka.Envelope[kafka.NodeDispatchPayload]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}
		if env.Attempt <= 0 {
			env.Attempt = 1
		}

		if env.Attempt > 1 {
			reg.IncCounter("worker_retry_total", nil, 1)
		}

		claimed, err := st.TryClaimNodeAttempt(ctx, env.JobID, env.NodeID, env.Attempt, workerID)
		if err != nil {
			return err
		}
		if !claimed {
			log.Printf("[worker] duplicate claim ignored job=%s node=%s attempt=%d", env.JobID, env.NodeID, env.Attempt)
			return nil
		}

		_ = prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeNodeStatus,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			NodeID:  env.NodeID,
			Attempt: env.Attempt,
			Payload: kafka.NodeStatusPayload{
				Status: "STARTED",
				Worker: struct {
					ID   string `json:"id"`
					Host string `json:"host,omitempty"`
				}{ID: workerID, Host: host},
			},
		})

		op, ok := registry.Get(env.Payload.Node.Type)
		if !ok {
			reg.IncCounter("worker_node_total", map[string]string{"result": "failed"}, 1)
			return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeStatus,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
				Attempt: env.Attempt,
				Payload: kafka.NodeStatusPayload{
					Status: "FAILED",
					Worker: struct {
						ID   string `json:"id"`
						Host string `json:"host,omitempty"`
					}{ID: workerID, Host: host},
					ErrorMsg: "unknown operator: " + env.Payload.Node.Type,
				},
			})
		}

		opCtx := operators.OpContext{
			JobID:    env.JobID,
			NodeID:   env.NodeID,
			DataDir:  cfg.DataDir,
			Upstream: env.Payload.Context.UpstreamOutputs,
		}

		runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		start := time.Now()
		outText, uri, err := op.Run(runCtx, env.Payload.Node.Params, opCtx)
		costMs := float64(time.Since(start).Milliseconds())
		reg.ObserveHistogram("worker_op_duration_ms", map[string]string{"operator": env.Payload.Node.Type}, costMs, opDurationBuckets)

		if err != nil {
			reg.IncCounter("worker_node_total", map[string]string{"result": "failed"}, 1)
			return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeStatus,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
				Attempt: env.Attempt,
				Payload: kafka.NodeStatusPayload{
					Status: "FAILED",
					Worker: struct {
						ID   string `json:"id"`
						Host string `json:"host,omitempty"`
					}{ID: workerID, Host: host},
					ErrorMsg: err.Error(),
				},
			})
		}

		reg.IncCounter("worker_node_total", map[string]string{"result": "succeeded"}, 1)
		return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeNodeStatus,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			NodeID:  env.NodeID,
			Attempt: env.Attempt,
			Payload: kafka.NodeStatusPayload{
				Status:      "SUCCEEDED",
				OutputText:  outText,
				ArtifactURI: uri,
				Worker: struct {
					ID   string `json:"id"`
					Host string `json:"host,omitempty"`
				}{ID: workerID, Host: host},
			},
		})
	}

	cons, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-worker", []string{topics.Dispatch}, handler)
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	log.Printf("[worker] consuming %s", topics.Dispatch)
	log.Fatal(cons.Run(context.Background()))
}

func serveMetrics(addr string, reg *observability.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[worker] metrics server stopped: %v", err)
	}
}
