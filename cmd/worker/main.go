package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
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
		return executeNode(ctx, prod, reg, registry, topics, cfg.DataDir, workerID, host, env)
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

func executeNode(
	ctx context.Context,
	prod *kafka.Producer,
	reg *observability.Registry,
	registry *operators.Registry,
	topics kafka.Topics,
	dataDir, workerID, host string,
	env kafka.Envelope[kafka.NodeDispatchPayload],
) error {
	op, ok := registry.Get(env.Payload.Node.Type)
	if !ok {
		reg.IncCounter("worker_operator_total", map[string]string{"operator": env.Payload.Node.Type, "result": "failed"}, 1)
		reg.IncCounter("worker_node_total", map[string]string{"result": "failed"}, 1)
		return publishNodeStatus(prod, topics, workerID, host, env, kafka.NodeStatusPayload{
			Status:   "FAILED",
			ErrorMsg: "unknown operator: " + env.Payload.Node.Type,
		})
	}

	if err := publishNodeStatus(prod, topics, workerID, host, env, kafka.NodeStatusPayload{
		Status: "STARTED",
	}); err != nil {
		return err
	}

	opCtx := operators.OpContext{
		JobID:    env.JobID,
		NodeID:   env.NodeID,
		Attempt:  env.Attempt,
		WorkerID: workerID,
		DataDir:  dataDir,
		Upstream: env.Payload.Context.UpstreamOutputs,
	}

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	outText, uri, execErr := runOperatorSafely(runCtx, op, env.Payload.Node.Params, opCtx)
	costMs := float64(time.Since(start).Milliseconds())
	labels := map[string]string{"operator": env.Payload.Node.Type}
	reg.ObserveHistogram("worker_op_duration_ms", labels, costMs, opDurationBuckets)

	if execErr != nil {
		class := classifyOpError(execErr)
		reg.IncCounter("worker_operator_total", map[string]string{"operator": env.Payload.Node.Type, "result": "failed", "class": class}, 1)
		reg.IncCounter("worker_node_total", map[string]string{"result": "failed"}, 1)
		if class == "panic" {
			reg.IncCounter("worker_operator_panic_total", map[string]string{"operator": env.Payload.Node.Type}, 1)
		}
		return publishNodeStatus(prod, topics, workerID, host, env, kafka.NodeStatusPayload{
			Status:   "FAILED",
			ErrorMsg: execErr.Error(),
		})
	}

	reg.IncCounter("worker_operator_total", map[string]string{"operator": env.Payload.Node.Type, "result": "succeeded"}, 1)
	reg.IncCounter("worker_node_total", map[string]string{"result": "succeeded"}, 1)
	return publishNodeStatus(prod, topics, workerID, host, env, kafka.NodeStatusPayload{
		Status:      "SUCCEEDED",
		OutputText:  outText,
		ArtifactURI: uri,
	})
}

func publishNodeStatus(
	prod *kafka.Producer,
	topics kafka.Topics,
	workerID, host string,
	env kafka.Envelope[kafka.NodeDispatchPayload],
	payload kafka.NodeStatusPayload,
) error {
	payload.Worker = struct {
		ID   string `json:"id"`
		Host string `json:"host,omitempty"`
	}{ID: workerID, Host: host}

	return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
		EventID: util.NewID("ev"),
		Type:    kafka.TypeNodeStatus,
		TSMs:    time.Now().UnixMilli(),
		TraceID: env.TraceID,
		JobID:   env.JobID,
		NodeID:  env.NodeID,
		Attempt: env.Attempt,
		Payload: payload,
	})
}

func runOperatorSafely(
	ctx context.Context,
	op operators.Operator,
	params map[string]any,
	opCtx operators.OpContext,
) (outputText, artifactURI string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("operator panic: %v", r)
		}
	}()
	return op.Run(ctx, params, opCtx)
}

func classifyOpError(err error) string {
	switch {
	case err == nil:
		return "ok"
	case strings.Contains(err.Error(), "panic:"):
		return "panic"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	case errors.Is(err, operators.ErrInvalidParams), errors.Is(err, operators.ErrUnsafeFilename), errors.Is(err, operators.ErrNoUpstreamOutput), errors.Is(err, operators.ErrAmbiguousInput):
		return "input"
	default:
		return "execution"
	}
}
