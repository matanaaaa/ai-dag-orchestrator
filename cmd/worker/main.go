package main

import (
	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/worker/operators"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	cfg := config.Load()
	cfg.MustValidate()

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

	reg := operators.NewRegistry(
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

		// started
		_ = prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeNodeStatus,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			NodeID:  env.NodeID,
			Payload: kafka.NodeStatusPayload{
				Status: "STARTED",
				Worker: struct {
					ID   string `json:"id"`
					Host string `json:"host,omitempty"`
				}{ID: workerID, Host: host},
			},
		})

		op, ok := reg.Get(env.Payload.Node.Type)
		if !ok {
			return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeStatus,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
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
			JobID:   env.JobID,
			NodeID:  env.NodeID,
			DataDir: cfg.DataDir,
			Upstream: env.Payload.Context.UpstreamOutputs,
		}

		// timeout（P0 简化：固定 10s）
		runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		outText, uri, err := op.Run(runCtx, env.Payload.Node.Params, opCtx)
		if err != nil {
			return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
				EventID: util.NewID("ev"),
				Type:    kafka.TypeNodeStatus,
				TSMs:    time.Now().UnixMilli(),
				TraceID: env.TraceID,
				JobID:   env.JobID,
				NodeID:  env.NodeID,
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

		return prod.Publish(topics.NodeStatus, env.JobID, kafka.Envelope[kafka.NodeStatusPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeNodeStatus,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			NodeID:  env.NodeID,
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