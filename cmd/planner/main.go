package main

import (
	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
	"context"
	"encoding/json"
	"log"
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

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var env kafka.Envelope[kafka.JobSubmitPayload]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}

		// 固定 DAG：download -> transform_upper -> upload_local
		d := dsl.DAG{
			DAGVersion: "v1",
			Nodes: []dsl.Node{
				{ID: "download", Type: "download", Params: map[string]any{"text": "hello world"}},
				{ID: "upper", Type: "transform_upper", Params: map[string]any{}},
				{ID: "upload", Type: "upload_local", Params: map[string]any{"filename": env.JobID + ".txt"}},
			},
			Edges: []dsl.Edge{
				{From: "download", To: "upper"},
				{From: "upper", To: "upload"},
			},
		}

		out := kafka.Envelope[kafka.PlanResultPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypePlanResult,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			Payload: kafka.PlanResultPayload{OK: true, DAG: d},
		}

		return prod.Publish(topics.PlanResult, env.JobID, out)
	}

	cons, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-planner", []string{topics.Submit}, handler)
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	log.Printf("[planner] consuming %s", topics.Submit)
	log.Fatal(cons.Run(context.Background()))
}