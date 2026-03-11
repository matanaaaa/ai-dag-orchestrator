package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
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
		Retry:      cfg.TopicNodeRetry,
		DLQ:        cfg.TopicNodeDLQ,
	}

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var env kafka.Envelope[kafka.NodeRetryPayload]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}

		waitMs := env.Payload.NotBeforeMs - time.Now().UnixMilli()
		if waitMs > 0 {
			timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}

		dispatchPayload := kafka.NodeDispatchPayload{Node: env.Payload.Node}
		dispatchPayload.Context.UpstreamOutputs = env.Payload.Context.UpstreamOutputs

		out := kafka.Envelope[kafka.NodeDispatchPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypeNodeDispatch,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			NodeID:  env.NodeID,
			Attempt: env.Attempt,
			Payload: dispatchPayload,
		}
		return prod.Publish(topics.Dispatch, env.JobID, out)
	}

	cons, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-retryhandler", []string{topics.Retry}, handler)
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	log.Printf("[retryhandler] consuming %s", topics.Retry)
	log.Fatal(cons.Run(context.Background()))
}
