package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/observability"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/orchestrator"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"
)

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

	eng := &orchestrator.Engine{
		Store: st, Prod: prod, Topics: topics,
		MaxRetry: cfg.NodeMaxRetry, RetryBackoffMs: cfg.RetryBackoffMs, Metrics: reg,
	}

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		reg.SetGauge("orchestrator_queue_lag_ms", map[string]string{"topic": msg.Topic}, float64(time.Now().UnixMilli()-msg.Timestamp.UnixMilli()))
		switch msg.Topic {
		case topics.PlanResult:
			return eng.HandlePlanResult(ctx, msg.Value)
		case topics.NodeStatus:
			return eng.HandleNodeStatus(ctx, msg.Value)
		default:
			return nil
		}
	}

	cons, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-orchestrator", []string{topics.PlanResult, topics.NodeStatus}, handler)
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	log.Printf("[orchestrator] consuming %s,%s", topics.PlanResult, topics.NodeStatus)
	log.Fatal(cons.Run(context.Background()))
}

func serveMetrics(addr string, reg *observability.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[orchestrator] metrics server stopped: %v", err)
	}
}
