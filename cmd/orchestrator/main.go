package main

import (
	"context"
	"log"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/orchestrator"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/store"

	"github.com/IBM/sarama"
)

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

	eng := &orchestrator.Engine{Store: st, Prod: prod, Topics: topics}

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
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