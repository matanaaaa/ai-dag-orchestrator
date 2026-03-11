package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"

	"github.com/matanaaaa/ai-dag-orchestrator/internal/config"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/dsl"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/kafka"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/observability"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/planner"
	"github.com/matanaaaa/ai-dag-orchestrator/internal/util"
)

var llmLatencyBuckets = []float64{50, 100, 200, 500, 1000, 2000, 5000, 10000}

func main() {
	cfg := config.Load()
	cfg.MustValidate()

	reg := observability.NewRegistry()
	go serveMetrics(cfg.MetricsAddr, reg)

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

	var schemaAny map[string]any
	if err := planner.LoadJSONFile("assets/schema/dag_v1.schema.json", &schemaAny); err != nil {
		log.Fatalf("[planner] load schema failed: %v", err)
	}
	var cat planner.OperatorCatalog
	if err := planner.LoadJSONFile("assets/catalog/operator_catalog.json", &cat); err != nil {
		log.Fatalf("[planner] load catalog failed: %v", err)
	}

	devPrompt := planner.BuildDeveloperPrompt(cat)
	allowedSet := planner.BuildAllowedOperatorSet(cat)

	var llm planner.LLMProvider
	switch cfg.LLMProvider {
	case "openai":
		if cfg.OpenAIAPIKey == "" {
			log.Printf("[planner] LLM_PROVIDER=openai but OPENAI_API_KEY empty, will fallback to fixed DAG")
		} else {
			llm = planner.NewOpenAIResponsesProvider(cfg.OpenAIAPIKey)
			log.Printf("[planner] using OpenAI Responses API model=%s max_repair=%d", cfg.OpenAIModel, cfg.PlannerMaxRepair)
		}
	case "compatible":
		llm = planner.NewOpenAICompatibleChatCompletionsProvider(cfg.LLMBaseURL, cfg.OpenAIAPIKey)
		log.Printf("[planner] using compatible ChatCompletions endpoint=%s model=%s max_repair=%d",
			cfg.LLMBaseURL, cfg.OpenAIModel, cfg.PlannerMaxRepair)
	default:
		log.Printf("[planner] unsupported LLM_PROVIDER=%s, will fallback to fixed DAG", cfg.LLMProvider)
	}

	pl := &planner.Planner{
		LLM:                  llm,
		MaxRepair:            cfg.PlannerMaxRepair,
		Model:                cfg.OpenAIModel,
		SchemaAny:            schemaAny,
		DevPrompt:            devPrompt,
		AllowedOperatorTypes: allowedSet,
	}

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var env kafka.Envelope[kafka.JobSubmitPayload]
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return err
		}
		log.Printf("[planner] got submit job_id=%s trace_id=%s user_len=%d", env.JobID, env.TraceID, len(env.Payload.UserRequest))

		reg.SetGauge("planner_queue_lag_ms", nil, float64(time.Now().UnixMilli()-env.TSMs))

		var (
			d    dsl.DAG
			meta planner.PlanMeta
			perr error
		)

		if pl.LLM != nil {
			d, meta, perr = pl.Plan(ctx, env.Payload.UserRequest)
			if perr != nil {
				log.Printf("[planner] plan failed job_id=%s err=%v", env.JobID, perr)
			} else {
				log.Printf("[planner] plan ok job_id=%s model=%s latency_ms=%d repair=%d",
					env.JobID, meta.Model, meta.LatencyMs, meta.RepairRounds)
			}
			reg.ObserveHistogram("planner_llm_latency_ms", nil, float64(meta.LatencyMs), llmLatencyBuckets)
			reg.IncCounter("planner_repair_rounds_total", nil, float64(max(meta.RepairRounds, 0)))
		} else {
			perr = context.Canceled
			log.Printf("[planner] LLM is nil, provider=%s (check LLM_PROVIDER)", cfg.LLMProvider)
		}

		if perr != nil {
			reg.IncCounter("planner_plan_total", map[string]string{"result": "fallback"}, 1)
			d = dsl.DAG{
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
			meta.Model = "fallback"
			meta.LatencyMs = 0
			meta.RepairRounds = -1
		} else {
			reg.IncCounter("planner_plan_total", map[string]string{"result": "success"}, 1)
		}

		payload := kafka.PlanResultPayload{
			OK:               true,
			DAG:              d,
			PlannerModel:     meta.Model,
			PlannerLatencyMs: meta.LatencyMs,
			RepairRounds:     meta.RepairRounds,
		}
		if perr != nil && pl.LLM != nil {
			payload.Err = "llm_failed_fallback_used: " + perr.Error()
		}

		out := kafka.Envelope[kafka.PlanResultPayload]{
			EventID: util.NewID("ev"),
			Type:    kafka.TypePlanResult,
			TSMs:    time.Now().UnixMilli(),
			TraceID: env.TraceID,
			JobID:   env.JobID,
			Payload: payload,
		}
		log.Printf("[planner] publish plan_result job_id=%s planner_model=%s fallback=%v",
    		env.JobID, meta.Model, meta.Model == "fallback")
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

func serveMetrics(addr string, reg *observability.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[planner] metrics server stopped: %v", err)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
