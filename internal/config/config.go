package config

import (
	"fmt"
	"os"
	"strings"
	"strconv"
)

type Config struct {
	Env string

	MySQLDSN string // e.g. user:pass@tcp(localhost:3306)/dag?parseTime=true&multiStatements=true

	KafkaBrokers []string
	KafkaGroupID string

	TopicSubmit     string
	TopicPlanResult string
	TopicDispatch   string
	TopicNodeStatus string
	TopicNodeRetry  string
	TopicNodeDLQ    string

	HTTPAddr string
	DataDir  string
	MetricsAddr string

	OpenAIAPIKey      string
	OpenAIModel       string
	LLMProvider       string
	LLMBaseURL        string
	PlannerMaxRepair  int
	NodeMaxRetry      int
	RetryBackoffMs    int64
}

func Load() Config {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	group := getenv("KAFKA_GROUP_ID", "ai-dag")

	return Config{
		Env:         getenv("ENV", "dev"),
		MySQLDSN:    getenv("MYSQL_DSN", "root:root@tcp(localhost:3306)/dag?parseTime=true&multiStatements=true"),
		KafkaBrokers: strings.Split(brokers, ","),
		KafkaGroupID: group,

		TopicSubmit:     getenv("TOPIC_SUBMIT", "dag.job.submit"),
		TopicPlanResult: getenv("TOPIC_PLAN_RESULT", "dag.plan.result"),
		TopicDispatch:   getenv("TOPIC_NODE_DISPATCH", "dag.node.dispatch"),
		TopicNodeStatus: getenv("TOPIC_NODE_STATUS", "dag.node.status"),
		TopicNodeRetry:  getenv("TOPIC_NODE_RETRY", "dag.node.retry"),
		TopicNodeDLQ:    getenv("TOPIC_NODE_DLQ", "dag.node.dlq"),

		HTTPAddr: getenv("HTTP_ADDR", ":8080"),
		DataDir:  getenv("DATA_DIR", "./data"),
		MetricsAddr: getenv("METRICS_ADDR", ":9090"),

		OpenAIAPIKey: getenv("OPENAI_API_KEY", ""),
		OpenAIModel:  getenv("OPENAI_MODEL", "gpt-4o-mini"),
		LLMProvider:  getenv("LLM_PROVIDER", "openai"),
		LLMBaseURL: getenv("LLM_BASE_URL", "https://api.openai.com/v1"),
		PlannerMaxRepair: mustAtoi(getenv("PLANNER_MAX_REPAIR", "2")),
		NodeMaxRetry: mustAtoi(getenv("NODE_MAX_RETRY", "3")),
		RetryBackoffMs: mustAtoi64(getenv("RETRY_BACKOFF_MS", "5000")),
	}
}

func (c Config) MustValidate() {
	if c.MySQLDSN == "" {
		panic("MYSQL_DSN is empty")
	}
	if len(c.KafkaBrokers) == 0 || (len(c.KafkaBrokers) == 1 && c.KafkaBrokers[0] == "") {
		panic("KAFKA_BROKERS is empty")
	}
	for _, t := range []string{c.TopicSubmit, c.TopicPlanResult, c.TopicDispatch, c.TopicNodeStatus, c.TopicNodeRetry, c.TopicNodeDLQ} {
		if t == "" {
			panic(fmt.Sprintf("topic empty: %v", t))
		}
	}
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 2
	}
	return n
}

func mustAtoi64(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 5000
	}
	return n
}
