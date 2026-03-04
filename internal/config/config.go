package config

import (
	"fmt"
	"os"
	"strings"
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

	HTTPAddr string
	DataDir  string
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

		HTTPAddr: getenv("HTTP_ADDR", ":8080"),
		DataDir:  getenv("DATA_DIR", "./data"),
	}
}

func (c Config) MustValidate() {
	if c.MySQLDSN == "" {
		panic("MYSQL_DSN is empty")
	}
	if len(c.KafkaBrokers) == 0 || (len(c.KafkaBrokers) == 1 && c.KafkaBrokers[0] == "") {
		panic("KAFKA_BROKERS is empty")
	}
	for _, t := range []string{c.TopicSubmit, c.TopicPlanResult, c.TopicDispatch, c.TopicNodeStatus} {
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