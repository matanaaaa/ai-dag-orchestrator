package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	sp sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Version = sarama.V2_8_0_0

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{sp: p}, nil
}

func (p *Producer) Close() error { return p.sp.Close() }

func (p *Producer) Publish(topic, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, _, err = p.sp.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	})
	return err
}

type HandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

type Consumer struct {
	group sarama.ConsumerGroup
	topics []string
	handler HandlerFunc
}

func NewConsumer(brokers []string, groupID string, topics []string, handler HandlerFunc) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Session.Timeout = 10 * time.Second

	g, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: g, topics: topics, handler: handler}, nil
}

func (c *Consumer) Close() error { return c.group.Close() }

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.group.Consume(ctx, c.topics, &cgHandler{ctx: ctx, handler: c.handler}); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

type cgHandler struct {
	ctx context.Context
	handler HandlerFunc
}

func (h *cgHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *cgHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *cgHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		err := h.handler(h.ctx, msg)
		if err != nil {
			log.Printf("[kafka] handler error topic=%s partition=%d offset=%d err=%v",
				msg.Topic, msg.Partition, msg.Offset, err)
		}
		sess.MarkMessage(msg, fmt.Sprintf("handled_at=%d", time.Now().UnixMilli()))
	}
	return nil
}