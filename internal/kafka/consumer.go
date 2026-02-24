package kafka

import (
	"context"
	"encoding/json"
	"flight_processing/internal/cache"
	"flight_processing/internal/metrics"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type MessageProcessor interface {
	ProcessFlightMessage(message []byte) error
}

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	handler sarama.ConsumerGroupHandler
	logger  *log.Logger
}

func NewConsumer(
	brokers []string,
	groupID string,
	topic string,
	processor MessageProcessor,
	c cache.Cache,
	logger *log.Logger,
) (*Consumer, error) {
	if logger == nil {
		logger = log.Default()
	}

	cfg := sarama.NewConfig()

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Важно: коммит только руками после успешной обработки
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}
	cfg.Consumer.Group.Session.Timeout = 30 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	h := &flightGroupHandler{
		processor: processor,
		logger:    logger,
		cache:     c,
	}

	return &Consumer{
		group:   group,
		topic:   topic,
		handler: h,
		logger:  logger,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	// Ошибки группы в отдельный поток логов
	go func() {
		for err := range c.group.Errors() {
			c.logger.Printf("consumer group error: %v", err)
			metrics.IncKafkaError("consumer", "group")
		}
	}()

	for {
		err := c.group.Consume(ctx, []string{c.topic}, c.handler)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Printf("consume loop error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

type flightGroupHandler struct {
	processor MessageProcessor
	logger    *log.Logger
	cache     cache.Cache
}

func (h *flightGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *flightGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *flightGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for kafkaMsg := range claim.Messages() {
		lag := claim.HighWaterMarkOffset() - kafkaMsg.Offset - 1
		metrics.SetKafkaConsumerLag(kafkaMsg.Topic, kafkaMsg.Partition, lag)
		// retry до успеха (или пока не отменён контекст)
		if err := h.processWithRetry(session.Context(), kafkaMsg); err != nil {
			// (2) Ошибка обработки
			metrics.IncKafkaError("consumer", "process")
			// Сообщение НЕ отмечаем и НЕ коммитим -> будет прочитано снова
			return err
		}
		// (3) Успешная обработка
		metrics.IncKafkaProcessed()

		//(4) Инвалидация кеша
		if h.cache != nil {
			_ = h.invalidateCache(session.Context(), kafkaMsg.Value)
		}
		// Только после успеха:
		session.MarkMessage(kafkaMsg, "")
		session.Commit()
	}
	return nil
}

func (h *flightGroupHandler) processWithRetry(ctx context.Context, m *sarama.ConsumerMessage) error {
	attempt := 0

	for {
		attempt++
		err := h.processOnce(ctx, m)
		if err == nil {
			return nil
		}

		backoff := retryBackoff(attempt)
		h.logger.Printf(
			"process kafka message failed topic=%s partition=%d offset=%d attempt=%d err=%v; retry in %s",
			m.Topic, m.Partition, m.Offset, attempt, err, backoff,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func (h *flightGroupHandler) processOnce(ctx context.Context, m *sarama.ConsumerMessage) error {
	if err := h.processor.ProcessFlightMessage(m.Value); err != nil {
		return fmt.Errorf("process message in service: %w", err)
	}
	return nil
}

func retryBackoff(attempt int) time.Duration {
	// линейный backoff 1..30 сек
	d := time.Duration(attempt) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (h *flightGroupHandler) invalidateCache(ctx context.Context, payload []byte) error {
	// достаём flight_number и departure_date из Kafka payload
	var x struct {
		FlightNumber  string    `json:"flight_number"`
		DepartureDate time.Time `json:"departure_date"`
	}
	if err := json.Unmarshal(payload, &x); err != nil {
		return err
	}
	if x.FlightNumber == "" || x.DepartureDate.IsZero() {
		return nil
	}

	// 1) удалить кеш конкретного рейса
	_ = h.cache.Del(ctx, cache.FlightDataKey(x.FlightNumber, x.DepartureDate))

	// 2) удалить все кеши meta по рейсу (через set ключей)
	setKey := cache.FlightMetaKeysSetKey(x.FlightNumber)
	keys, err := h.cache.SMembers(ctx, setKey)
	if err == nil && len(keys) > 0 {
		_ = h.cache.Del(ctx, keys...)
	}
	_ = h.cache.Del(ctx, setKey)

	return nil
}
