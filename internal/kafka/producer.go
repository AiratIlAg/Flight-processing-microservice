package kafka

import (
	"encoding/json"
	"flight_processing/internal/models"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	topic    string
	producer sarama.SyncProducer
}

func NewSyncProducer(brokers []string, topic string) (*Producer, error) {
	cfg := sarama.NewConfig()

	// SyncProducer обязательно:
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 500 * time.Millisecond

	// Для локального стенда обычно так ок.
	// При необходимости можно задать cfg.Version явно.
	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create sarama sync producer: %w", err)
	}

	return &Producer{
		topic:    topic,
		producer: prod,
	}, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

// SendFlightMessage(metaID int, request *FlightRequest) error
func (p *Producer) SendFlightMessage(metaID int, request *models.FlightRequest) error {
	if request == nil {
		return fmt.Errorf("request is nil")
	}
	if metaID <= 0 {
		return fmt.Errorf("invalid metaID")
	}

	payload := NewFlightMessage(metaID, request)
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal flight message: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(request.FlightNumber),
		Value:     sarama.ByteEncoder(b),
		Timestamp: time.Now(),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send kafka message: %w", err)
	}

	return nil
}
