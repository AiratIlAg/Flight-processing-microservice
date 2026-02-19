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

	msg := NewFlightMessage(metaID, request)
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal flight message: %w", err)
	}

	return p.SendRaw(p.topic, request.FlightNumber, b)
}

// SendRaw отправляет уже сформированный payload в указанный topic.
// key можно передать пустым, но лучше (для порядка по ключу) использовать flight_number.
func (p *Producer) SendRaw(topic string, key string, payload []byte) error {
	if topic == "" {
		topic = p.topic
	}
	if len(payload) == 0 {
		return fmt.Errorf("payload is empty")
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.Now(),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	_, _, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send kafka message: %w", err)
	}
	return nil
}
