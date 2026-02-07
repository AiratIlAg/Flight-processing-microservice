package kafka

import (
	"encoding/json"
	"flight_processing/internal/models"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	cfg := sarama.NewConfig()

	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: p,
		topic:    topic,
	}, nil
}

func (p *Producer) SendFlightMessage(metaID int, req *models.FlightRequest) error {
	payload := FlightKafkaMessage{
		MetaID:  metaID,
		Request: *req,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = p.producer.SendMessage(msg)
	return err
}
