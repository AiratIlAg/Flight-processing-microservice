package service

import (
	"context"
	"encoding/json"
	"flight_processing/internal/kafka"
	"flight_processing/internal/metrics"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"fmt"
	"log"
	"time"
)

type OutboxSender struct {
	repo          *repository.OutboxRepository
	producer      *kafka.Producer
	pollInterval  time.Duration
	batchSize     int
	retentionDays int
	maxRetries    int
	logger        *log.Logger

	cleanupEvery time.Duration
}

func NewOutboxSender(
	repo *repository.OutboxRepository,
	producer *kafka.Producer,
	pollInterval time.Duration,
	batchSize int,
	retentionDays int,
	maxRetries int,
	logger *log.Logger,
) *OutboxSender {
	if maxRetries <= 0 {
		maxRetries = 10
	}
	if logger == nil {
		logger = log.Default()
	}
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if retentionDays < 0 {
		retentionDays = 0
	}

	return &OutboxSender{
		repo:          repo,
		producer:      producer,
		pollInterval:  pollInterval,
		batchSize:     batchSize,
		retentionDays: retentionDays,
		maxRetries:    maxRetries,
		logger:        logger,
		// чистку делаем реже, чтобы не дёргать БД постоянно
		cleanupEvery: 1 * time.Hour,
	}
}

// Start запускает фоновую горутину.
func (s *OutboxSender) Start(ctx context.Context) {
	go func() {
		s.logger.Println("outbox sender started")
		defer s.logger.Println("outbox sender stopped")

		ticker := time.NewTicker(s.pollInterval)
		defer ticker.Stop()

		cleanupTicker := time.NewTicker(s.cleanupEvery)
		defer cleanupTicker.Stop()

		// можно сделать первый запуск сразу
		s.flushOnce()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.flushOnce()
			case <-cleanupTicker.C:
				s.cleanupOnce()
			}
		}
	}()
}

func (s *OutboxSender) flushOnce() {
	msgs, err := s.repo.GetPendingMessages(s.batchSize)
	if err != nil {
		s.logger.Printf("outbox get pending failed: %v", err)
		return
	}
	if len(msgs) == 0 {
		return
	}

	for _, m := range msgs {
		if err := s.sendOne(m); err != nil {
			// увеличиваем retry_count и сохраняем ошибку; repo сам поставит failed, если лимит превышен
			if err2 := s.repo.MarkAsFailed(m.MessageID, err.Error()); err2 != nil {
				s.logger.Printf("outbox mark failed error: %v", err2)
			}
			// METRICS: если после этого инкремента retry_count достигает лимита -> это final failed
			if m.RetryCount+1 >= s.maxRetries {
				metrics.IncOutboxFailed()
			}
			continue
		}
		if err := s.repo.MarkAsSent(m.MessageID); err != nil {
			s.logger.Printf("outbox mark sent failed: %v", err)
		}
	}
}

func (s *OutboxSender) sendOne(m *models.OutboxMessage) error {
	if m == nil {
		return fmt.Errorf("outbox message is nil")
	}
	if m.Topic == "" {
		return fmt.Errorf("outbox topic is empty")
	}
	if len(m.Payload) == 0 {
		return fmt.Errorf("outbox payload is empty")
	}

	// --- METRICS: outbox lag (сколько лежало в outbox до попытки отправки)
	// created_at -> сейчас
	metrics.ObserveOutboxLagSeconds(time.Since(m.CreatedAt).Seconds())

	start := time.Now()

	// Чтобы выставить Kafka key, достанем flight_number из JSON.
	// payload у нас будет JSON сообщения (обычно FlightMessage), где есть "flight_number".
	key, err := extractFlightNumber(m.Payload)
	if err != nil {
		// --- METRICS: это ошибка подготовки/парсинга payload
		metrics.IncKafkaError("producer", "prepare")
		metrics.ObserveOutboxProcessing(time.Since(start))
		return fmt.Errorf("extract flight_number: %w", err)
	}

	// отправляем в Kafka
	if err := s.producer.SendRaw(m.Topic, key, m.Payload); err != nil {
		// --- METRICS: ошибка отправки
		metrics.IncKafkaError("producer", "send")
		metrics.IncOutboxRetry()
		metrics.ObserveOutboxProcessing(time.Since(start))

		return fmt.Errorf("kafka send failed: %w", err)
	}

	// --- METRICS: успешная отправка
	metrics.IncKafkaSent()
	metrics.IncOutboxSent()
	metrics.ObserveOutboxProcessing(time.Since(start))

	return nil
}

func (s *OutboxSender) cleanupOnce() {
	if s.retentionDays <= 0 {
		return
	}
	n, err := s.repo.CleanupOldMessages(s.retentionDays)
	if err != nil {
		s.logger.Printf("outbox cleanup failed: %v", err)
		return
	}
	if n > 0 {
		s.logger.Printf("outbox cleanup: deleted %d messages", n)
	}
}

// payload может быть json.RawMessage ([]byte) или []byte — тут ожидаем []byte.
func extractFlightNumber(payload []byte) (string, error) {
	var x struct {
		FlightNumber string `json:"flight_number"`
	}
	if err := json.Unmarshal(payload, &x); err != nil {
		return "", err
	}
	if x.FlightNumber == "" {
		return "", fmt.Errorf("flight_number is empty in payload")
	}
	return x.FlightNumber, nil
}
