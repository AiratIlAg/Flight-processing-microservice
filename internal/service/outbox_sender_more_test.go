package service

import (
	"encoding/json"
	"errors"
	"flight_processing/internal/models"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Отдельные имена моков, чтобы не конфликтовать с уже существующими в других *_test.go.
type outboxRepoMockMore struct{ mock.Mock }

func (m *outboxRepoMockMore) GetPendingMessages(limit int) ([]*models.OutboxMessage, error) {
	args := m.Called(limit)
	res, _ := args.Get(0).([]*models.OutboxMessage)
	return res, args.Error(1)
}
func (m *outboxRepoMockMore) MarkAsSent(messageID string) error {
	args := m.Called(messageID)
	return args.Error(0)
}
func (m *outboxRepoMockMore) MarkAsFailed(messageID string, errorMsg string) error {
	args := m.Called(messageID, errorMsg)
	return args.Error(0)
}
func (m *outboxRepoMockMore) CleanupOldMessages(retentionDays int) (int, error) {
	args := m.Called(retentionDays)
	return args.Int(0), args.Error(1)
}

type producerMockMore struct{ mock.Mock }

func (m *producerMockMore) SendRaw(topic string, key string, payload []byte) error {
	args := m.Called(topic, key, payload)
	return args.Error(0)
}

// 13) payload невалидный/нет flight_number -> MarkAsFailed, SendRaw НЕ вызывается
func TestOutboxSender_InvalidPayload_NoFlightNumber_MarksFailed(t *testing.T) {
	repo := new(outboxRepoMockMore)
	prod := new(producerMockMore)

	msg := &models.OutboxMessage{
		MessageID:  "uuid-13",
		Topic:      "flight_requests",
		Payload:    json.RawMessage(`{"foo":"bar"}`), // нет flight_number
		RetryCount: 0,
		CreatedAt:  time.Now().Add(-2 * time.Second),
	}

	repo.On("GetPendingMessages", 10).Return([]*models.OutboxMessage{msg}, nil).Once()

	// ждём MarkAsFailed и проверяем, что текст ошибки содержит flight_number
	repo.On("MarkAsFailed", "uuid-13", mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) {
			errMsg := args.String(1)
			require.Contains(t, errMsg, "flight_number", "error message should mention flight_number")
		}).
		Return(nil).Once()

	// producer.SendRaw НЕ должен вызываться
	prod.AssertNotCalled(t, "SendRaw", mock.Anything, mock.Anything, mock.Anything)

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)
	s.flushOnce()

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

// 14) GetPendingMessages вернул ошибку -> sender не падает и не вызывает producer/mark-методы
func TestOutboxSender_GetPendingMessagesError_DoesNotPanicOrSend(t *testing.T) {
	repo := new(outboxRepoMockMore)
	prod := new(producerMockMore)

	repo.On("GetPendingMessages", 10).Return([]*models.OutboxMessage(nil), errors.New("db error")).Once()

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)
	require.NotPanics(t, func() { s.flushOnce() })

	// никаких отправок/маркировок не должно быть
	prod.AssertNotCalled(t, "SendRaw", mock.Anything, mock.Anything, mock.Anything)
	repo.AssertNotCalled(t, "MarkAsSent", mock.Anything)
	repo.AssertNotCalled(t, "MarkAsFailed", mock.Anything, mock.Anything)

	repo.AssertExpectations(t)
}

// 15) Cleanup вызывается, а ошибка cleanup не “валит” sender
func TestOutboxSender_CleanupError_DoesNotPanic(t *testing.T) {
	repo := new(outboxRepoMockMore)
	prod := new(producerMockMore)

	// retentionDays=7 -> cleanupOnce должен вызвать CleanupOldMessages(7)
	repo.On("CleanupOldMessages", 7).Return(0, errors.New("cleanup error")).Once()

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)
	require.NotPanics(t, func() { s.cleanupOnce() })

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

// 16) Batch: если вернулось N сообщений -> SendRaw и MarkAsSent вызываются N раз
func TestOutboxSender_BatchSendsAllMessages(t *testing.T) {
	repo := new(outboxRepoMockMore)
	prod := new(producerMockMore)
	msg1 := &models.OutboxMessage{
		MessageID: "uuid-16-1",
		Topic:     "flight_requests",
		Payload:   json.RawMessage(`{"flight_number":"SU-101"}`),
		CreatedAt: time.Now(),
	}
	msg2 := &models.OutboxMessage{
		MessageID: "uuid-16-2",
		Topic:     "flight_requests",
		Payload:   json.RawMessage(`{"flight_number":"SU-202"}`),
		CreatedAt: time.Now(),
	}
	msg3 := &models.OutboxMessage{
		MessageID: "uuid-16-3",
		Topic:     "flight_requests",
		Payload:   json.RawMessage(`{"flight_number":"SU-303"}`),
		CreatedAt: time.Now(),
	}

	repo.On("GetPendingMessages", 10).Return([]*models.OutboxMessage{msg1, msg2, msg3}, nil).Once()

	// Producer sends
	prod.On("SendRaw", "flight_requests", "SU-101", []byte(msg1.Payload)).Return(nil).Once()
	prod.On("SendRaw", "flight_requests", "SU-202", []byte(msg2.Payload)).Return(nil).Once()
	prod.On("SendRaw", "flight_requests", "SU-303", []byte(msg3.Payload)).Return(nil).Once()

	// Mark sent for each
	repo.On("MarkAsSent", "uuid-16-1").Return(nil).Once()
	repo.On("MarkAsSent", "uuid-16-2").Return(nil).Once()
	repo.On("MarkAsSent", "uuid-16-3").Return(nil).Once()

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)
	s.flushOnce()

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}
