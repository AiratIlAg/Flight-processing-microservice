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

type mockOutboxRepo struct{ mock.Mock }

func (m *mockOutboxRepo) GetPendingMessages(limit int) ([]*models.OutboxMessage, error) {
	args := m.Called(limit)
	res, _ := args.Get(0).([]*models.OutboxMessage)
	return res, args.Error(1)
}
func (m *mockOutboxRepo) MarkAsSent(messageID string) error {
	args := m.Called(messageID)
	return args.Error(0)
}
func (m *mockOutboxRepo) MarkAsFailed(messageID string, errorMsg string) error {
	args := m.Called(messageID, errorMsg)
	return args.Error(0)
}
func (m *mockOutboxRepo) CleanupOldMessages(days int) (int, error) {
	args := m.Called(days)
	return args.Int(0), args.Error(1)
}

type mockProducer struct{ mock.Mock }

func (m *mockProducer) SendRaw(topic, key string, payload []byte) error {
	args := m.Called(topic, key, payload)
	return args.Error(0)
}

func TestOutboxSender_SendSuccess_MarksSent(t *testing.T) {
	repo := new(mockOutboxRepo)
	prod := new(mockProducer)

	msg := &models.OutboxMessage{
		MessageID:  "uuid-1",
		Topic:      "flight_requests",
		Payload:    json.RawMessage(`{"flight_number":"SU-123"}`),
		RetryCount: 0,
		CreatedAt:  time.Now().Add(-2 * time.Second),
	}

	repo.On("GetPendingMessages", 10).Return([]*models.OutboxMessage{msg}, nil).Once()
	prod.On("SendRaw", "flight_requests", "SU-123", []byte(msg.Payload)).Return(nil).Once()
	repo.On("MarkAsSent", "uuid-1").Return(nil).Once()

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)

	// вызываем одну итерацию напрямую (тест в пакете service может вызывать приватные методы)
	s.flushOnce()

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestOutboxSender_SendFail_MarksFailed(t *testing.T) {
	repo := new(mockOutboxRepo)
	prod := new(mockProducer)

	msg := &models.OutboxMessage{
		MessageID:  "uuid-2",
		Topic:      "flight_requests",
		Payload:    json.RawMessage(`{"flight_number":"SU-999"}`),
		RetryCount: 1,
		CreatedAt:  time.Now().Add(-5 * time.Second),
	}

	repo.On("GetPendingMessages", 10).Return([]*models.OutboxMessage{msg}, nil).Once()
	prod.On("SendRaw", "flight_requests", "SU-999", []byte(msg.Payload)).Return(errors.New("kafka down")).Once()
	repo.On("MarkAsFailed", "uuid-2", mock.Anything).Return(nil).Once()

	s := NewOutboxSender(repo, prod, 100*time.Millisecond, 10, 7, 10, nil)
	s.flushOnce()

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
	require.True(t, true)
}
