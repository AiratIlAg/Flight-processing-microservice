package models

import (
	"encoding/json"
	"time"
)

type OutboxMessage struct {
	ID        int             `db:"id"`
	MessageID string          `db:"message_id"` // UUID
	Topic     string          `db:"topic"`
	Payload   json.RawMessage `db:"payload"` // JSON (хранится как JSONB)

	Status     string     `db:"status"` // pending, sent, failed
	RetryCount int        `db:"retry_count"`
	CreatedAt  time.Time  `db:"created_at"`
	SentAt     *time.Time `db:"sent_at"` // может быть NULL, пока не отправили
	LastError  *string    `db:"last_error"`
}

//Почему не Payload string и не SentAt time.Time как в ТЗ?
//Потому что в БД sent_at и last_error будут NULL до отправки/ошибки — с time.Time/string будут проблемы при чтении.
//Это нормальная практическая адаптация под PostgreSQL.
