package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"flight_processing/internal/models"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OutboxStatusPending = "pending"
	OutboxStatusSent    = "sent"
	OutboxStatusFailed  = "failed"
)

type OutboxRepository struct {
	db         *pgxpool.Pool
	sb         sq.StatementBuilderType
	maxRetries int
}

func NewOutboxRepository(db *pgxpool.Pool, maxRetries int) *OutboxRepository {
	if maxRetries <= 0 {
		maxRetries = 10
	}
	return &OutboxRepository{
		db:         db,
		sb:         sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		maxRetries: maxRetries,
	}
}

// CreateMessage(tx, message) – сохранить сообщение в outbox в рамках транзакции.
func (r *OutboxRepository) CreateMessage(ctx context.Context, tx pgx.Tx, msg *models.OutboxMessage) error {
	if msg == nil {
		return fmt.Errorf("outbox message is nil")
	}
	if msg.Topic == "" {
		return fmt.Errorf("topic is empty")
	}
	if len(msg.Payload) == 0 {
		return fmt.Errorf("payload is empty")
	}
	if !json.Valid(msg.Payload) {
		return fmt.Errorf("payload is not valid json")
	}

	q := r.sb.
		Insert("outbox_messages").
		Columns("topic", "payload", "status", "retry_count").
		Values(msg.Topic, msg.Payload, OutboxStatusPending, 0).
		Suffix("RETURNING id, message_id::text, created_at")

	sqlStr, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build outbox insert: %w", err)
	}

	var id int64
	if err := tx.QueryRow(ctx, sqlStr, args...).Scan(&id, &msg.MessageID, &msg.CreatedAt); err != nil {
		return fmt.Errorf("insert outbox message: %w", err)
	}

	msg.ID = int(id)
	msg.Status = OutboxStatusPending
	msg.RetryCount = 0
	msg.SentAt = nil
	msg.LastError = nil
	return nil
}

// GetPendingMessages(limit) – получить limit сообщений со статусом pending, сортируя по created_at.
func (r *OutboxRepository) GetPendingMessages(limit int) ([]*models.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}

	q := r.sb.
		Select(
			"id",
			"message_id::text",
			"topic",
			"payload",
			"status",
			"retry_count",
			"created_at",
			"sent_at",
			"last_error",
		).
		From("outbox_messages").
		Where(sq.Eq{"status": OutboxStatusPending}).
		OrderBy("created_at ASC", "id ASC").
		Limit(uint64(limit))

	sqlStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build outbox select pending: %w", err)
	}

	ctx := context.Background()
	rows, err := r.db.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("query outbox pending: %w", err)
	}
	defer rows.Close()

	res := make([]*models.OutboxMessage, 0, limit)

	for rows.Next() {
		var (
			m         models.OutboxMessage
			id        int64
			payload   []byte
			sentAt    pgtype.Timestamptz
			lastError pgtype.Text
		)

		if err := rows.Scan(
			&id,
			&m.MessageID,
			&m.Topic,
			&payload,
			&m.Status,
			&m.RetryCount,
			&m.CreatedAt,
			&sentAt,
			&lastError,
		); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}

		m.ID = int(id)
		m.Payload = payload

		if sentAt.Valid {
			t := sentAt.Time
			m.SentAt = &t
		}
		if lastError.Valid {
			s := lastError.String
			m.LastError = &s
		}

		res = append(res, &m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}

	return res, nil
}

// MarkAsSent(messageID) – статус sent, sent_at, retry_count=0.
func (r *OutboxRepository) MarkAsSent(messageID string) error {
	if messageID == "" {
		return fmt.Errorf("messageID is empty")
	}

	q := r.sb.
		Update("outbox_messages").
		Set("status", OutboxStatusSent).
		Set("sent_at", sq.Expr("NOW()")).
		Set("retry_count", 0).
		Set("last_error", nil).
		Where(sq.Eq{"message_id": messageID})

	sqlStr, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build outbox mark sent: %w", err)
	}

	ctx := context.Background()
	tag, err := r.db.Exec(ctx, sqlStr, args...)
	if err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkAsFailed(messageID, errorMsg) – retry_count++, last_error, если превышен лимит -> failed.
func (r *OutboxRepository) MarkAsFailed(messageID string, errorMsg string) error {
	if messageID == "" {
		return fmt.Errorf("messageID is empty")
	}
	if errorMsg == "" {
		errorMsg = "unknown error"
	}

	q := r.sb.
		Update("outbox_messages").
		Set("retry_count", sq.Expr("retry_count + 1")).
		Set("last_error", errorMsg).
		Set("status", sq.Expr(
			"CASE WHEN (retry_count + 1) >= ? THEN ? ELSE ? END",
			r.maxRetries, OutboxStatusFailed, OutboxStatusPending,
		)).
		Where(sq.Eq{"message_id": messageID})

	sqlStr, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build outbox mark failed: %w", err)
	}

	ctx := context.Background()
	tag, err := r.db.Exec(ctx, sqlStr, args...)
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// CleanupOldMessages(retentionDays) – удалить sent старше N дней.
func (r *OutboxRepository) CleanupOldMessages(retentionDays int) (int, error) {
	if retentionDays <= 0 {
		return 0, nil
	}

	q := r.sb.
		Delete("outbox_messages").
		Where(sq.Eq{"status": OutboxStatusSent}).
		Where(sq.Expr("created_at < NOW() - (? * INTERVAL '1 day')", retentionDays))

	sqlStr, args, err := q.ToSql()
	if err != nil {
		return 0, fmt.Errorf("build outbox cleanup: %w", err)
	}

	ctx := context.Background()
	tag, err := r.db.Exec(ctx, sqlStr, args...)
	if err != nil {
		return 0, fmt.Errorf("cleanup outbox: %w", err)
	}

	return int(tag.RowsAffected()), nil
}
