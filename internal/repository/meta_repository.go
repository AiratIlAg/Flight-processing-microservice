package repository

import (
	"context"
	"fmt"

	"flight_processing/internal/models"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending   = "pending"
	StatusProcessed = "processed"
	StatusError     = "error"
)

var allowedStatuses = map[string]struct{}{
	StatusPending:   {},
	StatusProcessed: {},
	StatusError:     {},
}

type MetaRepository struct {
	db *pgxpool.Pool
	sb sq.StatementBuilderType
}

func NewMetaRepository(db *pgxpool.Pool) *MetaRepository {
	return &MetaRepository{
		db: db,
		sb: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
}

// Create(meta *FlightMeta) error - вставка с статусом "pending"
func (r *MetaRepository) Create(meta *models.FlightMeta) error {
	if meta == nil {
		return fmt.Errorf("meta is nil")
	}
	if meta.FlightNumber == "" {
		return fmt.Errorf("flight_number is empty")
	}

	meta.Status = StatusPending

	query := r.sb.
		Insert("flight_meta").
		Columns("flight_number", "departure_date", "status").
		Values(meta.FlightNumber, meta.DepartureDate, StatusPending).
		Suffix("RETURNING id, created_at")

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("build create meta sql: %w", err)
	}

	ctx := context.Background()
	var id int64
	if err := r.db.QueryRow(ctx, sqlStr, args...).Scan(&id, &meta.CreatedAt); err != nil {
		return fmt.Errorf("create meta: %w", err)
	}

	meta.ID = int(id)
	meta.ProcessedAt = nil

	return nil
}

// UpdateStatus(id int, status string) error - обновление статуса и processed_at
func (r *MetaRepository) UpdateStatus(id int, status string) error {
	if id <= 0 {
		return fmt.Errorf("invalid id")
	}
	if _, ok := allowedStatuses[status]; !ok {
		return fmt.Errorf("invalid status: %s", status)
	}

	query := r.sb.
		Update("flight_meta").
		Set("status", status).
		Set("processed_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id})

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("build update meta status sql: %w", err)
	}

	ctx := context.Background()
	tag, err := r.db.Exec(ctx, sqlStr, args...)
	if err != nil {
		return fmt.Errorf("update meta status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// GetByFlightNumber(flightNumber, status, limit, offset) ([]*FlightMeta, int, error)
func (r *MetaRepository) GetByFlightNumber(
	flightNumber string,
	status string,
	limit int,
	offset int,
) ([]*models.FlightMeta, int, error) {
	if flightNumber == "" {
		return nil, 0, fmt.Errorf("flight_number is empty")
	}
	if status != "" {
		if _, ok := allowedStatuses[status]; !ok {
			return nil, 0, fmt.Errorf("invalid status: %s", status)
		}
	}

	filters := sq.And{
		sq.Eq{"flight_number": flightNumber},
	}
	if status != "" {
		filters = append(filters, sq.Eq{"status": status})
	}

	// 1) count
	countQuery := r.sb.
		Select("COUNT(*)").
		From("flight_meta").
		Where(filters)

	countSQL, countArgs, err := countQuery.ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("build count meta sql: %w", err)
	}

	ctx := context.Background()

	var total int64
	if err := r.db.QueryRow(ctx, countSQL, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count meta rows: %w", err)
	}

	// 2) data
	dataQuery := r.sb.
		Select("id", "flight_number", "departure_date", "status", "created_at", "processed_at").
		From("flight_meta").
		Where(filters).
		OrderBy("created_at DESC", "id DESC")

	if limit > 0 {
		dataQuery = dataQuery.Limit(uint64(limit))
	}
	if offset > 0 {
		dataQuery = dataQuery.Offset(uint64(offset))
	}

	dataSQL, dataArgs, err := dataQuery.ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("build select meta sql: %w", err)
	}

	rows, err := r.db.Query(ctx, dataSQL, dataArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query meta rows: %w", err)
	}
	defer rows.Close()

	result := make([]*models.FlightMeta, 0)

	for rows.Next() {
		var (
			m         models.FlightMeta
			id        int64
			processed pgtype.Timestamptz
		)

		if err := rows.Scan(
			&id,
			&m.FlightNumber,
			&m.DepartureDate,
			&m.Status,
			&m.CreatedAt,
			&processed,
		); err != nil {
			return nil, 0, fmt.Errorf("scan meta row: %w", err)
		}

		m.ID = int(id)
		if processed.Valid {
			t := processed.Time
			m.ProcessedAt = &t
		} else {
			m.ProcessedAt = nil
		}

		result = append(result, &m)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate meta rows: %w", err)
	}

	return result, int(total), nil
}

func (r *MetaRepository) UpdateStatusTx(ctx context.Context, tx pgx.Tx, id int, status string) error {
	if id <= 0 {
		return fmt.Errorf("invalid id")
	}
	if _, ok := allowedStatuses[status]; !ok {
		return fmt.Errorf("invalid status: %s", status)
	}

	query := r.sb.
		Update("flight_meta").
		Set("status", status).
		Set("processed_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id})

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("build update meta status tx sql: %w", err)
	}

	tag, err := tx.Exec(ctx, sqlStr, args...)
	if err != nil {
		return fmt.Errorf("update meta status tx: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}
