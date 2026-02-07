package repository

import (
	"context"
	"flight_processing/internal/models"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type MetaRepository struct {
	db *pgxpool.Pool
}

func NewMetaRepository(db *pgxpool.Pool) *MetaRepository {
	return &MetaRepository{db: db}
}

func (r *MetaRepository) Create(ctx context.Context, meta *models.FlightMeta) error {
	query, args, err := psql.
		Insert("flight_meta").
		Columns(
			"flight_number",
			"departure_date",
			"status",
			"created_at",
		).
		Values(
			meta.FlightNumber,
			meta.DepartureDate,
			"pending",
			time.Now(),
		).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return err
	}

	return r.db.QueryRow(ctx, query, args...).Scan(&meta.ID)
}

func (r *MetaRepository) UpdateStatus(ctx context.Context, id int, status string) error {
	query, args, err := psql.
		Update("flight_meta").
		Set("status", status).
		Set("processed_at", time.Now()).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, query, args...)
	return err
}

func (r *MetaRepository) GetByFlightNumber(
	ctx context.Context,
	flightNumber string,
	status string,
	limit int,
	offset int,
) ([]*models.FlightMeta, int, error) {

	builder := psql.
		Select(
			"id",
			"flight_number",
			"departure_date",
			"status",
			"created_at",
			"processed_at",
		).
		From("flight_meta").
		Where(sq.Eq{"flight_number": flightNumber}).
		OrderBy("created_at DESC").
		Limit(uint64(limit)).
		Offset(uint64(offset))

	if status != "" {
		builder = builder.Where(sq.Eq{"status": status})
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*models.FlightMeta

	for rows.Next() {
		var m models.FlightMeta
		err := rows.Scan(
			&m.ID,
			&m.FlightNumber,
			&m.DepartureDate,
			&m.Status,
			&m.CreatedAt,
			&m.ProcessedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, &m)
	}

	countBuilder := psql.
		Select("count(*)").
		From("flight_meta").
		Where(sq.Eq{"flight_number": flightNumber})

	if status != "" {
		countBuilder = countBuilder.Where(sq.Eq{"status": status})
	}

	countQuery, countArgs, err := countBuilder.ToSql()
	if err != nil {
		return nil, 0, err
	}

	var total int
	err = r.db.QueryRow(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	return result, total, nil
}
