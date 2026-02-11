package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"flight_processing/internal/models"
	sq "github.com/Masterminds/squirrel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FlightRepository struct {
	db *pgxpool.Pool
	sb sq.StatementBuilderType
}

func NewFlightRepository(db *pgxpool.Pool) *FlightRepository {
	return &FlightRepository{
		db: db,
		sb: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
}

// Upsert(flight *FlightData) error - вставка или обновление по (flight_number, departure_date)
func (r *FlightRepository) Upsert(flight *models.FlightData) error {
	if flight == nil {
		return fmt.Errorf("flight is nil")
	}
	if flight.FlightNumber == "" {
		return fmt.Errorf("flight_number is empty")
	}
	if flight.PassengersCount < 0 {
		return fmt.Errorf("passengers_count must be >= 0")
	}

	query := r.sb.
		Insert("flights").
		Columns(
			"flight_number",
			"departure_date",
			"aircraft_type",
			"arrival_date",
			"passengers_count",
		).
		Values(
			flight.FlightNumber,
			flight.DepartureDate,
			flight.AircraftType,
			flight.ArrivalDate,
			flight.PassengersCount,
		).
		Suffix(`
ON CONFLICT (flight_number, departure_date)
DO UPDATE SET
	aircraft_type = EXCLUDED.aircraft_type,
	arrival_date = EXCLUDED.arrival_date,
	passengers_count = EXCLUDED.passengers_count,
	updated_at = NOW()
`)

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("build upsert flight sql: %w", err)
	}

	ctx := context.Background()
	if _, err := r.db.Exec(ctx, sqlStr, args...); err != nil {
		return fmt.Errorf("upsert flight: %w", err)
	}

	return nil
}

// Get(flightNumber string, departureDate time.Time) (*FlightData, error)
func (r *FlightRepository) Get(flightNumber string, departureDate time.Time) (*models.FlightData, error) {
	if flightNumber == "" {
		return nil, fmt.Errorf("flight_number is empty")
	}
	if departureDate.IsZero() {
		return nil, fmt.Errorf("departure_date is zero")
	}

	query := r.sb.
		Select(
			"aircraft_type",
			"flight_number",
			"departure_date",
			"arrival_date",
			"passengers_count",
			"updated_at",
		).
		From("flights").
		Where(sq.Eq{
			"flight_number":  flightNumber,
			"departure_date": departureDate,
		}).
		Limit(1)

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build get flight sql: %w", err)
	}

	ctx := context.Background()
	var f models.FlightData
	err = r.db.QueryRow(ctx, sqlStr, args...).Scan(
		&f.AircraftType,
		&f.FlightNumber,
		&f.DepartureDate,
		&f.ArrivalDate,
		&f.PassengersCount,
		&f.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get flight: %w", err)
	}

	return &f, nil
}

func (r *FlightRepository) UpsertTx(ctx context.Context, tx pgx.Tx, flight *models.FlightData) error {
	if flight == nil {
		return fmt.Errorf("flight is nil")
	}
	if flight.FlightNumber == "" {
		return fmt.Errorf("flight_number is empty")
	}
	if flight.PassengersCount < 0 {
		return fmt.Errorf("passengers_count must be >= 0")
	}

	query := r.sb.
		Insert("flights").
		Columns(
			"flight_number",
			"departure_date",
			"aircraft_type",
			"arrival_date",
			"passengers_count",
		).
		Values(
			flight.FlightNumber,
			flight.DepartureDate,
			flight.AircraftType,
			flight.ArrivalDate,
			flight.PassengersCount,
		).
		Suffix(`
ON CONFLICT (flight_number, departure_date)
DO UPDATE SET
	aircraft_type = EXCLUDED.aircraft_type,
	arrival_date = EXCLUDED.arrival_date,
	passengers_count = EXCLUDED.passengers_count,
	updated_at = NOW()
`)

	sqlStr, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("build upsert flight tx sql: %w", err)
	}

	if _, err := tx.Exec(ctx, sqlStr, args...); err != nil {
		return fmt.Errorf("upsert flight tx: %w", err)
	}

	return nil
}
