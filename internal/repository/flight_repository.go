package repository

import (
	"context"
	"flight_processing/internal/models"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FlightRepository struct {
	db *pgxpool.Pool
}

func NewFlightRepository(db *pgxpool.Pool) *FlightRepository {
	return &FlightRepository{db: db}
}

func (r *FlightRepository) Upsert(ctx context.Context, f *models.FlightData) error {
	query, args, err := psql.
		Insert("flights").
		Columns(
			"flight_number",
			"departure_date",
			"aircraft_type",
			"arrival_date",
			"passengers_count",
			"updated_at",
		).
		Values(
			f.FlightNumber,
			f.DepartureDate,
			f.AircraftType,
			f.ArrivalDate,
			f.PassengersCount,
			time.Now(),
		).
		Suffix(`
			ON CONFLICT (flight_number, departure_date)
			DO UPDATE SET
				aircraft_type = EXCLUDED.aircraft_type,
				arrival_date = EXCLUDED.arrival_date,
				passengers_count = EXCLUDED.passengers_count,
				updated_at = EXCLUDED.updated_at
		`).
		ToSql()
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, query, args...)
	return err
}

func (r *FlightRepository) Get(ctx context.Context, flightNumber string, departureDate time.Time) (*models.FlightData, error) {
	query, args, err := psql.
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
		ToSql()
	if err != nil {
		return nil, err
	}

	var f models.FlightData
	err = r.db.QueryRow(ctx, query, args...).Scan(
		&f.AircraftType,
		&f.FlightNumber,
		&f.DepartureDate,
		&f.ArrivalDate,
		&f.PassengersCount,
		&f.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}
	return &f, nil
}
