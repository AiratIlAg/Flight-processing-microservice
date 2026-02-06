package service

import "time"

type FlightRequest struct {
	AircraftType    string    `json:"aircraft_type"`
	FlightNumber    string    `json:"flight_number"`
	DepartureDate   time.Time `json:"departure_date"`
	ArrivalDate     time.Time `json:"arrival_date"`
	PassengersCount int       `json:"passengers_count"`
}

type FlightMeta struct {
	ID            int       `db:"id"`
	FlightNumber  string    `db:"flight_number"`
	DepartureDate time.Time `db:"departure_date"`
	Status        string    `db:"status"` // pending, processed, error
	CreatedAt     time.Time `db:"created_at"`
	ProcessedAt   time.Time `db:"processed_at"`
}

type FlightData struct {
	AircraftType    string    `db:"aircraft_type"`
	FlightNumber    string    `db:"flight_number"`
	DepartureDate   time.Time `db:"departure_date"`
	ArrivalDate     time.Time `db:"arrival_date"`
	PassengersCount int       `db:"passengers_count"`
	UpdatedAt       time.Time `db:"updated_at"`
}
