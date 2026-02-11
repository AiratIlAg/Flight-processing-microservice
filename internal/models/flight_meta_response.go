package models

import "time"

type FlightMetaItemResponse struct {
	ID            int        `json:"id"`
	FlightNumber  string     `json:"flight_number"`
	DepartureDate time.Time  `json:"departure_date"`
	Status        string     `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	ProcessedAt   *time.Time `json:"processed_at"`
}

type Pagination struct {
	Total int `json:"total"`
	Limit int `json:"limit"`
}

type FlightMetaResponse struct {
	FlightNumber string                   `json:"flight_number"`
	Meta         []FlightMetaItemResponse `json:"meta"`
	Pagination   Pagination               `json:"pagination"`
}
