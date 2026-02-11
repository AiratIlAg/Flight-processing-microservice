package kafka

import (
	"flight_processing/internal/models"
	"time"
)

//делаем ДТО. В Kafka отправляем не только FlightRequest, но и meta_id, чтобы consumer мог обновить статус конкретной записи flight_meta.

type FlightMessage struct {
	MetaID          int       `json:"meta_id"`
	AircraftType    string    `json:"aircraft_type"`
	FlightNumber    string    `json:"flight_number"`
	DepartureDate   time.Time `json:"departure_date"`
	ArrivalDate     time.Time `json:"arrival_date"`
	PassengersCount int       `json:"passengers_count"`
}

func NewFlightMessage(metaID int, req *models.FlightRequest) *FlightMessage {
	return &FlightMessage{
		MetaID:          metaID,
		AircraftType:    req.AircraftType,
		FlightNumber:    req.FlightNumber,
		DepartureDate:   req.DepartureDate,
		ArrivalDate:     req.ArrivalDate,
		PassengersCount: req.PassengersCount,
	}
}

func (m *FlightMessage) ToFlightData() *models.FlightData {
	return &models.FlightData{
		AircraftType:    m.AircraftType,
		FlightNumber:    m.FlightNumber,
		DepartureDate:   m.DepartureDate,
		ArrivalDate:     m.ArrivalDate,
		PassengersCount: m.PassengersCount,
	}
}
