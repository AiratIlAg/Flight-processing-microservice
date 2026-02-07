package kafka

import (
	"flight_processing/internal/models"
)

type FlightKafkaMessage struct {
	MetaID  int                  `json:"meta_id"`
	Request models.FlightRequest `json:"request"`
}
