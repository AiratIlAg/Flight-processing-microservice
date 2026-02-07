package service

import "flight_processing/internal/models"

type KafkaJob struct {
	MetaID  int
	Request models.FlightRequest
}
