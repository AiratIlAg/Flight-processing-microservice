package service

import (
	"flight_processing/internal/models"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//доказывает, что сервис не пропускает мусорные запросы и корректно возвращает ErrInvalidInput (это важно для 400 в handlers).

func TestCreateFlight_InvalidInput_ReturnsErrInvalidInput(t *testing.T) {
	// сервис без db/repos — нам важно проверить, что ошибка возникает ДО работы с db
	s := NewFlightService(nil, nil, nil, nil, "flight_requests", nil)

	validDeparture := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)
	validArrival := time.Date(2023, 10, 1, 15, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		req  *models.FlightRequest
	}{
		{"nil request", nil},
		{"empty aircraft_type", &models.FlightRequest{
			AircraftType:    "",
			FlightNumber:    "SU-123",
			DepartureDate:   validDeparture,
			ArrivalDate:     validArrival,
			PassengersCount: 10,
		}},
		{"empty flight_number", &models.FlightRequest{
			AircraftType:    "Boeing 737",
			FlightNumber:    "",
			DepartureDate:   validDeparture,
			ArrivalDate:     validArrival,
			PassengersCount: 10,
		}},
		{"zero departure_date", &models.FlightRequest{
			AircraftType:    "Boeing 737",
			FlightNumber:    "SU-123",
			DepartureDate:   time.Time{},
			ArrivalDate:     validArrival,
			PassengersCount: 10,
		}},
		{"zero arrival_date", &models.FlightRequest{
			AircraftType:    "Boeing 737",
			FlightNumber:    "SU-123",
			DepartureDate:   validDeparture,
			ArrivalDate:     time.Time{},
			PassengersCount: 10,
		}},
		{"arrival before departure", &models.FlightRequest{
			AircraftType:    "Boeing 737",
			FlightNumber:    "SU-123",
			DepartureDate:   validArrival,
			ArrivalDate:     validDeparture,
			PassengersCount: 10,
		}},
		{"negative passengers", &models.FlightRequest{
			AircraftType:    "Boeing 737",
			FlightNumber:    "SU-123",
			DepartureDate:   validDeparture,
			ArrivalDate:     validArrival,
			PassengersCount: -1,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CreateFlight(tc.req)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidInput)
		})
	}
}

//доказывает, что consumer не упадёт с panic и не полезет в БД, если сообщение сломано/неполное — вернёт ошибку,
//и Kafka message будет перечитан (пока не исправишь источник данных).

func TestProcessFlightMessage_InvalidPayload_ReturnsError(t *testing.T) {
	// сервис без db/repos — нам важно, что упадёт на Unmarshal/валидации ДО работы с db
	s := NewFlightService(nil, nil, nil, nil, "flight_requests", nil)

	t.Run("invalid json", func(t *testing.T) {
		err := s.ProcessFlightMessage([]byte(`{bad json`))
		require.Error(t, err)
	})

	t.Run("missing required fields", func(t *testing.T) {
		// meta_id отсутствует/0 и flight_number пустой
		err := s.ProcessFlightMessage([]byte(`{
      "meta_id": 0,
      "flight_number": "",
      "departure_date": "2023-10-01T12:00:00Z",
      "arrival_date": "2023-10-01T15:00:00Z",
      "aircraft_type": "Boeing 737",
      "passengers_count": 10
    }`))
		require.Error(t, err)
	})

	t.Run("missing departure_date", func(t *testing.T) {
		err := s.ProcessFlightMessage([]byte(`{
      "meta_id": 123,
      "flight_number": "SU-123",
      "arrival_date": "2023-10-01T15:00:00Z",
      "aircraft_type": "Boeing 737",
      "passengers_count": 10
    }`))
		require.Error(t, err)
	})
}
