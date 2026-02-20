package handlers

import (
	"encoding/json"
	"errors"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"flight_processing/internal/service"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

// FlightService описывает методы сервисного слоя, которые нужны хендлерам.
type FlightService interface {
	CreateFlight(request *models.FlightRequest) (int, error)
	ProcessFlightMessage(message []byte) error
	GetFlight(flightNumber string, departureDate time.Time) (*models.FlightData, error)
	GetFlightMeta(flightNumber string, status string, limit int) (*models.FlightMetaResponse, error)
}

type FlightHandler struct {
	service FlightService
}

func NewFlightHandler(service FlightService) *FlightHandler {
	return &FlightHandler{service: service}
}

// POST /api/flights
// 201: { "id": int, "status": "pending" }
// 400: invalid input
// 500: internal error
func (h *FlightHandler) CreateFlight(w http.ResponseWriter, r *http.Request) {
	var req models.FlightRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	id, err := h.service.CreateFlight(&req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":     id,
		"status": repository.StatusPending,
	})
}

// GET /api/flights?flight_number=...&departure_date=...
// 200: { "flight_number": "...", "departure_date": "...", "passengers_count": 123 }
// 400: missing/invalid params
// 404: not found
// 500: internal error
func (h *FlightHandler) GetFlight(w http.ResponseWriter, r *http.Request) {
	flightNumber := strings.TrimSpace(r.URL.Query().Get("flight_number"))
	departureRaw := strings.TrimSpace(r.URL.Query().Get("departure_date"))

	if flightNumber == "" || departureRaw == "" {
		writeError(w, http.StatusBadRequest, "flight_number and departure_date are required")
		return
	}

	departureDate, err := time.Parse(time.RFC3339, departureRaw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid departure_date, expected RFC3339")
		return
	}

	flight, err := h.service.GetFlight(flightNumber, departureDate)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, repository.ErrNotFound):
			writeError(w, http.StatusNotFound, "flight not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to get flight")
		}
		return
	}

	// По ТЗ здесь возвращаем минимальный набор полей.
	writeJSON(w, http.StatusOK, map[string]any{
		"flight_number":    flight.FlightNumber,
		"departure_date":   flight.DepartureDate, // time.Time -> RFC3339 в JSON
		"passengers_count": flight.PassengersCount,
	})
}

// GET /api/flights/{flight_number}/meta?status=&limit=
// 200: { "flight_number": "...", "meta": [...], "pagination": {...} }
// 400: invalid params
// 500: internal error
func (h *FlightHandler) GetFlightMeta(w http.ResponseWriter, r *http.Request) {
	flightNumber := strings.TrimSpace(chi.URLParam(r, "flight_number"))
	if flightNumber == "" {
		writeError(w, http.StatusBadRequest, "flight_number is required")
		return
	}

	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))

	limit := 50
	if limitRaw := strings.TrimSpace(r.URL.Query().Get("limit")); limitRaw != "" {
		n, err := strconv.Atoi(limitRaw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 100 {
			n = 100
		}
		limit = n
	}

	resp, err := h.service.GetFlightMeta(flightNumber, status, limit)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to get flight meta")
		}
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	// Запрещаем второй JSON-объект в body
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("only one JSON object is allowed")
	}

	return nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
