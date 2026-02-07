package handlers

import (
	"encoding/json"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"flight_processing/internal/service"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

type FlightHandler struct {
	metaRepo   *repository.MetaRepository   // Репозиторий для метаданных
	flightRepo *repository.FlightRepository // Репозиторий для данных о рейсах
	kafkaChan  chan service.KafkaJob        // Канал для асинхронной отправки в Kafka
}

func NewFlightHandler(
	metaRepo *repository.MetaRepository,
	flightRepo *repository.FlightRepository,
	kafkaChan chan service.KafkaJob,
) *FlightHandler {
	return &FlightHandler{
		metaRepo:   metaRepo,
		flightRepo: flightRepo,
		kafkaChan:  kafkaChan,
	}
}

func (h *FlightHandler) CreateFlight(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req models.FlightRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	// базовая валидация
	if req.FlightNumber == "" || req.AircraftType == "" || req.PassengersCount <= 0 {
		http.Error(w, "invalid data", http.StatusBadRequest)
		return
	}

	meta := &models.FlightMeta{
		FlightNumber:  req.FlightNumber,
		DepartureDate: req.DepartureDate,
	}

	if err := h.metaRepo.Create(ctx, meta); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// async отправка в kafka через канал
	h.kafkaChan <- service.KafkaJob{
		MetaID:  meta.ID,
		Request: req,
	}

	resp := map[string]any{
		"id":     meta.ID,
		"status": "pending",
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *FlightHandler) GetFlight(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	fn := r.URL.Query().Get("flight_number")
	ds := r.URL.Query().Get("departure_date")

	if fn == "" || ds == "" {
		http.Error(w, "missing params", http.StatusBadRequest)
		return
	}

	dt, err := time.Parse(time.RFC3339, ds)
	if err != nil {
		http.Error(w, "bad date", http.StatusBadRequest)
		return
	}

	f, err := h.flightRepo.Get(ctx, fn, dt)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	resp := map[string]any{
		"flight_number":    f.FlightNumber,
		"departure_date":   f.DepartureDate,
		"passengers_count": f.PassengersCount,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *FlightHandler) GetMeta(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	flightNumber := chi.URLParam(r, "flight_number")
	status := r.URL.Query().Get("status")

	limit := 50
	if ls := r.URL.Query().Get("limit"); ls != "" {
		if v, err := strconv.Atoi(ls); err == nil {
			if v > 0 && v <= 100 {
				limit = v
			}
		}
	}

	meta, total, err := h.metaRepo.GetByFlightNumber(
		ctx,
		flightNumber,
		status,
		limit,
		0,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"flight_number": flightNumber,
		"meta":          meta,
		"pagination": map[string]any{
			"total": total,
			"limit": limit,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}
