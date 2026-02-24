package service

import (
	"context"
	"encoding/json"
	"errors"
	"flight_processing/internal/kafka"
	"flight_processing/internal/metrics"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidInput = errors.New("invalid input")
)

type FlightService struct {
	db         *pgxpool.Pool
	metaRepo   *repository.MetaRepository
	flightRepo *repository.FlightRepository
	outboxRepo *repository.OutboxRepository

	kafkaTopic string
	logger     *log.Logger
}

func NewFlightService(
	db *pgxpool.Pool,
	metaRepo *repository.MetaRepository,
	flightRepo *repository.FlightRepository,
	outboxRepo *repository.OutboxRepository,
	kafkaTopic string,
	logger *log.Logger,
) *FlightService {
	if logger == nil {
		logger = log.Default()
	}
	if strings.TrimSpace(kafkaTopic) == "" {
		kafkaTopic = "flight_requests"
	}

	return &FlightService{
		db:         db,
		metaRepo:   metaRepo,
		flightRepo: flightRepo,
		outboxRepo: outboxRepo,
		kafkaTopic: kafkaTopic,
		logger:     logger,
	}
}

// CreateFlight(request) – транзакционно: flight_meta + outbox_messages
func (s *FlightService) CreateFlight(request *models.FlightRequest) (int, error) {
	if err := validateCreateRequest(request); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	ctx := context.Background()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1) flight_meta (pending)
	meta := &models.FlightMeta{
		FlightNumber:  request.FlightNumber,
		DepartureDate: request.DepartureDate,
	}
	if err := s.metaRepo.CreateTx(ctx, tx, meta); err != nil {
		return 0, fmt.Errorf("create meta tx: %w", err)
	}

	// 2) payload для Kafka = FlightRequest + meta_id
	kmsg := kafka.NewFlightMessage(meta.ID, request)
	payload, err := json.Marshal(kmsg)
	if err != nil {
		return 0, fmt.Errorf("marshal kafka payload: %w", err)
	}

	// 3) outbox_messages (pending)
	ob := &models.OutboxMessage{
		Topic:   s.kafkaTopic,
		Payload: payload,
	}
	if err := s.outboxRepo.CreateMessage(ctx, tx, ob); err != nil {
		return 0, fmt.Errorf("create outbox message tx: %w", err)
	}

	// 4) commit
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	return meta.ID, nil
}

// ProcessFlightMessage(message) – как было: транзакционно flights upsert + meta processed
func (s *FlightService) ProcessFlightMessage(message []byte) error {
	var msg kafka.FlightMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return fmt.Errorf("unmarshal kafka message: %w", err)
	}

	if msg.MetaID <= 0 || strings.TrimSpace(msg.FlightNumber) == "" || msg.DepartureDate.IsZero() {
		return fmt.Errorf("%w: invalid kafka message payload", ErrInvalidInput)
	}

	ctx := context.Background()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	flight := msg.ToFlightData()

	if err := s.flightRepo.UpsertTx(ctx, tx, flight); err != nil {
		return fmt.Errorf("upsert flight tx: %w", err)
	}

	if err := s.metaRepo.UpdateStatusTx(ctx, tx, msg.MetaID, repository.StatusProcessed); err != nil {
		return fmt.Errorf("update meta status tx: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	metrics.IncFlightsProcessed()
	metrics.ObservePassengersCount(flight.PassengersCount)
	metrics.IncAircraftType(flight.AircraftType)

	return nil
}

func (s *FlightService) GetFlight(flightNumber string, departureDate time.Time) (*models.FlightData, error) {
	if strings.TrimSpace(flightNumber) == "" {
		return nil, fmt.Errorf("%w: flight_number is required", ErrInvalidInput)
	}
	if departureDate.IsZero() {
		return nil, fmt.Errorf("%w: departure_date is required", ErrInvalidInput)
	}
	return s.flightRepo.Get(flightNumber, departureDate)
}

func (s *FlightService) GetFlightMeta(flightNumber string, status string, limit int, offset int) (*models.FlightMetaResponse, error) {
	if strings.TrimSpace(flightNumber) == "" {
		return nil, fmt.Errorf("%w: flight_number is required", ErrInvalidInput)
	}

	if status != "" && !isValidStatus(status) {
		return nil, fmt.Errorf("%w: status must be pending|processed|error", ErrInvalidInput)
	}

	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	metaRows, total, err := s.metaRepo.GetByFlightNumber(flightNumber, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get meta by flight_number: %w", err)
	}

	items := make([]models.FlightMetaItemResponse, 0, len(metaRows))
	for _, m := range metaRows {
		items = append(items, models.FlightMetaItemResponse{
			ID:            m.ID,
			FlightNumber:  m.FlightNumber,
			DepartureDate: m.DepartureDate,
			Status:        m.Status,
			CreatedAt:     m.CreatedAt,
			ProcessedAt:   m.ProcessedAt,
		})
	}

	return &models.FlightMetaResponse{
		FlightNumber: flightNumber,
		Meta:         items,
		Pagination: models.Pagination{
			Total: total,
			Limit: limit,
		},
	}, nil
}

func validateCreateRequest(req *models.FlightRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if strings.TrimSpace(req.AircraftType) == "" {
		return errors.New("aircraft_type is required")
	}
	if strings.TrimSpace(req.FlightNumber) == "" {
		return errors.New("flight_number is required")
	}
	if req.DepartureDate.IsZero() {
		return errors.New("departure_date is required")
	}
	if req.ArrivalDate.IsZero() {
		return errors.New("arrival_date is required")
	}
	if !req.ArrivalDate.After(req.DepartureDate) {
		return errors.New("arrival_date must be after departure_date")
	}
	if req.PassengersCount < 0 {
		return errors.New("passengers_count must be >= 0")
	}
	return nil
}

func isValidStatus(s string) bool {
	return s == repository.StatusPending ||
		s == repository.StatusProcessed ||
		s == repository.StatusError
}
