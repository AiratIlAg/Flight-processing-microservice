package service

import (
	"context"
	"encoding/json"
	"errors"
	"flight_processing/internal/kafka"
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
	ErrQueueFull    = errors.New("dispatch queue is full")
)

type dispatchTask struct {
	metaID int
	req    models.FlightRequest
}

type FlightService struct {
	db         *pgxpool.Pool
	metaRepo   *repository.MetaRepository
	flightRepo *repository.FlightRepository
	producer   *kafka.Producer
	logger     *log.Logger

	dispatchCh chan dispatchTask
}

func NewFlightService(
	db *pgxpool.Pool,
	metaRepo *repository.MetaRepository,
	flightRepo *repository.FlightRepository,
	producer *kafka.Producer,
	queueSize int,
	logger *log.Logger,
) *FlightService {
	if queueSize <= 0 {
		queueSize = 1000
	}
	if logger == nil {
		logger = log.Default()
	}

	return &FlightService{
		db:         db,
		metaRepo:   metaRepo,
		flightRepo: flightRepo,
		producer:   producer,
		logger:     logger,
		dispatchCh: make(chan dispatchTask, queueSize),
	}
}

// Этап 6.2: отдельная goroutine, читающая из канала и отправляющая в Kafka
func (s *FlightService) StartDispatchWorker(ctx context.Context) {
	go func() {
		s.logger.Println("kafka dispatch worker started")
		defer s.logger.Println("kafka dispatch worker stopped")

		for {
			select {
			case <-ctx.Done():
				return
			case task := <-s.dispatchCh:
				if err := s.producer.SendFlightMessage(task.metaID, &task.req); err != nil {
					s.logger.Printf("send to kafka failed meta_id=%d: %v", task.metaID, err)
					// Если продьюсинг не удался — помечаем meta как error
					_ = s.metaRepo.UpdateStatus(task.metaID, repository.StatusError)
				}
			}
		}
	}()
}

// Этап 6.1: CreateFlight(request *FlightRequest) (int, error)
func (s *FlightService) CreateFlight(request *models.FlightRequest) (int, error) {
	if err := validateCreateRequest(request); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	meta := &models.FlightMeta{
		FlightNumber:  request.FlightNumber,
		DepartureDate: request.DepartureDate,
	}

	// всегда создаётся pending
	if err := s.metaRepo.Create(meta); err != nil {
		return 0, fmt.Errorf("create meta: %w", err)
	}

	// асинхронно: только кладём в буферизированный канал
	task := dispatchTask{
		metaID: meta.ID,
		req:    *request,
	}

	select {
	case s.dispatchCh <- task:
		return meta.ID, nil
	default:
		// буфер переполнен — фиксируем ошибочный статус
		_ = s.metaRepo.UpdateStatus(meta.ID, repository.StatusError)
		return 0, ErrQueueFull
	}
}

// Этап 6.1: ProcessFlightMessage(message []byte) error
// Транзакционно: flights upsert + meta status update(processed)
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

	return nil
}

// Этап 6.1: GetFlight(...)
func (s *FlightService) GetFlight(flightNumber string, departureDate time.Time) (*models.FlightData, error) {
	if strings.TrimSpace(flightNumber) == "" {
		return nil, fmt.Errorf("%w: flight_number is required", ErrInvalidInput)
	}
	if departureDate.IsZero() {
		return nil, fmt.Errorf("%w: departure_date is required", ErrInvalidInput)
	}

	flight, err := s.flightRepo.Get(flightNumber, departureDate)
	if err != nil {
		return nil, err
	}
	return flight, nil
}

// Этап 6.1: GetFlightMeta(...)
func (s *FlightService) GetFlightMeta(
	flightNumber string,
	status string,
	limit int,
) (*models.FlightMetaResponse, error) {
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

	metaRows, total, err := s.metaRepo.GetByFlightNumber(flightNumber, status, limit, 0)
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
