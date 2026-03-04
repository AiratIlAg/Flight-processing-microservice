package handlers

import (
	"bytes"
	"context"
	"errors"
	"flight_processing/internal/models"
	"flight_processing/internal/repository"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Fake кэши для тестов ---

// кэш, который возвращает ошибку при Get (имитирует сбой Redis)
type errGetCache struct{ *memCache }

func (e *errGetCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return nil, false, errors.New("redis get error")
}

// кэш, возвращающий ошибку при Set (имитация сбоя записи Redis)
type errSetCache struct{ *memCache }

func (e *errSetCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return errors.New("redis set error")
}

// 1) GET /api/flights -> 400 если нет параметров
func TestGetFlight_MissingParams_400(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	req := httptest.NewRequest(http.MethodGet, "/api/flights", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	svc.AssertNotCalled(t, "GetFlight", mock.Anything, mock.Anything)
}

// 2) GET /api/flights -> 400 если departure_date не RFC3339
func TestGetFlight_BadDate_400(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date=NOT_A_DATE",
		nil,
	)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	svc.AssertNotCalled(t, "GetFlight", mock.Anything, mock.Anything)
}

// 3) GET /api/flights -> 404 если рейс не найден
func TestGetFlight_NotFound_404(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	svc.On("GetFlight", "SU-123", dep).
		Return((*models.FlightData)(nil), repository.ErrNotFound).
		Once()

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date="+dep.Format(time.RFC3339),
		nil,
	)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
	svc.AssertExpectations(t)
}

// 4) GET /api/flights -> 500 при внутренней ошибке
func TestGetFlight_InternalError_500(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	svc.On("GetFlight", "SU-123", dep).
		Return((*models.FlightData)(nil), errors.New("db down")).
		Once()

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date="+dep.Format(time.RFC3339),
		nil,
	)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusInternalServerError, rr.Code)
	svc.AssertExpectations(t)
}

// 5) GET /api/flights -> если cache.Get вернул ошибку, всё равно идём в сервис и отвечаем 200
func TestGetFlight_CacheGetError_DegradesToService(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)

	base := newMemCache()
	c := &errGetCache{memCache: base}

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	svc.On("GetFlight", "SU-123", dep).Return(&models.FlightData{
		FlightNumber:    "SU-123",
		DepartureDate:   dep,
		PassengersCount: 150,
	}, nil).Once()

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date="+dep.Format(time.RFC3339),
		nil,
	)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "MISS", rr.Header().Get("X-Cache"))
	svc.AssertExpectations(t)
}

// 6) GET /api/flights -> если cache.Set вернул ошибку, ответ всё равно 200
func TestGetFlight_CacheSetError_StillReturns200(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	base := newMemCache()
	c := &errSetCache{memCache: base}

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	svc.On("GetFlight", "SU-123", dep).Return(&models.FlightData{
		FlightNumber:    "SU-123",
		DepartureDate:   dep,
		PassengersCount: 150,
	}, nil).Once()

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date="+dep.Format(time.RFC3339),
		nil,
	)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "MISS", rr.Header().Get("X-Cache"))
	svc.AssertExpectations(t)
}

// 7) GET /api/flights/{flight_number}/meta -> offset < 0 => 400
func TestGetFlightMeta_NegativeOffset_400(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	req := httptest.NewRequest(http.MethodGet, "/api/flights/SU-123/meta?offset=-1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	svc.AssertNotCalled(t, "GetFlightMeta", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// 8) GET /meta -> limit>100 должен clamp до 100 (и сервис должен получить 100)
func TestGetFlightMeta_LimitClampedTo100(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	resp := &models.FlightMetaResponse{
		FlightNumber: "SU-123",
		Meta:         []models.FlightMetaItemResponse{},
		Pagination:   models.Pagination{Total: 0, Limit: 100},
	}

	// status пустой, offset=0, limit должен стать 100
	svc.On("GetFlightMeta", "SU-123", "", 100, 0).Return(resp, nil).Once()

	req := httptest.NewRequest(http.MethodGet, "/api/flights/SU-123/meta?limit=999&offset=0", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "MISS", rr.Header().Get("X-Cache"))
	svc.AssertExpectations(t)
}

// 9) POST /api/flights -> 201 {id,status}
func TestCreateFlight_Success_201(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	body := `{
    "aircraft_type":"Boeing 737",
    "flight_number":"SU-123",
    "departure_date":"2023-10-01T12:00:00Z",
    "arrival_date":"2023-10-01T15:00:00Z",
    "passengers_count":150
  }`

	svc.On("CreateFlight", mock.AnythingOfType("*models.FlightRequest")).Return(123, nil).Once()

	req := httptest.NewRequest(http.MethodPost, "/api/flights", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	require.JSONEq(t, `{"id":123,"status":"`+repository.StatusPending+`"}`, rr.Body.String())
	svc.AssertExpectations(t)
}

// 10) POST /api/flights -> 400 на невалидный JSON и на неизвестные поля (DisallowUnknownFields)
func TestCreateFlight_InvalidJSON_400(t *testing.T) {
	r := chi.NewRouter()
	svc := new(mockFlightService)
	c := newMemCache()

	h := NewFlightHandler(svc, c, 5*time.Minute)
	RegisterFlightRoutes(r, h)

	t.Run("bad json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/flights", bytes.NewBufferString(`{bad`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		require.Equal(t, http.StatusBadRequest, rr.Code)
		svc.AssertNotCalled(t, "CreateFlight", mock.Anything)
	})
	t.Run("unknown field", func(t *testing.T) {
		body := `{
      "aircraft_type":"Boeing 737",
      "flight_number":"SU-123",
      "departure_date":"2023-10-01T12:00:00Z",
      "arrival_date":"2023-10-01T15:00:00Z",
      "passengers_count":150,
      "unknown_field":123
    }`
		req := httptest.NewRequest(http.MethodPost, "/api/flights", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		require.Equal(t, http.StatusBadRequest, rr.Code)
		svc.AssertNotCalled(t, "CreateFlight", mock.Anything)
	})
}
