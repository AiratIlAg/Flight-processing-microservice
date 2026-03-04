package handlers

import (
	"context"
	"encoding/json"
	"flight_processing/internal/cache"
	"flight_processing/internal/models"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockFlightService struct{ mock.Mock }

func (m *mockFlightService) CreateFlight(req *models.FlightRequest) (int, error) {
	args := m.Called(req)
	return args.Int(0), args.Error(1)
}
func (m *mockFlightService) ProcessFlightMessage(message []byte) error {
	args := m.Called(message)
	return args.Error(0)
}
func (m *mockFlightService) GetFlight(flightNumber string, departureDate time.Time) (*models.FlightData, error) {
	args := m.Called(flightNumber, departureDate)
	fd, _ := args.Get(0).(*models.FlightData)
	return fd, args.Error(1)
}

func (m *mockFlightService) GetFlightMeta(flightNumber string, status string, limit int, offset int) (*models.FlightMetaResponse, error) {
	args := m.Called(flightNumber, status, limit, offset)
	resp, _ := args.Get(0).(*models.FlightMetaResponse)
	return resp, args.Error(1)
}

func TestGetFlight_CacheHit(t *testing.T) {
	r := chi.NewRouter()

	svc := new(mockFlightService)
	c := newMemCache()
	ttl := 5 * time.Minute

	h := NewFlightHandler(svc, c, ttl)
	RegisterFlightRoutes(r, h)

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	// заранее положим в кеш то, что ожидаем получить
	resp := map[string]any{
		"flight_number":    "SU-123",
		"departure_date":   dep.Format(time.RFC3339),
		"passengers_count": 150,
	}
	b, _ := json.Marshal(resp)

	key := cache.FlightDataKey("SU-123", dep)
	_ = c.Set(context.Background(), key, b, ttl)

	req := httptest.NewRequest(http.MethodGet,
		"/api/flights?flight_number=SU-123&departure_date="+dep.Format(time.RFC3339),
		nil,
	)
	rr := httptest.NewRecorder()

	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "HIT", rr.Header().Get("X-Cache"))
	require.JSONEq(t, string(b), rr.Body.String())

	// БД (service) не должен вызываться
	svc.AssertNotCalled(t, "GetFlight", mock.Anything, mock.Anything)
}

func TestGetFlight_CacheMiss_StoresValue(t *testing.T) {
	r := chi.NewRouter()

	svc := new(mockFlightService)
	c := newMemCache()
	ttl := 5 * time.Minute

	h := NewFlightHandler(svc, c, ttl)
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

	// проверим что в кеш положили
	key := cache.FlightDataKey("SU-123", dep)
	_, ok, _ := c.Get(context.Background(), key)
	require.True(t, ok)

	svc.AssertExpectations(t)
}

func TestGetFlightMeta_CacheMiss_ThenHit(t *testing.T) {
	r := chi.NewRouter()

	svc := new(mockFlightService)
	c := newMemCache()
	ttl := 5 * time.Minute

	h := NewFlightHandler(svc, c, ttl)
	RegisterFlightRoutes(r, h)

	resp := &models.FlightMetaResponse{
		FlightNumber: "SU-123",
		Meta:         []models.FlightMetaItemResponse{},
		Pagination:   models.Pagination{Total: 0, Limit: 50},
	}

	flightNumber := "SU-123"
	status := "processed"
	limit := 50
	offset := 0

	// 1) первая загрузка — MISS -> вызов сервиса
	svc.On("GetFlightMeta", flightNumber, status, limit, offset).
		Return(resp, nil).Once()

	req1 := httptest.NewRequest(http.MethodGet,
		"/api/flights/SU-123/meta?status=processed&limit=50&offset=0",
		nil,
	)
	rr1 := httptest.NewRecorder()
	r.ServeHTTP(rr1, req1)

	require.Equal(t, http.StatusOK, rr1.Code)
	require.Equal(t, "MISS", rr1.Header().Get("X-Cache"))

	// Проверим, что ответ действительно валидный JSON
	var got models.FlightMetaResponse
	require.NoError(t, json.Unmarshal(rr1.Body.Bytes(), &got))
	require.Equal(t, "SU-123", got.FlightNumber)

	// 2) проверим, что кеш реально записался по правильному ключу
	cacheKey := cache.FlightMetaKey(flightNumber, status, limit, offset)
	_, ok, _ := c.Get(context.Background(), cacheKey)
	require.True(t, ok, "expected meta response cached by FlightMetaKey")

	// 3) (опционально) проверим, что handler добавил ключ в set для инвалидации
	setKey := cache.FlightMetaKeysSetKey(flightNumber)
	keys, err := c.SMembers(context.Background(), setKey)
	require.NoError(t, err)
	require.Contains(t, keys, cacheKey, "expected meta cache key stored in set for invalidation")

	// 4) вторая загрузка — HIT -> сервис не вызывается
	req2 := httptest.NewRequest(http.MethodGet,
		"/api/flights/SU-123/meta?status=processed&limit=50&offset=0",
		nil,
	)
	rr2 := httptest.NewRecorder()
	r.ServeHTTP(rr2, req2)

	require.Equal(t, http.StatusOK, rr2.Code)
	require.Equal(t, "HIT", rr2.Header().Get("X-Cache"))

	// Сервис должен быть вызван ровно 1 раз (только на MISS)
	svc.AssertExpectations(t)
	svc.AssertNumberOfCalls(t, "GetFlightMeta", 1)

	// На HIT не должно быть новых вызовов
	//svc.AssertNotCalled(t, "GetFlightMeta", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
