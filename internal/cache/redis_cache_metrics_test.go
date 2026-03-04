package cache

import (
	"context"
	"testing"
	"time"

	"flight_processing/internal/metrics"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Доказывает, что обёртка RedisCache реально:
// считает количество операций get/set/delete
// считает ошибки Redis
// то есть панели “Redis / Cache” будут отражать реальную нагрузку/проблемы.
func TestRedisCache_MetricsRequestsAndErrors(t *testing.T) {
	metrics.Register()

	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	rc := NewRedisCache(s.Addr(), "", 0)
	defer rc.Close()

	ctx := context.Background()

	getBefore := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "get"})
	setBefore := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "set"})
	delBefore := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "delete"})
	errGetBefore := getCounterVecValue(t, "redis_errors_total", map[string]string{"operation": "get"})

	// 1) GET missing -> request(get)++
	_, ok, err := rc.Get(ctx, "missing")
	require.NoError(t, err)
	require.False(t, ok)

	// 2) SET -> request(set)++
	require.NoError(t, rc.Set(ctx, "k", []byte(`v`), time.Minute))

	// 3) DEL -> request(delete)++
	require.NoError(t, rc.Del(ctx, "k"))

	getAfter := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "get"})
	setAfter := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "set"})
	delAfter := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "delete"})

	require.Equal(t, getBefore+1, getAfter)
	require.Equal(t, setBefore+1, setAfter)
	require.Equal(t, delBefore+1, delAfter)

	// 4) Ошибка Redis: остановим сервер и сделаем GET -> request(get)++ и errors(get)++
	s.Close()
	_, _, err = rc.Get(ctx, "any")
	require.Error(t, err)

	getAfter2 := getCounterVecValue(t, "redis_requests_total", map[string]string{"operation": "get"})
	errGetAfter := getCounterVecValue(t, "redis_errors_total", map[string]string{"operation": "get"})

	require.Equal(t, getAfter+1, getAfter2)
	require.Equal(t, errGetBefore+1, errGetAfter)
}

// достаём значение counter с лейблами из DefaultGatherer
func getCounterVecValue(t *testing.T, metricName string, labels map[string]string) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatchMetric(m, labels) {
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	// если метрика ещё не “создана” по этим label-ам, считаем 0
	return 0
}

func labelsMatchMetric(m *dto.Metric, want map[string]string) bool {
	got := make(map[string]string, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
