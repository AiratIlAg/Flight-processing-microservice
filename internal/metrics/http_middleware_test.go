package metrics

//делаем запрос через router и проверяем, что http_requests_total увеличился (через prometheus/testutil).

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Доказывает, что middleware реально считает запросы и latency — то есть Prometheus/Grafana панели по API корректны.
func TestHTTPMiddleware_IncrementsMetrics(t *testing.T) {
	// регистрируем метрики один раз для пакета
	Register()

	r := chi.NewRouter()
	r.Use(HTTPMiddleware)

	// важно: зарегистрировать роут, чтобы routePattern был "/test"
	r.Get("/test", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// baseline: counter должен быть 0
	before := testutil.ToFloat64(httpRequests.WithLabelValues("GET", "/test", "200"))
	require.Equal(t, 0.0, before)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	after := testutil.ToFloat64(httpRequests.WithLabelValues("GET", "/test", "200"))
	require.Equal(t, 1.0, after)

	// для histogram проверяем sample_count у метрики http_request_duration_seconds
	cnt := getHistogramSampleCount(t, "http_request_duration_seconds", map[string]string{
		"method": "GET",
		"route":  "/test",
		"code":   "200",
	})
	require.Equal(t, uint64(1), cnt)
}

// helper: достать counter sample из default registry по имени + лейблам
func getHistogramSampleCount(t *testing.T, metricName string, labels map[string]string) uint64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		// это MetricFamily типа HISTOGRAM
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, labels) {
				h := m.GetHistogram()
				if h == nil {
					t.Fatalf("metric %s matched labels but is not a histogram", metricName)
				}
				return h.GetSampleCount()
			}
		}
	}

	t.Fatalf("histogram %s with labels %v not found", metricName, labels)
	return 0
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
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
