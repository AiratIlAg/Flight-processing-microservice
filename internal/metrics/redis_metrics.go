package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	redisRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_requests_total",
			Help: "Total number of Redis requests",
		},
		[]string{"operation"}, // get, set, delete
	)

	redisCacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	redisCacheMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	redisErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_errors_total",
			Help: "Total number of Redis errors",
		},
		[]string{"operation"},
	)

	// время ответа Redis (гистограмма)
	redisRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_request_duration_seconds",
			Help:    "Redis request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	redisCacheSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cache_size_bytes",
			Help: "Approximate size of Redis cache (if available)",
		},
	)
)

var redisRegisterOnce sync.Once

// Вызывается из metrics.Register() (см. ниже)
func registerRedisMetrics() {
	redisRegisterOnce.Do(func() {
		prometheus.MustRegister(
			redisRequestsTotal,
			redisCacheHitsTotal,
			redisCacheMissesTotal,
			redisErrorsTotal,
			redisRequestDuration,
			redisCacheSize,
		)
	})
}

// --- Public helpers ---

func IncRedisRequest(op string) {
	redisRequestsTotal.WithLabelValues(op).Inc()
}

func IncRedisError(op string) {
	redisErrorsTotal.WithLabelValues(op).Inc()
}

func ObserveRedisDuration(op string, d time.Duration) {
	redisRequestDuration.WithLabelValues(op).Observe(d.Seconds())
}

func IncRedisHit()  { redisCacheHitsTotal.Inc() }
func IncRedisMiss() { redisCacheMissesTotal.Inc() }

func SetRedisCacheSizeBytes(n int64) {
	if n < 0 {
		n = 0
	}
	redisCacheSize.Set(float64(n))
}
