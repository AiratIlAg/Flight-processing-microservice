package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
		},
		[]string{"method", "route", "code"},
	)
	httpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route", "code"},
	)

	// Kafka
	kafkaMessagesSent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_messages_sent_total",
			Help: "Total number of Kafka messages successfully sent.",
		},
	)
	kafkaMessagesProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_messages_processed_total",
			Help: "Total number of Kafka messages successfully processed.",
		},
	)
	kafkaErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_errors_total",
			Help: "Total number of Kafka-related errors.",
		},
		[]string{"component", "operation"},
	)
	kafkaConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Kafka consumer lag (high watermark - current offset - 1).",
		},
		[]string{"topic", "partition"},
	)

	// Business
	flightsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "flights_processed_total",
			Help: "Total number of flights processed (business metric).",
		},
	)
	passengers = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "passengers_count",
			Help:    "Distribution of passengers_count for processed flights.",
			Buckets: []float64{0, 10, 20, 50, 100, 150, 200, 300, 400, 500, 800, 1000},
		},
	)
	aircraftTypeCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aircraft_type_processed_total",
			Help: "Number of processed flights by aircraft type.",
		},
		[]string{"aircraft_type"},
	)
	flightMetaStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flight_meta_status_count",
			Help: "Current count of flight_meta rows by status.",
		},
		[]string{"status"},
	)

	// Outbox
	outboxMessagesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_messages_count",
			Help: "Current count of outbox messages by status.",
		},
		[]string{"status"},
	)
	outboxMessagesSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_messages_sent_total",
			Help: "Total number of outbox messages marked as sent.",
		},
	)
	outboxMessagesFailedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_messages_failed_total",
			Help: "Total number of outbox messages marked as failed.",
		},
	)
	outboxProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "outbox_processing_duration_seconds",
			Help:    "Time spent sending a single outbox message (seconds).",
			Buckets: prometheus.DefBuckets,
		},
	)
	outboxRetryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_retries_total",
			Help: "Total number of outbox send retries (failed attempts).",
		},
	)
	outboxLagSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "outbox_lag_seconds",
			Help:    "Lag between outbox message creation and send attempt (seconds).",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
		},
	)
	outboxPendingCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending_count",
			Help: "Current number of pending outbox messages.",
		},
	)
)

var registerOnce sync.Once

func Register() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			httpRequests,
			httpDuration,

			kafkaMessagesSent,
			kafkaMessagesProcessed,
			kafkaErrors,
			kafkaConsumerLag,

			flightsProcessed,
			passengers,
			aircraftTypeCount,
			flightMetaStatus,

			outboxMessagesTotal,
			outboxMessagesSentTotal,
			outboxMessagesFailedTotal,
			outboxProcessingDuration,
			outboxRetryCount,
			outboxLagSeconds,
			outboxPendingCount,
		)
	})
}

func Handler() http.Handler {
	return promhttp.Handler()
}

// --- HTTP ---
func ObserveHTTPRequest(method, route, code string, d time.Duration) {
	httpRequests.WithLabelValues(method, route, code).Inc()
	httpDuration.WithLabelValues(method, route, code).Observe(d.Seconds())
}

// --- Kafka ---
func IncKafkaSent()      { kafkaMessagesSent.Inc() }
func IncKafkaProcessed() { kafkaMessagesProcessed.Inc() }
func IncKafkaError(component, operation string) {
	kafkaErrors.WithLabelValues(component, operation).Inc()
}
func SetKafkaConsumerLag(topic string, partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	kafkaConsumerLag.WithLabelValues(topic, itoa32(partition)).Set(float64(lag))
}

// --- Business ---
func IncFlightsProcessed()         { flightsProcessed.Inc() }
func ObservePassengersCount(n int) { passengers.Observe(float64(max0(n))) }
func IncAircraftType(t string)     { aircraftTypeCount.WithLabelValues(t).Inc() }

// --- Outbox ---
func IncOutboxSent()                          { outboxMessagesSentTotal.Inc() }
func IncOutboxFailed()                        { outboxMessagesFailedTotal.Inc() }
func ObserveOutboxProcessing(d time.Duration) { outboxProcessingDuration.Observe(d.Seconds()) }
func IncOutboxRetry()                         { outboxRetryCount.Inc() }
func ObserveOutboxLagSeconds(sec float64) {
	if sec < 0 {
		sec = 0
	}
	outboxLagSeconds.Observe(sec)
}

// --- Gauges (DB collectors) ---
func SetFlightMetaStatusCount(status string, count int64) {
	if count < 0 {
		count = 0
	}
	flightMetaStatus.WithLabelValues(status).Set(float64(count))
}
func SetOutboxStatusCount(status string, count int64) {
	if count < 0 {
		count = 0
	}
	outboxMessagesTotal.WithLabelValues(status).Set(float64(count))
}
func SetOutboxPendingCount(count int64) {
	if count < 0 {
		count = 0
	}
	outboxPendingCount.Set(float64(count))
}

// helpers
func max0(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

func itoa32(v int32) string { return fmtInt(int64(v)) }

func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [32]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
