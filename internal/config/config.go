package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DBDSN        string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	HTTPPort     string

	OutboxPollInterval  time.Duration
	OutboxBatchSize     int
	OutboxMaxRetries    int
	OutboxRetentionDays int
}

func Load() *Config {
	// Чтобы "go run" тоже читал .env.
	// В Docker Compose env_file уже подставит переменные — это не мешает.
	_ = godotenv.Load(".env")

	cfg := &Config{
		DBDSN:        mustEnv("DB_DSN"),
		KafkaBrokers: splitCSV(mustEnv("KAFKA_BROKERS")),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "flight_requests"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "flight-service-group"),
		HTTPPort:     getEnv("HTTP_PORT", "8080"),

		OutboxPollInterval:  mustDurationEnv("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:     mustIntEnv("OUTBOX_BATCH_SIZE", 100),
		OutboxMaxRetries:    mustIntEnv("OUTBOX_MAX_RETRIES", 10),
		OutboxRetentionDays: mustIntEnv("OUTBOX_RETENTION_DAYS", 7),
	}

	return cfg
}

func mustEnv(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Fatalf("missing required env: %s", key)
	}
	return v
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func mustIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		log.Fatalf("invalid env %s=%q (expected non-negative int)", key, v)
	}
	return n
}

func mustDurationEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Fatalf("invalid env %s=%q (expected duration like 500ms, 2s)", key, v)
	}
	return d
}
