package config

import (
	"log"
	"os"
	"strings"
)

type Config struct {
	DBDSN        string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	HTTPPort     string
}

func Load() *Config {
	cfg := &Config{
		DBDSN:        getEnv("DB_DSN", "postgres://flight:flight@localhost:5432/flights?sslmode=disable"),
		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "flight_requests"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "flight-service-group"),
		HTTPPort:     getEnv("HTTP_PORT", "8080"),
	}

	log.Println("config loaded")
	return cfg
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
