package main

import (
	"context"
	"flight_processing/internal/config"
	"flight_processing/internal/handlers"
	"flight_processing/internal/kafka"
	"flight_processing/internal/repository"
	"flight_processing/internal/service"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {
	ctx := context.Background()

	// ---------- config ----------
	cfg := config.Load()

	// ---------- db ----------
	pool, err := repository.NewPool(ctx, cfg.DBDSN)
	if err != nil {
		log.Fatal("db:", err)
	}

	// ---------- repositories ----------
	metaRepo := repository.NewMetaRepository(pool)
	flightRepo := repository.NewFlightRepository(pool)

	// ---------- kafka producer ----------
	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatal("kafka producer:", err)
	}

	// ---------- kafka consumer ----------
	consumer, err := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		pool,
		metaRepo,
		flightRepo,
	)
	if err != nil {
		log.Fatal("kafka consumer:", err)
	}

	go consumer.Start(ctx, []string{cfg.KafkaTopic})

	// ---------- async kafka channel ----------
	kafkaChan := make(chan service.KafkaJob, 100)
	service.StartKafkaWorker(kafkaChan, producer)

	// ---------- handlers ----------
	h := handlers.NewFlightHandler(metaRepo, flightRepo, kafkaChan)

	// ---------- router ----------
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	r.Post("/api/flights", h.CreateFlight)
	r.Get("/api/flights", h.GetFlight)
	r.Get("/api/flights/{flight_number}/meta", h.GetMeta)

	// ---------- start server ----------
	addr := ":" + cfg.HTTPPort
	log.Println("server starting on", addr)

	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}
