package main

import (
	"context"
	"errors"
	"flight_processing/internal/config"
	"flight_processing/internal/handlers"
	"flight_processing/internal/kafka"
	"flight_processing/internal/repository"
	"flight_processing/internal/service"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.Load()

	// Контекст завершения (Ctrl+C / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1) PostgreSQL
	dbPool, err := repository.NewPool(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db init error: %v", err)
	}
	defer dbPool.Close()

	// 2) Миграции (без внешних инструментов, простой встроенный runner)
	if err := runMigrations(ctx, dbPool, "./migrations"); err != nil {
		log.Fatalf("migrations error: %v", err)
	}

	// 3) Репозитории
	metaRepo := repository.NewMetaRepository(dbPool)
	flightRepo := repository.NewFlightRepository(dbPool)

	// 4) Kafka Producer
	producer, err := kafka.NewSyncProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("kafka producer init error: %v", err)
	}
	defer producer.Close()

	// 5) Сервисный слой (канал + бизнес-логика)
	flightSvc := service.NewFlightService(
		dbPool,
		metaRepo,
		flightRepo,
		producer,
		1000, // буфер канала отправки в Kafka
		log.Default(),
	)
	flightSvc.StartDispatchWorker(ctx)

	// 6) Kafka Consumer Group
	consumer, err := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		cfg.KafkaTopic,
		flightSvc, // важно: сервис реализует ProcessFlightMessage([]byte) error
		log.Default(),
	)
	if err != nil {
		log.Fatalf("kafka consumer init error: %v", err)
	}
	defer consumer.Close()

	// 7) HTTP router
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	h := handlers.NewFlightHandler(flightSvc)
	handlers.RegisterFlightRoutes(r, h)

	addr := ":" + cfg.HTTPPort
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 8) Метрики — отключены по твоему требованию
	log.Println("metrics: disabled (Prometheus/Grafana are not used)")

	// 9) Запуск горутин
	errCh := make(chan error, 2)

	go func() {
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("consumer failed: %w", err)
		}
	}()

	go func() {
		log.Println("http server starting on", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server failed: %w", err)
		}
	}()

	// 10) Ожидание сигнала остановки или фатальной ошибки
	select {
	case err := <-errCh:
		log.Printf("fatal runtime error: %v", err)
		stop() // инициируем graceful shutdown
	case <-ctx.Done():
		log.Println("shutdown signal received")
	}

	// 11) Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	} else {
		log.Println("http server stopped gracefully")
	}

	log.Println("application stopped")
}

// runMigrations применяет все *.up.sql из папки migrations один раз.
// Простой встроенный механизм без Prometheus/Grafana и без внешнего migrate CLI.
func runMigrations(ctx context.Context, db *pgxpool.Pool, migrationsDir string) error {
	// Таблица учёта применённых миграций
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".up.sql") {
			files = append(files, name)
		}
	}

	sort.Strings(files)

	for _, file := range files {
		var exists bool
		err := db.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			file,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", file, err)
		}
		if exists {
			continue
		}

		path := filepath.Join(migrationsDir, file)
		sqlBytes, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", file, err)
		}

		tx, err := db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration tx %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("execute migration %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations(version) VALUES($1)`, file,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("save migration record %s: %w", file, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", file, err)
		}

		log.Printf("migration applied: %s", file)
	}

	return nil
}
