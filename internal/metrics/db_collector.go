package metrics

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartDBCollectors(ctx context.Context, db *pgxpool.Pool, interval time.Duration, logger *log.Logger) {
	if db == nil {
		return
	}
	if logger == nil {
		logger = log.Default()
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		updateDBGauges(ctx, db, logger)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				updateDBGauges(ctx, db, logger)
			}
		}
	}()
}

func updateDBGauges(ctx context.Context, db *pgxpool.Pool, logger *log.Logger) {
	// flight_meta counts by status
	{
		rows, err := db.Query(ctx, `SELECT status, COUNT(*) FROM flight_meta GROUP BY status`)
		if err != nil {
			logger.Printf("metrics db query flight_meta: %v", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var status string
				var cnt int64
				if err := rows.Scan(&status, &cnt); err != nil {
					logger.Printf("metrics db scan flight_meta: %v", err)
					continue
				}
				SetFlightMetaStatusCount(status, cnt)
			}
		}
	}

	// outbox counts by status (+ pending)
	{
		rows, err := db.Query(ctx, `SELECT status, COUNT(*) FROM outbox_messages GROUP BY status`)
		if err != nil {
			// если таблица ещё не создана — просто пропускаем
			return
		}
		defer rows.Close()

		var pending int64
		for rows.Next() {
			var status string
			var cnt int64
			if err := rows.Scan(&status, &cnt); err != nil {
				logger.Printf("metrics db scan outbox: %v", err)
				continue
			}
			SetOutboxStatusCount(status, cnt)
			if status == "pending" {
				pending = cnt
			}
		}
		SetOutboxPendingCount(pending)
	}
}
