package cache

import (
	"context"
	"flight_processing/internal/metrics"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartRedisSizeCollector(ctx context.Context, client *redis.Client, interval time.Duration, logger *log.Logger) {
	if client == nil {
		return
	}
	if logger == nil {
		logger = log.Default()
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		update := func() {
			info, err := client.Info(ctx, "memory").Result()
			if err != nil {
				metrics.IncRedisError("get")
				return
			}
			// ищем строку вида: used_memory:123456
			for _, line := range strings.Split(info, "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "used_memory:") {
					v := strings.TrimPrefix(line, "used_memory:")
					v = strings.TrimSpace(v)
					n, err := strconv.ParseInt(v, 10, 64)
					if err == nil {
						metrics.SetRedisCacheSizeBytes(n)
					}
					return
				}
			}
		}

		update()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				update()
			}
		}
	}()
}
