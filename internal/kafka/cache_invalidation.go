package kafka

import (
	"context"
	"encoding/json"
	"flight_processing/internal/cache"
	"time"
)

// выносим инвалидацию кеша в отдельную функцию и покроем её юнит-тестами. Тогда: consumer просто вызывает эту функцию
// после успешного commit БД и перед commit offset; тесты не требуют Sarama и не становятся интеграционными
// best-effort инвалидация. Ошибки Redis не должны ломать обработку Kafka.
func InvalidateCacheFromKafkaMessage(ctx context.Context, c cache.Cache, msgBytes []byte) {
	if c == nil || len(msgBytes) == 0 {
		return
	}

	var m struct {
		FlightNumber  string    `json:"flight_number"`
		DepartureDate time.Time `json:"departure_date"`
	}

	if err := json.Unmarshal(msgBytes, &m); err != nil {
		return
	}
	if m.FlightNumber == "" || m.DepartureDate.IsZero() {
		return
	}

	// 1) invalidate GET /api/flights cache
	_ = c.Del(ctx, cache.FlightDataKey(m.FlightNumber, m.DepartureDate))

	// 2) invalidate ALL cached pages for /meta via set of keys
	setKey := cache.FlightMetaKeysSetKey(m.FlightNumber)
	keys, err := c.SMembers(ctx, setKey)
	if err == nil && len(keys) > 0 {
		_ = c.Del(ctx, keys...)
	}
	_ = c.Del(ctx, setKey)
}
