package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"flight_processing/internal/cache"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordCache struct {
	mu sync.Mutex

	kv   map[string][]byte
	sets map[string]map[string]struct{}

	// simulate failures
	failDel      bool
	failSMembers bool
}

func newRecordCache() *recordCache {
	return &recordCache{
		kv:   make(map[string][]byte),
		sets: make(map[string]map[string]struct{}),
	}
}

func (c *recordCache) Close() error { return nil }

func (c *recordCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.kv[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (c *recordCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kv[key] = append([]byte(nil), value...)
	return nil
}

func (c *recordCache) Del(ctx context.Context, keys ...string) error {
	if c.failDel {
		return errors.New("redis del error")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.kv, k)
		delete(c.sets, k)
	}
	return nil
}

func (c *recordCache) SAdd(ctx context.Context, key string, members ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sets[key]
	if !ok {
		s = make(map[string]struct{})
		c.sets[key] = s
	}
	for _, m := range members {
		s[m] = struct{}{}
	}
	return nil
}

func (c *recordCache) SMembers(ctx context.Context, key string) ([]string, error) {
	if c.failSMembers {
		return nil, errors.New("redis smembers error")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sets[key]
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	return out, nil
}

func (c *recordCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return nil
}

// helpers for assertions
func (c *recordCache) existsKey(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.kv[key]
	return ok
}
func (c *recordCache) setSize(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sets[key])
}

// 17) successful invalidation removes flight:data + all meta page keys + set
// показывает, что инвалидация корректно чистит flight:data и все страницы /meta, которые записаны в set — то есть
// кеш реально “сбрасывается” после обновления БД.
func TestInvalidateCacheFromKafkaMessage_RemovesFlightAndMetaKeys(t *testing.T) {
	ctx := context.Background()
	rc := newRecordCache()

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	flightKey := cache.FlightDataKey("SU-123", dep)
	metaKey1 := cache.FlightMetaKey("SU-123", "processed", 50, 0)
	metaKey2 := cache.FlightMetaKey("SU-123", "processed", 50, 50)
	setKey := cache.FlightMetaKeysSetKey("SU-123")

	// place keys in cache
	require.NoError(t, rc.Set(ctx, flightKey, []byte(`{"ok":1}`), time.Minute))
	require.NoError(t, rc.Set(ctx, metaKey1, []byte(`{"page":1}`), time.Minute))
	require.NoError(t, rc.Set(ctx, metaKey2, []byte(`{"page":2}`), time.Minute))
	require.NoError(t, rc.SAdd(ctx, setKey, metaKey1, metaKey2))

	require.True(t, rc.existsKey(flightKey))
	require.True(t, rc.existsKey(metaKey1))
	require.True(t, rc.existsKey(metaKey2))
	require.Equal(t, 2, rc.setSize(setKey))

	// build kafka message bytes
	msg := map[string]any{
		"flight_number":  "SU-123",
		"departure_date": dep.Format(time.RFC3339),
	}
	b, _ := json.Marshal(msg)

	InvalidateCacheFromKafkaMessage(ctx, rc, b)

	// all should be deleted
	require.False(t, rc.existsKey(flightKey))
	require.False(t, rc.existsKey(metaKey1))
	require.False(t, rc.existsKey(metaKey2))
	require.Equal(t, 0, rc.setSize(setKey))
}

// ошибки redis во время инвалидации не должны вызывать панику и не должны блокировать обработку (best-effort)
// показывает, что падение Redis не ломает обработку Kafka (best-effort), то есть сервис не зациклится на одном сообщении из-за Redis.
func TestInvalidateCacheFromKafkaMessage_RedisErrors_DoNotPanic(t *testing.T) {
	ctx := context.Background()
	rc := newRecordCache()
	rc.failDel = true
	rc.failSMembers = true

	dep := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)
	msg := map[string]any{
		"flight_number":  "SU-123",
		"departure_date": dep.Format(time.RFC3339),
	}
	b, _ := json.Marshal(msg)

	require.NotPanics(t, func() {
		InvalidateCacheFromKafkaMessage(ctx, rc, b)
	})
}
