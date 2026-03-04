package handlers

import (
	"context"
	"flight_processing/internal/cache"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Инвалидация meta через set ключей: удаляем все page-keys и сам set
// гарантирует, что механизм “set ключей на рейс” реально позволяет инвалидировать все страницы без SCAN.
// helper: логика инвалидации meta keys (как ты делаешь в consumer)
func invalidateMetaCache(ctx context.Context, c *memCache, flightNumber string) error {
	setKey := cache.FlightMetaKeysSetKey(flightNumber)

	keys, err := c.SMembers(ctx, setKey)
	if err != nil {
		return err
	}

	// удалить все page keys
	if len(keys) > 0 {
		_ = c.Del(ctx, keys...)
	}
	// удалить сам set
	_ = c.Del(ctx, setKey)

	return nil
}

func TestInvalidateMetaCache_DeletesAllPageKeysAndSet(t *testing.T) {
	ctx := context.Background()
	c := newMemCache()

	flightNumber := "SU-123"
	setKey := cache.FlightMetaKeysSetKey(flightNumber)

	// смоделируем 3 страницы меты
	k1 := cache.FlightMetaKey(flightNumber, "processed", 50, 0)
	k2 := cache.FlightMetaKey(flightNumber, "processed", 50, 50)
	k3 := cache.FlightMetaKey(flightNumber, "pending", 50, 0)

	// кладём "страницы" в кеш
	require.NoError(t, c.Set(ctx, k1, []byte(`{"page":1}`), 5*time.Minute))
	require.NoError(t, c.Set(ctx, k2, []byte(`{"page":2}`), 5*time.Minute))
	require.NoError(t, c.Set(ctx, k3, []byte(`{"page":3}`), 5*time.Minute))

	// и регистрируем их в set для инвалидации
	require.NoError(t, c.SAdd(ctx, setKey, k1, k2, k3))

	// sanity: всё существует
	_, ok1, _ := c.Get(ctx, k1)
	_, ok2, _ := c.Get(ctx, k2)
	_, ok3, _ := c.Get(ctx, k3)
	require.True(t, ok1 && ok2 && ok3)

	members, err := c.SMembers(ctx, setKey)
	require.NoError(t, err)
	require.Len(t, members, 3)

	// действие: инвалидируем
	require.NoError(t, invalidateMetaCache(ctx, c, flightNumber))

	// ожидание: страницы удалены
	_, ok1, _ = c.Get(ctx, k1)
	_, ok2, _ = c.Get(ctx, k2)
	_, ok3, _ = c.Get(ctx, k3)
	require.False(t, ok1)
	require.False(t, ok2)
	require.False(t, ok3)

	// ожидание: set тоже удалён
	members, _ = c.SMembers(ctx, setKey)
	require.Len(t, members, 0)
}
