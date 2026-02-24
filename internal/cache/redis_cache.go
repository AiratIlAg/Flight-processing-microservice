package cache

import (
	"context"
	"errors"
	"flight_processing/internal/metrics"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	c *redis.Client
}

func NewRedisCache(addr, password string, db int) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisCache{c: rdb}
}

func (r *RedisCache) Close() error { return r.c.Close() }

// operation label строго по ТЗ: get/set/delete
const (
	opGet    = "get"
	opSet    = "set"
	opDelete = "delete"
)

func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	start := time.Now()
	metrics.IncRedisRequest(opGet)
	defer metrics.ObserveRedisDuration(opGet, time.Since(start))

	b, err := r.c.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		metrics.IncRedisError(opGet)
		return nil, false, err
	}
	return b, true, nil
}

func (r *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	metrics.IncRedisRequest(opSet)
	defer metrics.ObserveRedisDuration(opSet, time.Since(start))

	if err := r.c.Set(ctx, key, value, ttl).Err(); err != nil {
		metrics.IncRedisError(opSet)
		return err
	}
	return nil
}

func (r *RedisCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	start := time.Now()
	metrics.IncRedisRequest(opDelete)
	defer metrics.ObserveRedisDuration(opDelete, time.Since(start))

	if err := r.c.Del(ctx, keys...).Err(); err != nil {
		metrics.IncRedisError(opDelete)
		return err
	}
	return nil
}

// Эти операции нужны для “set ключей meta” (инвалидация без SCAN).
// По ТЗ метрики только get/set/delete — логично отнести их к "set" и "get".
func (r *RedisCache) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	start := time.Now()
	metrics.IncRedisRequest(opSet)
	defer metrics.ObserveRedisDuration(opSet, time.Since(start))

	if err := r.c.SAdd(ctx, key, members).Err(); err != nil {
		metrics.IncRedisError(opSet)
		return err
	}
	return nil
}

func (r *RedisCache) SMembers(ctx context.Context, key string) ([]string, error) {
	start := time.Now()
	metrics.IncRedisRequest(opGet)
	defer metrics.ObserveRedisDuration(opGet, time.Since(start))

	res, err := r.c.SMembers(ctx, key).Result()
	if err != nil {
		metrics.IncRedisError(opGet)
		return nil, err
	}
	return res, nil
}

func (r *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	metrics.IncRedisRequest(opSet)
	defer metrics.ObserveRedisDuration(opSet, time.Since(start))

	if err := r.c.Expire(ctx, key, ttl).Err(); err != nil {
		metrics.IncRedisError(opSet)
		return err
	}
	return nil
}

func (r *RedisCache) RawClient() *redis.Client { return r.c }
