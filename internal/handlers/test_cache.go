package handlers

import (
	"context"
	"sync"
	"time"
)

type memCache struct {
	mu   sync.Mutex
	kv   map[string][]byte
	sets map[string]map[string]struct{}
}

func newMemCache() *memCache {
	return &memCache{
		kv:   make(map[string][]byte),
		sets: make(map[string]map[string]struct{}),
	}
}

func (m *memCache) Close() error { return nil }

func (m *memCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.kv[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (m *memCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = append([]byte(nil), value...)
	return nil
}

func (m *memCache) Del(ctx context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.kv, k)
		delete(m.sets, k)
	}
	return nil
}

func (m *memCache) SAdd(ctx context.Context, key string, members ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sets[key]
	if !ok {
		s = make(map[string]struct{})
		m.sets[key] = s
	}
	for _, mem := range members {
		s[mem] = struct{}{}
	}
	return nil
}

func (m *memCache) SMembers(ctx context.Context, key string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	return out, nil
}

func (m *memCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	// TTL не моделируем в unit-тестах
	return nil
}
