package redisstore

import (
	"errors"
	"sync"
	"time"
)

type redisValue struct {
	Value     string
	ExpiresAt *time.Time
}

type RedisStore struct {
	mutex *sync.Mutex
	store map[string]redisValue
}

func NewRedisStore() *RedisStore {
	return &RedisStore{
		mutex: &sync.Mutex{},
		store: make(map[string]redisValue),
	}
}

func (s *RedisStore) Set(key string, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.store[key] = redisValue{
		Value:     value,
		ExpiresAt: nil,
	}
}

func (s *RedisStore) SetWithTTL(key string, value string, duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	expiresAt := time.Now().Add(duration)
	s.store[key] = redisValue{
		Value:     value,
		ExpiresAt: &expiresAt,
	}
}

func (s *RedisStore) Get(key string) (string, bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, ok := s.store[key]

	if !ok {
		return "", false, errors.New("EXPIRED")
	}

	return value.Value, ok, nil
}
