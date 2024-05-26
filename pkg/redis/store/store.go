package redisstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	redisvalue "github.com/codecrafters-io/redis-starter-go/pkg/redis/redis-value"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

var ErrNotFound = errors.New("not-found")
var ErrExpired = errors.New("expired")

type RedisStore struct {
	mutex *sync.Mutex
	store map[string]redisvalue.RedisValue
}

func NewRedisStore(dir string, dbfilename string) *RedisStore {
	store := make(map[string]redisvalue.RedisValue)
	if dir != "" && dbfilename != "" {
		st := persistence.ParseRDBFile(dir, dbfilename)
		if st != nil {
			store = st
		}
	}

	return &RedisStore{
		mutex: &sync.Mutex{},
		store: store,
	}
}

func (s *RedisStore) Set(key string, value string) {
	fmt.Printf("SET key: %s, value: %s\n", key, value)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.store[key] = redisvalue.RedisValue{
		Value:     value,
		ExpiresAt: nil,
	}
}

func (s *RedisStore) SetWithTTL(
	key string, value string, duration time.Duration,
) {
	fmt.Printf(
		"SET key: %s, value: %s, duration: %s\n",
		key, value, duration.String(),
	)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	expiresAt := time.Now().Add(duration)
	s.store[key] = redisvalue.RedisValue{
		Value:     value,
		ExpiresAt: &expiresAt,
	}
}

func (s *RedisStore) Get(key string) (string, bool, error) {
	fmt.Printf("GET key: %s\n", key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, ok := s.store[key]

	if !ok {
		fmt.Printf("GET key: %s NOT_FOUND\n", key)
		return "", false, ErrNotFound
	}

	if value.ExpiresAt != nil && time.Now().After(*value.ExpiresAt) {
		fmt.Printf("GET key: %s EXPIRED\n", key)
		return "", false, ErrExpired
	}

	fmt.Printf("GET key: %s, value: %s\n", key, value.Value)
	return value.Value, ok, nil
}

func (s *RedisStore) Keys() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	keys := make([]string, 0, len(s.store))

	for key := range s.store {
		keys = append(keys, key)
	}

	return keys
}
