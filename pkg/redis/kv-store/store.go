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

type KVStore struct {
	mu    *sync.RWMutex
	store map[string]redisvalue.RedisValue
}

func NewKVStore(dir string, dbfilename string) *KVStore {
	store := make(map[string]redisvalue.RedisValue)
	if dir != "" && dbfilename != "" {
		st, err := persistence.ParseRDBFile(dir, dbfilename)
		if err != nil {
			fmt.Println("Error parsing RDB file: ", err.Error())
		}
		if st != nil {
			store = st
		}
	}

	return &KVStore{
		mu:    &sync.RWMutex{},
		store: store,
	}
}

func (s *KVStore) Set(key string, value string) {
	fmt.Printf("SET key: %s, value: %s\n", key, value)
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[key] = redisvalue.RedisValue{
		Value:     value,
		ExpiresAt: nil,
	}
}

func (s *KVStore) SetWithTTL(
	key string, value string, duration time.Duration,
) {
	fmt.Printf(
		"SET key: %s, value: %s, duration: %s\n",
		key, value, duration.String(),
	)
	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt := time.Now().Add(duration)
	s.store[key] = redisvalue.RedisValue{
		Value:     value,
		ExpiresAt: &expiresAt,
	}
}

func (s *KVStore) Get(key string) (string, bool, error) {
	fmt.Printf("GET key: %s\n", key)
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	return value.Value, true, nil
}

func (s *KVStore) Delete(keys []string) int {
	fmt.Printf("DEL keys: %s\n", keys)
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for _, key := range keys {
		_, ok := s.store[key]
		if ok {
			deleted++
		}
		delete(s.store, key)
	}

	return deleted
}

func (s *KVStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.store))

	for key := range s.store {
		keys = append(keys, key)
	}

	return keys
}
