package redisstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	redisvalue "github.com/codecrafters-io/redis-starter-go/pkg/redis/redis-value"
	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/store/id"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

var ErrNotFound = errors.New("not-found")
var ErrExpired = errors.New("expired")

type StreamEntry struct {
	ID     string
	Values []string
}

type RedisStore struct {
	storeMut *sync.Mutex
	store    map[string]redisvalue.RedisValue

	streamsMut *sync.Mutex
	streams    map[string][]StreamEntry
}

func NewRedisStore(dir string, dbfilename string) *RedisStore {
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

	return &RedisStore{
		storeMut: &sync.Mutex{},
		store:    store,

		streamsMut: &sync.Mutex{},
		streams:    make(map[string][]StreamEntry),
	}
}

func (s *RedisStore) Set(key string, value string) {
	fmt.Printf("SET key: %s, value: %s\n", key, value)
	s.storeMut.Lock()
	defer s.storeMut.Unlock()

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
	s.storeMut.Lock()
	defer s.storeMut.Unlock()

	expiresAt := time.Now().Add(duration)
	s.store[key] = redisvalue.RedisValue{
		Value:     value,
		ExpiresAt: &expiresAt,
	}
}

func (s *RedisStore) Get(key string) (string, bool, error) {
	fmt.Printf("GET key: %s\n", key)
	s.storeMut.Lock()
	defer s.storeMut.Unlock()

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

func (s *RedisStore) Delete(keys []string) int {
	fmt.Printf("DEL keys: %s\n", keys)
	s.storeMut.Lock()
	defer s.storeMut.Unlock()

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

func (s *RedisStore) Keys() []string {
	s.storeMut.Lock()
	defer s.storeMut.Unlock()

	keys := make([]string, 0, len(s.store))

	for key := range s.store {
		keys = append(keys, key)
	}

	return keys
}

func (s *RedisStore) AppendToStream(key string, id string, values []string) (string, error) {
	s.streamsMut.Lock()
	defer s.streamsMut.Unlock()

	stream, ok := s.streams[key]
	if ok && len(stream) > 0 {
		prevEntry := stream[len(stream)-1]

		msTime, _, err := entry_id.ParseID(id)
		if err == entry_id.ErrGenerateSeqNum {
			prevMsTime, prevSeqNum, _ := entry_id.ParseID(prevEntry.ID)
			seqNum := prevSeqNum + 1
			if msTime > prevMsTime {
				seqNum = 0
			}
			id = fmt.Sprintf("%d-%d", msTime, seqNum)
		} else if err == entry_id.ErrGenerateID {
			prevMsTime, prevSeqNum, _ := entry_id.ParseID(prevEntry.ID)
			id = fmt.Sprintf("%d-%d", prevMsTime+1, prevSeqNum)
		} else if err != nil {
			return "", err
		}

		if !entry_id.AreIDsIncreasing(prevEntry.ID, id) {
			return "", entry_id.ErrIDsNotIncreasing
		}

		s.streams[key] = append(stream, StreamEntry{
			ID:     id,
			Values: values,
		})
		return id, nil
	}

	msTime, _, err := entry_id.ParseID(id)
	if err == entry_id.ErrGenerateSeqNum {
		id = fmt.Sprintf("%d-%d", msTime, 1)
	} else if err == entry_id.ErrGenerateID {
		id = "0-1"
	} else if err != nil {
		return "", err
	}

	entry := StreamEntry{
		ID:     id,
		Values: values,
	}
	s.streams[key] = []StreamEntry{entry}

	return id, nil
}

func (s *RedisStore) GetStream(key string) ([]StreamEntry, bool) {
	s.streamsMut.Lock()
	defer s.streamsMut.Unlock()

	values, ok := s.streams[key]
	if !ok {
		return nil, false
	}

	return values, true
}
