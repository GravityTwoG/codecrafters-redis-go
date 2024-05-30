package redisstore

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	redisvalue "github.com/codecrafters-io/redis-starter-go/pkg/redis/redis-value"
	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/store/id"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

var ErrNotFound = errors.New("not-found")
var ErrExpired = errors.New("expired")

type StreamEntry struct {
	ID     entry_id.EntryID
	Values []string
}

type RedisStore struct {
	storeMut *sync.RWMutex
	store    map[string]redisvalue.RedisValue

	streamsMut *sync.RWMutex
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
		storeMut: &sync.RWMutex{},
		store:    store,

		streamsMut: &sync.RWMutex{},
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
	s.storeMut.RLock()
	defer s.storeMut.RUnlock()

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
	s.storeMut.RLock()
	defer s.storeMut.RUnlock()

	keys := make([]string, 0, len(s.store))

	for key := range s.store {
		keys = append(keys, key)
	}

	return keys
}

func (s *RedisStore) AppendToStream(key string, id string, values []string) (string, error) {

	if len(values)%2 != 0 {
		return "", fmt.Errorf("invalid number of values")
	}

	s.streamsMut.Lock()
	defer s.streamsMut.Unlock()

	stream, ok := s.streams[key]
	if ok && len(stream) > 0 {
		prevEntry := stream[len(stream)-1]

		parsedID, err := entry_id.ParseID(id)
		if err == entry_id.ErrGenerateID || err == entry_id.ErrGenerateSeqNum {
			parsedID = entry_id.GenerateID(id, &prevEntry.ID)
			if id == "" {
				return "", entry_id.ErrIDLessThanMinimum
			}
		} else if err != nil {
			return "", err
		}

		if !parsedID.Greater(&prevEntry.ID) {
			return "", entry_id.ErrIDsNotIncreasing
		}

		s.streams[key] = append(stream, StreamEntry{
			ID:     *parsedID,
			Values: values,
		})
		return parsedID.String(), nil
	}

	parsedID, err := entry_id.ParseID(id)
	if err == entry_id.ErrGenerateID || err == entry_id.ErrGenerateSeqNum {
		parsedID = entry_id.GenerateID(id, nil)
		if parsedID == nil {
			return "", entry_id.ErrIDLessThanMinimum
		}
	} else if err != nil {
		return "", err
	}

	entry := StreamEntry{
		ID:     *parsedID,
		Values: values,
	}
	s.streams[key] = []StreamEntry{entry}

	return parsedID.String(), nil
}

func (s *RedisStore) GetStream(key string) ([]StreamEntry, bool) {
	s.streamsMut.RLock()
	defer s.streamsMut.RUnlock()

	values, ok := s.streams[key]
	if !ok {
		return nil, false
	}

	return values, true
}

func (s *RedisStore) Range(key string, start string, end string) ([]StreamEntry, error) {
	s.streamsMut.RLock()
	defer s.streamsMut.RUnlock()

	stream, ok := s.streams[key]
	if !ok {
		return nil, ErrNotFound
	}

	var startID *entry_id.EntryID = nil
	if start == "-" {
		startID = &entry_id.EntryID{
			MsTime: 0,
			SeqNum: 1,
		}
	} else {
		var err error = nil
		startID, err = entry_id.ParseID(start)
		if err == entry_id.ErrGenerateSeqNum {
			startID.SeqNum = 0
		}
		if err != nil {
			return nil, err
		}
	}

	var endID *entry_id.EntryID = nil
	if end == "+" {
		endID = &entry_id.EntryID{
			MsTime: math.MaxInt,
			SeqNum: math.MaxInt,
		}
	} else {
		var err error = nil
		endID, err = entry_id.ParseID(end)
		if err == entry_id.ErrGenerateSeqNum {
			endID.SeqNum = math.MaxInt
		}
		if err != nil {
			return nil, err
		}
	}

	entries := make([]StreamEntry, 0, len(stream))

	for _, entry := range stream {
		if entry.ID.Less(startID) {
			continue
		}
		if entry.ID.Greater(endID) {
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
