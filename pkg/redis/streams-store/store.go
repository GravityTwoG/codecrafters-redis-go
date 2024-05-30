package streams_store

import (
	"errors"
	"fmt"
	"math"
	"sync"

	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/streams-store/id"
)

var ErrNotFound = errors.New("not-found")
var ErrExpired = errors.New("expired")

type StreamEntry struct {
	ID     entry_id.EntryID
	Values []string
}

type StreamsStore struct {
	streamsMut *sync.RWMutex
	streams    map[string][]StreamEntry
}

func NewStreamsStore() *StreamsStore {
	return &StreamsStore{
		streamsMut: &sync.RWMutex{},
		streams:    make(map[string][]StreamEntry),
	}
}

func (s *StreamsStore) AppendToStream(key string, id string, values []string) (string, error) {

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

func (s *StreamsStore) GetStream(key string) ([]StreamEntry, bool) {
	s.streamsMut.RLock()
	defer s.streamsMut.RUnlock()

	values, ok := s.streams[key]
	if !ok {
		return nil, false
	}

	return values, true
}

func (s *StreamsStore) Range(key string, start string, end string) ([]StreamEntry, error) {
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

func (s *StreamsStore) RangeExclusive(key string, start string, end string) ([]StreamEntry, error) {
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
	} else if start == "0-0" {
		startID = &entry_id.EntryID{
			MsTime: 0,
			SeqNum: 0,
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
		if entry.ID.LessOrEqual(startID) {
			continue
		}
		if entry.ID.GreaterOrEqual(endID) {
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
