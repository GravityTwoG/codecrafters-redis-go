package streams_store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/streams-store/id"
	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
)

var ErrNotFound = errors.New("not-found")
var ErrExpired = errors.New("expired")

type StreamEntry struct {
	ID     entry_id.EntryID
	Values []string
}

type Stream = []StreamEntry

type StreamListener struct {
	key     string
	startID string
	endID   string
	added   chan struct{}
}

type StreamsStore struct {
	wg        *sync.WaitGroup
	streamsMu *sync.RWMutex
	streams   map[string]Stream

	listenersMu     *sync.RWMutex
	streamListeners []*StreamListener
}

func NewStreamsStore(wg *sync.WaitGroup) *StreamsStore {
	return &StreamsStore{
		wg: wg,

		streamsMu: &sync.RWMutex{},
		streams:   make(map[string]Stream),

		listenersMu:     &sync.RWMutex{},
		streamListeners: make([]*StreamListener, 0),
	}
}

func (s *StreamsStore) Append(
	key string,
	id string,
	values []string,
) (string, error) {

	if len(values)%2 != 0 {
		return "", fmt.Errorf("invalid number of values")
	}

	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()

	defer func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.notifyStreamListeners(key)
		}()
	}()

	stream, ok := s.streams[key]
	if !ok || len(stream) == 0 {
		return s.createStream(key, id, values)
	}

	prevEntry, _ := utils.Last(stream)

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

func (s *StreamsStore) createStream(
	key string,
	id string,
	values []string,
) (string, error) {

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

func (s *StreamsStore) notifyStreamListeners(key string) {
	s.listenersMu.RLock()
	defer s.listenersMu.RUnlock()

	for _, listener := range s.streamListeners {
		if listener.key != key {
			continue
		}

		entries, err := s.RangeExclusive(
			key,
			listener.startID,
			listener.endID,
		)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			continue
		}
		if len(entries) == 0 {
			continue
		}
		// non-blocking write
		select {
		case listener.added <- struct{}{}:
		default:
		}
	}
}

func (s *StreamsStore) WaitForADD(ctx context.Context, key, start, end string) error {
	listener := &StreamListener{
		key:     key,
		startID: start,
		endID:   end,
		added:   make(chan struct{}),
	}
	s.listenersMu.Lock()
	s.streamListeners = append(s.streamListeners, listener)
	s.listenersMu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-listener.added:
		close(listener.added)
		s.listenersMu.Lock()
		defer s.listenersMu.Unlock()

		idx := slices.Index(s.streamListeners, listener)
		utils.RemoveIndex(s.streamListeners, idx)
		return nil
	}
}

func (s *StreamsStore) ParseStartID(key string, id string) string {
	if id != "$" {
		return id
	}

	startID := "0-1"

	entries, ok := s.Get(key)
	if ok && len(entries) > 0 {
		lastEntry, _ := utils.Last(entries)
		fmt.Printf("StartID: %s\n", lastEntry.ID.String())
		return lastEntry.ID.String()
	}

	return startID
}

func (s *StreamsStore) Get(key string) (Stream, bool) {
	s.streamsMu.RLock()
	defer s.streamsMu.RUnlock()

	values, ok := s.streams[key]
	if !ok {
		return nil, false
	}

	return values, true
}

func (s *StreamsStore) Range(
	key string,
	start string,
	end string,
) (Stream, error) {
	s.streamsMu.RLock()
	defer s.streamsMu.RUnlock()

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
		endID = entry_id.MaxID()
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
		if entry.ID.Between(startID, endID) {
			entries = append(entries, entry)
			continue
		}

		if entry.ID.Greater(endID) {
			break
		}
	}

	return entries, nil
}

func (s *StreamsStore) RangeExclusive(
	key string,
	start string,
	end string,
) (Stream, error) {
	s.streamsMu.RLock()
	defer s.streamsMu.RUnlock()

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
		endID = entry_id.MaxID()
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
		if entry.ID.BetweenExclusive(startID, endID) {
			entries = append(entries, entry)
			continue
		}

		if entry.ID.Greater(endID) {
			break
		}
	}

	return entries, nil
}
