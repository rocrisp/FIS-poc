package state

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var ErrVersionConflict = errors.New("version conflict")

type Store struct {
	mu      sync.RWMutex
	version int64
	data    json.RawMessage
	updated time.Time
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Read() (json.RawMessage, int64, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data, s.version, s.updated
}

func (s *Store) Write(data json.RawMessage, expectedVersion int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.version != expectedVersion {
		return s.version, ErrVersionConflict
	}

	s.version++
	s.data = data
	s.updated = time.Now()
	return s.version, nil
}
