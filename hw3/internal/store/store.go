// Package store provides a thread-safe in-memory key-value store.
//
// The store is intentionally simple — no TTLs, no eviction, no persistence.
// It is the in-memory state that a replicated node maintains.
//
// This file is provided. Do not modify it.
package store

import "sync"

// Entry is a stored value.
type Entry struct {
	Value string
}

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]Entry)}
}

// Put stores key → value, overwriting any existing entry.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{Value: value}
}

// Get returns the entry for key and true, or the zero Entry and false if the
// key does not exist.
func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// Snapshot returns a copy of all current key-value entries.
// Useful for debugging, log replay, and tests.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
