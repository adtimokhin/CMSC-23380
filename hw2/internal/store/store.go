// Package store provides a thread-safe in-memory key-value store that tracks
// the Lamport timestamp of each write.
//
// The store is intentionally simple — no TTLs, no eviction, no persistence.
// It is the in-memory state that a replicated node maintains. Timestamps are
// used to order concurrent writes and to determine which replica has the most
// recent value for a key.
//
// This file is provided. Do not modify it.
package store

import "sync"

// Entry is a stored value together with the Lamport timestamp of the write
// that created it.
type Entry struct {
	Value string
	Ts    int64
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

// Put stores key → value with the given Lamport timestamp. If an entry already
// exists for key, it is unconditionally overwritten. (Conflict resolution based
// on timestamps is left to the caller.)
func (s *Store) Put(key, value string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entry{Value: value, Ts: ts}
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
