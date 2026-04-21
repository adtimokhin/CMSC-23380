package store_test

import (
	"sync"
	"testing"

	"kvstore/internal/store"
)

func TestPutAndGet(t *testing.T) {
	s := store.New()
	s.Put("x", "hello", 1)
	e, ok := s.Get("x")
	if !ok {
		t.Fatal("Get after Put: expected found=true")
	}
	if e.Value != "hello" {
		t.Fatalf("value: want hello, got %q", e.Value)
	}
	if e.Ts != 1 {
		t.Fatalf("timestamp: want 1, got %d", e.Ts)
	}
}

func TestGetMissing(t *testing.T) {
	s := store.New()
	_, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("Get on missing key: expected found=false")
	}
}

func TestOverwrite(t *testing.T) {
	s := store.New()
	s.Put("k", "v1", 1)
	s.Put("k", "v2", 5)
	e, ok := s.Get("k")
	if !ok || e.Value != "v2" || e.Ts != 5 {
		t.Fatalf("overwrite: want (v2, 5), got (%q, %d)", e.Value, e.Ts)
	}
}

func TestSnapshot(t *testing.T) {
	s := store.New()
	s.Put("a", "1", 1)
	s.Put("b", "2", 2)
	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len: want 2, got %d", len(snap))
	}
	if snap["a"].Value != "1" || snap["b"].Value != "2" {
		t.Fatalf("snapshot contents wrong: %v", snap)
	}
	// Mutating snap must not affect store
	snap["a"] = store.Entry{Value: "modified", Ts: 99}
	e, _ := s.Get("a")
	if e.Value != "1" {
		t.Fatal("snapshot mutation leaked into store")
	}
}

func TestLen(t *testing.T) {
	s := store.New()
	if s.Len() != 0 {
		t.Fatalf("initial len: want 0, got %d", s.Len())
	}
	s.Put("x", "1", 1)
	s.Put("y", "2", 2)
	if s.Len() != 2 {
		t.Fatalf("len after 2 puts: want 2, got %d", s.Len())
	}
	s.Put("x", "3", 3) // overwrite, not new key
	if s.Len() != 2 {
		t.Fatalf("len after overwrite: want 2, got %d", s.Len())
	}
}

func TestConcurrentPutGet(t *testing.T) {
	s := store.New()
	const n = 500
	var wg sync.WaitGroup
	wg.Add(n * 2)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := "key"
			s.Put(key, "val", int64(i))
		}()
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
	// Just verify no race or panic — value is nondeterministic
}
