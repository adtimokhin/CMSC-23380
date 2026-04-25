package store_test

import (
	"sync"
	"testing"

	"kvraft/internal/store"
)

func TestPutAndGet(t *testing.T) {
	s := store.New()
	s.Put("x", "hello")
	e, ok := s.Get("x")
	if !ok {
		t.Fatal("Get after Put: expected found=true")
	}
	if e.Value != "hello" {
		t.Fatalf("value: want hello, got %q", e.Value)
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
	s.Put("k", "v1")
	s.Put("k", "v2")
	e, ok := s.Get("k")
	if !ok || e.Value != "v2" {
		t.Fatalf("overwrite: want v2, got %q", e.Value)
	}
}

func TestSnapshot(t *testing.T) {
	s := store.New()
	s.Put("a", "1")
	s.Put("b", "2")
	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len: want 2, got %d", len(snap))
	}
	if snap["a"].Value != "1" || snap["b"].Value != "2" {
		t.Fatalf("snapshot contents wrong: %v", snap)
	}
	// Mutating snap must not affect store.
	snap["a"] = store.Entry{Value: "modified"}
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
	s.Put("x", "1")
	s.Put("y", "2")
	if s.Len() != 2 {
		t.Fatalf("len after 2 puts: want 2, got %d", s.Len())
	}
	s.Put("x", "3") // overwrite, not new key
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
		go func() {
			defer wg.Done()
			s.Put("key", "val")
		}()
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
	// Just verify no race or panic — value is nondeterministic under concurrency.
}
