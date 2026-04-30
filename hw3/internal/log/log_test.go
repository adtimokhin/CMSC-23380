package log_test

import (
	"testing"

	"kvraft/internal/log"
)

func TestEmpty(t *testing.T) {
	l := log.New()
	if l.LastIndex() != 0 {
		t.Fatalf("empty log LastIndex: want 0, got %d", l.LastIndex())
	}
	if l.LastTerm() != 0 {
		t.Fatalf("empty log LastTerm: want 0, got %d", l.LastTerm())
	}
	if l.Len() != 0 {
		t.Fatalf("empty log Len: want 0, got %d", l.Len())
	}
}

func TestAppendAndAt(t *testing.T) {
	l := log.New()
	l.Append(log.LogEntry{Index: 1, Term: 1, Command: "put:x:1"})
	l.Append(log.LogEntry{Index: 2, Term: 1, Command: "put:y:2"})
	l.Append(log.LogEntry{Index: 3, Term: 2, Command: "put:z:3"})

	if l.LastIndex() != 3 {
		t.Fatalf("LastIndex: want 3, got %d", l.LastIndex())
	}
	if l.LastTerm() != 2 {
		t.Fatalf("LastTerm: want 2, got %d", l.LastTerm())
	}
	if l.Len() != 3 {
		t.Fatalf("Len: want 3, got %d", l.Len())
	}

	e, ok := l.At(2)
	if !ok {
		t.Fatal("At(2): expected found=true")
	}
	if e.Command != "put:y:2" || e.Term != 1 {
		t.Fatalf("At(2): want put:y:2 term=1, got %q term=%d", e.Command, e.Term)
	}

	_, ok = l.At(0)
	if ok {
		t.Fatal("At(0): expected found=false (sentinel)")
	}
	_, ok = l.At(4)
	if ok {
		t.Fatal("At(4): expected found=false (out of range)")
	}
}

func TestTruncate(t *testing.T) {
	l := log.New()
	for i := int64(1); i <= 5; i++ {
		l.Append(log.LogEntry{Index: i, Term: 1, Command: "cmd"})
	}
	l.Truncate(3) // remove entries 3, 4, 5

	if l.LastIndex() != 2 {
		t.Fatalf("after Truncate(3): LastIndex want 2, got %d", l.LastIndex())
	}
	if l.Len() != 2 {
		t.Fatalf("after Truncate(3): Len want 2, got %d", l.Len())
	}
	_, ok := l.At(3)
	if ok {
		t.Fatal("entry at index 3 should be gone after Truncate(3)")
	}

	// Can append again after truncation
	l.Append(log.LogEntry{Index: 3, Term: 2, Command: "new"})
	e, ok := l.At(3)
	if !ok || e.Term != 2 {
		t.Fatalf("after re-append: At(3) want term=2, got %v ok=%v", e, ok)
	}
}

func TestTruncateNoOp(t *testing.T) {
	l := log.New()
	l.Append(log.LogEntry{Index: 1, Term: 1, Command: "cmd"})
	l.Truncate(0)   // invalid index: no-op
	l.Truncate(100) // beyond end: no-op
	if l.Len() != 1 {
		t.Fatalf("no-op truncate should not change len: got %d", l.Len())
	}
}

func TestEntries(t *testing.T) {
	l := log.New()
	for i := int64(1); i <= 5; i++ {
		l.Append(log.LogEntry{Index: i, Term: int64(i), Command: "cmd"})
	}

	got := l.Entries(2, 4) // [2, 4) → entries 2 and 3
	if len(got) != 2 {
		t.Fatalf("Entries(2,4): want 2 entries, got %d", len(got))
	}
	if got[0].Index != 2 || got[1].Index != 3 {
		t.Fatalf("Entries(2,4): wrong indices %v", got)
	}

	// Empty / invalid ranges
	if l.Entries(3, 3) != nil {
		t.Fatal("Entries(3,3): want nil (empty range)")
	}
	if l.Entries(5, 2) != nil {
		t.Fatal("Entries(5,2): want nil (inverted range)")
	}
}

func TestAppendPanicWrongIndex(t *testing.T) {
	l := log.New()
	l.Append(log.LogEntry{Index: 1, Term: 1, Command: "ok"})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on wrong index, got none")
		}
	}()
	l.Append(log.LogEntry{Index: 5, Term: 1, Command: "wrong index"})
}
