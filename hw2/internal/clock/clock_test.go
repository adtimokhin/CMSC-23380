package clock_test

import (
	"sync"
	"testing"

	"kvstore/internal/clock"
)

func TestTick(t *testing.T) {
	c := clock.New()
	if got := c.Now(); got != 0 {
		t.Fatalf("initial value: want 0, got %d", got)
	}
	if got := c.Tick(); got != 1 {
		t.Fatalf("first tick: want 1, got %d", got)
	}
	if got := c.Tick(); got != 2 {
		t.Fatalf("second tick: want 2, got %d", got)
	}
}

func TestUpdateMaxRule(t *testing.T) {
	c := clock.New()
	c.Tick() // local = 1

	// received > local: should set to received+1
	got := c.Update(10)
	if got != 11 {
		t.Fatalf("Update(10) on clock=1: want 11, got %d", got)
	}
	if c.Now() != 11 {
		t.Fatalf("Now() after Update(10): want 11, got %d", c.Now())
	}
}

func TestUpdateLocalHigher(t *testing.T) {
	c := clock.New()
	for i := 0; i < 5; i++ {
		c.Tick() // local = 5
	}
	// received < local: should increment local by 1
	got := c.Update(3)
	if got != 6 {
		t.Fatalf("Update(3) on clock=5: want 6, got %d", got)
	}
}

func TestUpdateEqual(t *testing.T) {
	c := clock.New()
	c.Tick() // local = 1
	// received == local: should increment by 1
	got := c.Update(1)
	if got != 2 {
		t.Fatalf("Update(1) on clock=1: want 2, got %d", got)
	}
}

func TestMonotonicallyIncreasing(t *testing.T) {
	c := clock.New()
	prev := c.Now()
	for i := 0; i < 100; i++ {
		next := c.Tick()
		if next <= prev {
			t.Fatalf("tick %d: clock went from %d to %d (not strictly increasing)", i, prev, next)
		}
		prev = next
	}
}

func TestConcurrentTicks(t *testing.T) {
	c := clock.New()
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c.Tick()
		}()
	}
	wg.Wait()
	if got := c.Now(); got != n {
		t.Fatalf("after %d concurrent ticks: want %d, got %d", n, n, got)
	}
}

// TestHappensBefore verifies the core Lamport invariant:
// if we simulate "send on A, receive on B", then clock(A) < clock(B).
func TestHappensBefore(t *testing.T) {
	a := clock.New()
	b := clock.New()

	// Some internal events on b first
	b.Tick() // b = 1
	b.Tick() // b = 2

	// a sends: tick a
	sendTs := a.Tick() // a = 1

	// b receives message with a's send timestamp
	recvTs := b.Update(sendTs) // b = max(2,1)+1 = 3

	// The send (sendTs=1) must have a lower timestamp than the receive (recvTs=3)
	if sendTs >= recvTs {
		t.Fatalf("Lamport invariant violated: send ts %d >= recv ts %d", sendTs, recvTs)
	}
}
