// Package clock provides a Lamport logical clock safe for concurrent use.
//
// A Lamport clock assigns a monotonically increasing integer timestamp to
// events so that if event a happens-before event b (a → b), then
// clock(a) < clock(b). The converse does not hold — two events with
// different timestamps may be concurrent.
//
// Rules (Lamport 1978):
//  1. On any internal event or send: Tick() — increment and stamp the event.
//  2. On receive of a message with timestamp t: Update(t) — apply the max-rule
//     and increment: local = max(local, t) + 1.
//
// This file is provided. Do not modify it.
package clock

import "sync"

// LamportClock is a thread-safe Lamport logical clock.
type LamportClock struct {
	mu sync.Mutex
	t  int64
}

// New returns a new LamportClock with an initial value of 0.
func New() *LamportClock {
	return &LamportClock{}
}

// Tick increments the clock by 1 and returns the new value.
// Call Tick on every internal event and every send.
func (c *LamportClock) Tick() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t++
	return c.t
}

// Now returns the current clock value without incrementing it.
func (c *LamportClock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Update applies the Lamport max-rule: sets the clock to max(local, received)+1
// and returns the new value. Call Update on every receive.
func (c *LamportClock) Update(received int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.t {
		c.t = received
	}
	c.t++
	return c.t
}
