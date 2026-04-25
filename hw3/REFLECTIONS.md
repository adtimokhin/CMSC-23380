# HW3 Reflections

**Name:**
**GitHub Username:**
**Hours spent:**

---

## Stage Progress

- [ ] Stage 1 — Leader election
- [ ] Stage 2 — Log replication
- [ ] Stage 3 — Safety under failures
- [ ] Extension — Log compaction

---

## Reflection Questions

### Q1 — Safety (Lec 7)

Can two leaders exist simultaneously in your implementation?

Walk through the term-number argument for why not. Then: under what network
condition could a *partitioned* leader still believe it is the leader? Is that
safe (can it commit new entries)?

**Your answer:**

---

### Q2 — HW2 comparison (Lec 4)

Give a concrete execution where HW2's Lamport-bully election elects a backup
that is missing the last write that was acknowledged to a client. Trace through:

1. The write that was lost
2. Why the winning backup didn't have it
3. What the client observes after the election

Why can this not happen in your Raft implementation? Reference §5.4.1 of the
Raft paper (the election restriction).

**Your answer:**

---

### Q3 — Commit rule (Lec 8)

Why does Raft's commit rule require that the entry being committed is from the
*current term* (not just any term)?

Reconstruct the exact scenario from Figure 8 of the Raft paper step-by-step.
At each step, identify which log entries exist on which nodes, which node is
the leader, and what commitIndex each node holds. Show where a naive commit
rule (ignoring the current-term requirement) would make Raft unsafe.

**Your answer:**

---

### Q4 — Consistency model (Lec 5)

Are your Get operations linearizable?

If you implemented stale-leader reads (Get does not go through Raft):
construct a 3-operation history — one Put and two Gets from two different
clients — that violates linearizability. Be precise about timing.

What would the read-index protocol add to make reads linearizable? Would it
require an extra round-trip?

**Your answer:**

---

### Q5 — Election timeout randomization (Lec 6)

The skeleton uses a random timeout drawn from `[ElectionTimeoutMin, ElectionTimeoutMax]`.
Explain why randomization is necessary — what liveness failure would a single
fixed timeout produce across all nodes? What constraint must the range satisfy
relative to `HeartbeatInterval`, and why?

**Your answer:**

---

### Q6 — Read semantics (Lec 5)

Your `Get` handler reads directly from the local store without going through Raft.
Describe a concrete partition scenario where a client reads a stale value from a
node that still believes it is leader. What does the read-index protocol (Raft §6.4)
do differently to prevent this, and what is its cost in extra round-trips?

**Your answer:**

---

## Challenges and Surprises

_What was the hardest bug you had to debug? How did you find it?
What would you do differently if you started over?_

**Your answer:**
