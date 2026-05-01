# HW3 Reflections

**Name:** Aleksandr Timokhin
**GitHub Username:** adtimokhin
**Hours spent:** 5

---

## Stage Progress

- [x] Stage 1 — Leader election
- [x] Stage 2 — Log replication
- [x]  Stage 3 — Safety under failures
- [] Extension — Log compaction

---

## Reflection Questions

### Q1 — Safety (Lec 7)

Can two leaders exist simultaneously in your implementation?

Walk through the term-number argument for why not. Then: under what network
condition could a *partitioned* leader still believe it is the leader? Is that
safe (can it commit new entries)?

**Your answer:**

In RAFT at most one leader can exist (so two leaders is impossible).

When followers do not hear from a leader for some time (random value in the range [ElectionTimeoutMin, ElectionTimeoutMax]) they start an election with a higher term number than current. If the other followers consider that new candidate has up-to-date log they will vote for him and switch to them as a new leader.

In election only one leader can emerge due to majority quorum - the number of votes needed is n/2 + 1, so if there are 2 candidates. Even if two candidates fire election purposal simultaneously they still need a mojority to become a leader.

Imagine a network partition where the leader looses connection to followers. In the meantime, a new leader is elected with the higher term than the last leader. That last leader then comes back to life and tries to send a message to other nodes with a lower term number. It receives a response from servers, telling them that the current term is higher, and it immediatelly switches to being a follower.

Though for some time there might be two or more machines who believe to be leaders, in actuallity the cluster will have only one actual leader.
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

The reason for randomized timeout is simple - it helps to prevent elections from having 0 leaders. When a single server is elegible to become a leader, all of the servers will give their vote to it. But that is not usually the case.

Servers would consider many other servers up-to-date with their log (term number, and ties are broken with log length). So they would give their vote to any of these servers randomly if they all send the election request simultaneously.

It is possible to end up in a sittuation where multiple candidates get some votes, but not the majority. This sittuation is called liveness failure.

However, if you to add a randomized timeout a single of these candidates will fire before others and will collect all of the eligible votes. If their count is enough (majority) - they become the leader.

Constraint that the range for the election timeout must satisfy - it must be bigger than the heartbeat frequency (and ideally plus some time for the network delay). Otherwise alive leaders might send a heartbeat message too late, and some nodes start an election (which is redundant).

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
