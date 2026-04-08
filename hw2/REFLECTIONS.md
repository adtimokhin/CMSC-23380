# HW2 Reflections

**Name:**
**GitHub Username:**
**Hours spent:**

---

## Stage Progress

Check off each stage as you complete it:

- [ ] Stage 1 — Single-node KV store
- [ ] Stage 2 — Replication
- [ ] Stage 3 — Heartbeat & failure detection
- [ ] Stage 4 — Leader election
- [ ] Stage 5 — Client redirect
- [ ] Extension A — Write-ahead log
- [ ] Extension B — Chain replication

---

## Design Decisions

For each decision, state what you chose and why.

### 1. Replicate fan-out strategy

_Did you send replication RPCs to backups sequentially or in parallel?
How does your choice affect Put latency when both backups are healthy?
What about when one backup is slow?_

**Your answer:**

---

### 2. Replication timeout

_What context timeout did you use for each Replicate RPC?
What happens if it is too short? Too long?
What value did you choose and why?_

**Your answer:**

---

### 3. Consistency model

_Does your implementation serve Get only from the primary, or from any node?
What consistency guarantee does each choice give — linearizable, sequentially
consistent, or causally consistent? Which level does your implementation achieve?
Give a concrete 3-step execution trace that demonstrates it._

**Your answer:**

---

### 4. Failure detection thresholds

_What values did you choose for HeartbeatInterval and HeartbeatTimeout?
How did you reason about the tradeoff between false-positive rate (suspecting
a live node) and detection latency (time to notice a real crash)?_

**Your answer:**

---

### 5. Simultaneous elections

_When both backups detect the primary's failure at the same time, both call
runElection concurrently. Walk through what happens in your implementation
step by step. How does it guarantee exactly one winner?_

**Your answer:**

---

## Reflection Questions

### Q1 — Lamport clock placement (Lec 3)

When does the primary call `clock.Tick()` in your `Put` implementation — before
or after writing to the local store, and before or after issuing `Replicate` RPCs?

Give a concrete 2-node execution trace showing what breaks if you move the
`Tick()` call to a different position.

**Your answer:**

---

### Q2 — False positives in failure detection (Lec 4)

Your heartbeat-based detector is a `◇P` (eventually perfect) failure detector,
not a `P` (perfect) detector. Describe a realistic scenario — such as a garbage
collection pause or a TCP retransmit storm — in which your detector temporarily
suspects a live node.

What happens in your election protocol when two backups both suspect the
primary at the same time? Is your system safe in this scenario?

**Your answer:**

---

### Q3 — Consistency model (Lec 5)

Your Design Decision 3 above identified the consistency level your system
provides. Now justify it more formally.

Give a concrete 3-step execution — for example, a Put on the primary followed
by two Gets issued concurrently by different clients — that an observer could
use to distinguish your system's consistency level from a strictly stronger one.
Use the definitions from Lecture 5. If your system is sequentially consistent
but not linearizable, show an execution that a linearizable system could not
produce but yours can.

**Your answer:**

---

### Q4 — Replication tradeoffs (Lec 6)

Your primary waits for ACKs from both backups before responding to the client.

1. What happens to write latency if one backup is alive but slow (e.g., 500ms RTT)?
2. If you changed the protocol to wait for only one backup ACK instead of two,
   how would that affect your fault-tolerance guarantee?
3. Using the formula from Lecture 6, what is the minimum number of total nodes
   needed to tolerate f=2 simultaneous crash-stop failures while still being
   able to commit writes?

**Your answer:**

---

## Challenges and Surprises

_Describe the hardest part of this assignment. What did you expect to be simple
that turned out to be tricky? What did you learn from it?_

**Your answer:**

---

## Looking Ahead — HW3

Your HW2 implementation has two hard correctness problems that no amount of tuning
can fix:

**Split-brain.** If both backups detect the primary's failure simultaneously, both
may elect themselves primary. Your Lamport-clock tiebreak is a heuristic, not a proof:
there is no guarantee the "winner" has the most up-to-date state, and nothing prevents
the loser from continuing to serve clients.

**Lost writes.** A `Put` that the primary acknowledged to the client may disappear
after the primary crashes if the winning backup did not receive the `Replicate` RPC
before the crash.

In HW3 you will replace this replication layer with **Raft consensus**, which
eliminates both problems via term numbers, majority quorums, and a replicated log.
The client-facing gRPC interface (`Put`, `Get`, `GetPrimary`) stays exactly the same.
