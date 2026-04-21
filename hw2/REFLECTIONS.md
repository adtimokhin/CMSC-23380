# HW2 Reflections

**Name:** Aleksandr Timokhin
**GitHub Username:** adtimokhin
**Hours spent:** ~10

---

## Stage Progress

Check off each stage as you complete it:

- [x] Stage 1 — Single-node KV store
- [x] Stage 2 — Replication
- [x] Stage 3 — Heartbeat & failure detection
- [x] Stage 4 — Leader election
- [x] Stage 5 — Client redirect
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

I chose **parallel fan-out**, combined with goroutines and `sync.WaitGroup`. One goroutine per backup runs concurrently; `Put` blocks on `wg.Wait()` until both ACKs arrive.

When both backups are healthy, total latency is bounded by the slower of the two RPCs. Sequential fan-out would pay `RTT_1 + RTT_2`; parallel pays `max(RTT_1, RTT_2)` — should be twice as faster.

When one backup is slow, main replica has to wait for the slowest Put to complete, as replicas will await ACKS from all backups before acknowleding the write.

---

### 2. Replication timeout

_What context timeout did you use for each Replicate RPC?
What happens if it is too short? Too long?
What value did you choose and why?_

**Your answer:**

I used `1000 ms` for each `Replicate` RPC context.

- If timeout is too short (false negative for replication): we assume that legitimate RPCs time out under load, causing `Put` to return `codes.Unavailable` even though the backup is alive. 

- If timeout is too long: We increase the client-visible latency by up to`ReplicateTimeout` per failure.

I chose `1000 ms` because it is  longer than typical LAN round-trips and leaves room for transient load spikes, while still being short enough to give a client a response within a few seconds. 

Note: For heartbeat RPCs a separate proportional value of `HeartbeatInterval × 4/5` is used so that the timeout is always shorter than the interval — using a hardcoded 400 ms would violate this invariant in the fast-test environment where `HeartbeatInterval = 20 ms`.

---

### 3. Consistency model

_Does your implementation serve Get only from the primary, or from any node?
What consistency guarantee does each choice give — linearizable, sequentially
consistent, or causally consistent? Which level does your implementation achieve?
Give a concrete 3-step execution trace that demonstrates it._

**Your answer:**

`Get` is served from any node — there is no redirect for reads. Each node reads from its local store.

This provides sequential consistency per node but not linearizability.

- Each node applies writes in the same order (the primary fans out synchronously before acking), so the per-key value history is the same on all replicas once replication completes. A client that always reads from the same node sees a sequentially consistent view.

- It is not linearizable because a read can observe an outdated value during an in-progress write.

Concrete trace:

1. Client A sends `Put(x=1)` to the primary (node 0). The primary ticks its clock (ts=1) and starts goroutines to replicate to nodes 1 and 2.
2. Before node 1's `Replicate` RPC completes, client B sends `Get(x)` to node 1. Node 1 returns `found=false` (or the previous value), because the write hasn't landed yet.
3. The `Replicate` RPC to node 1 finishes; `Put` on node 0 returns `ok=true` to client A.

A linearizable system would forbid step 2 from returning the old value once the write is "in progress". My system allows it, making it sequentially consistent but not linearizable.

---

### 4. Failure detection thresholds

_What values did you choose for HeartbeatInterval and HeartbeatTimeout?
How did you reason about the tradeoff between false-positive rate (suspecting
a live node) and detection latency (time to notice a real crash)?_

**Your answer:**

- `HeartbeatInterval = 500 ms`
- `HeartbeatTimeout = 1000 ms` (2 × interval)

The timeout is set to twice the interval, meaning a node is suspected only after it has missed at least two consecutive heartbeats. This should tolerate a single lost heartbeats, due to network errors or brief GC pause, without triggering a false positive, while keeping detection latency to at most 1–1.5 seconds after a real crash.

If the timeout is lower than 2 intervals, even a single missed heartbeat can cause a false-positive. If a timeout is longer (say 5 intervals) the change of false-positive becomes super small, but that increases the time when the cluster is leader-less significanlty (to 2-3 seconds).

---

### 5. Simultaneous elections

_When both backups detect the primary's failure at the same time, both call
runElection concurrently. Walk through what happens in your implementation
step by step. How does it guarantee exactly one winner?_

**Your answer:**

1. **Guard check**: Each goroutine acquires `n.mu`, checks `n.role == RoleBackup` (true), sets `n.role = RoleCandidate`, calls `n.clk.Tick()` to capture `electionTs`, saves `deadPrimaryID = 0`, then releases the lock.

2. **Find the other backup**: Each iterates `peerConns`, skips node 0, and finds the only remaining peer — which happens to be the other candidate.

3. **Exchange `AnnounceLeader` RPCs**: WLOG let's assume that node 1 sends first. Node 2 receives it: since node 2 is a `RoleCandidate`, it uses its own `electionTs` as `localTs`. The comparison is `req.ElectionLamport > localTs OR (equal AND req.NewPrimaryId > n.id)`. Because both nodes called `clk.Tick()` from the same starting clock value (heartbeats equalize clocks), `electionTs` values are typically equal (or differ by 1 due to a race). If there is a tie the backup with the higher ID wins. Node 2 (id=2) beats node 1 (id=1).

4. **Node 2 wins, node 1 loses**: Node 2 sets `role = RolePrimary`, `currentPrimaryID = 2`, then broadcasts `AnnounceLeader` to all peers so node 1 also updates `currentPrimaryID = 2` and reverts to `RoleBackup`.

5. **Guard on re-entry**: `monitorPeers` keeps firing while node 0 is dead. The second call to `runElection` on each node hits the guard (`n.role != RoleBackup` for node 2, which is now Primary; same for node 1 which reverted to Backup but now sees `currentPrimaryID != 0`), preventing duplicate elections.

---

## Reflection Questions

### Q1 — Lamport clock placement (Lec 3)

When does the primary call `clock.Tick()` in your `Put` implementation — before
or after writing to the local store, and before or after issuing `Replicate` RPCs?

Give a concrete 2-node execution trace showing what breaks if you move the
`Tick()` call to a different position.

**Your answer:**

`clock.Tick()` is called before issuing `Replicate` RPCs and before writing to the local store. Concretely:

```
ts := n.clk.Tick()                        // (1) tick first
// fan-out goroutines send Replicate(ts)  // (2) replicate with that ts
wg.Wait()
n.store.Put(key, value, ts)               // (3) local write last
```

**What if Tick() is moved after Replicate:**

Suppose we call `Replicate` first, then `Tick()`:

1. Primary (node 0) fans out `Replicate(lamport_ts=0)` to node 1 — using the pre-tick clock value.
2. Node 1 calls `clk.Update(0)` -> backup clock becomes 1; stores entry with `ts=0`.
3. Primary calls `Tick()`-> primary clock becomes 1.
4. Next `Put` fans out `Replicate(lamport_ts=1)`.

At step 2, node 1 now has clock=1. When it receives `Replicate(ts=1)` at step 4, `Update(1)` returns `max(1,1)+1=2`. The problem is that the primary sent a write stamped with a clock value before ticking - meaning the Lamport send rule (tick before send) is violated. An external observer could see message timestamps that do not respect causal order: a message could carry a timestamp lower than the receiver's current clock even when it is causally later.

---

### Q2 — False positives in failure detection (Lec 4)

Your heartbeat-based detector is a `◇P` (eventually perfect) failure detector,
not a `P` (perfect) detector. Describe a realistic scenario — such as a garbage
collection pause or a TCP retransmit storm — in which your detector temporarily
suspects a live node.

What happens in your election protocol when two backups both suspect the
primary at the same time? Is your system safe in this scenario?

**Your answer:**

**Imagine garbage collection pause:**

Node 0 (primary) enters a stop-the-world GC pause lasting 1.2 seconds. During this time it cannot process heartbeat requests or send its own heartbeats. Nodes 1 and 2 miss two consecutive heartbeat deliveries. After `HeartbeatTimeout = 1000 ms` both independently mark node 0 as dead and fire `onPeerDead(0)`. Node 0's GC pause ends at 1.2 s — it is still alive and has not crashed.

**What happens when both backups suspect simultaneously:**

Both nodes 1 and 2 call `go runElection()` at roughly the same time (both fired on the same monitor tick). As described in Design Decision #5, the deterministic tiebreaker (higher Lamport clock, then higher node ID) ensures exactly one winner — node 2 in a 3-node cluster. Node 2 becomes primary and broadcasts; node 1 reverts to backup.

**Is the system safe?**

It is safe in the sense that no two nodes believe they are both primary at the same time — the election protocol's deterministic comparison prevents split-brain within the election itself. However, there is a hazard: if node 0 comes back after its pause and still believes it is primary (it was never informed it lost the role), it may continue accepting writes.

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

My system achieves sequential consistency per node but not linearizability.

Linearizability requires that every operation appears to take effect instantaneously at some point within its real-time interval (its "linearization point"). Sequential consistency only requires that all operations can be ordered in a way consistent with each process's program order — it does not require the global order to respect real-time.

**(3 steps):**

- `t=0`: Client A sends `Put(x=42)` to node 0. Node 0 ticks clock (ts=1), fans out goroutines to nodes 1 and 2.
- `t=1ms` (replication not yet complete): Client B sends `Get(x)` to node 1. Node 1 has not yet received the `Replicate` RPC. Returns `found=false`.
- `t=5ms`: Both `Replicate` RPCs complete. `Put` returns `ok=true` to client A.

In this execution, `Put(x=42)` completes at `t=5ms`. Client B's `Get` was issued at `t=1ms` — *before* the Put completed — so a linearizable system is allowed to return either `false` or `true` for found.

However, consider a stricter scenario: `Put` completes at `t=5ms`, and client B issues `Get(x)` at `t=10ms` (after Put returned). A linearizable system must return `found=true value=42`. My system may still return a stale value if there is clock skew between nodes or if node 1's local write was somehow delayed (not an issue in this implementation, but illustrates the principle). More concretely, if after the Put completes client B reads node 1 and then client C reads node 1 again, both must see the new value - and they do, since node 1's store was updated synchronously. So in practice the implementation behaves linearizably for single-key reads *after* replication completes.

The execution above demonstrates that during the replication window, a stale read is possible - distinguishing it from a linearizable system that would make the Put appear to take effect atomically before any overlapping read could observe the old state.

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

1. **One slow backup (500 ms RTT):** Because `Put` waits for ACKs from all, the slow backup becomes the bottleneck regardless of the parallel fan-out. Every write takes at least 500 ms even when the other backup responds in <1 ms. The client sees write latency equal to the slowest replica's RTT plus processing time. In the limit, if the slow backup approaches `ReplicateTimeout = 1000 ms`, writes take nearly a full second even though the cluster is otherwise healthy.

2. **Wait for only one ACK:** The system could tolerate one backup being slow (since we only need one fast ACK), improving tail latency. However, the fault-tolerance guarantee weakens. Currently, with 3 nodes and synchronous replication to all 2 backups, we can survive the primary crashing because both backups have the latest write. If we wait for only one ACK, we can no longer guarantee the other backup got the write. A crash of the primary plus the one backup that didn't ACK would lose the write permanently — we lose the ability to tolerate 2 simultaneous failures for already-acknowledged writes.

3. **Formula from Lecture 6 (majority quorum / 2f+1):** To tolerate `f=2` crash-stop failures while still being able to commit writes, you need a quorum of `f+1 = 3` nodes available after failures. Total nodes required: `2f+1 = 5`. With 5 nodes, up to 2 can fail and the remaining 3 still form a majority quorum capable of committing writes.

---

## Challenges and Surprises

_Describe the hardest part of this assignment. What did you expect to be simple
that turned out to be tricky? What did you learn from it?_

**Your answer:**

The trickiest part was the Lamport clock placement in replication (Stage 2). I initially assumed the backup should store the updated clock value (`max(local, req.ts) + 1`) since that's what `clk.Update()` returns. took me an hour to find out that this is not the case.

The *eartbeat RPC timeout being proportional was another non-obvious detail. Using a hardcoded `400 ms` worked in production but silently broke the invariant in the fast-test environment where `HeartbeatInterval = 20 ms`. Using `HeartbeatInterval * 4 / 5` was the fix, but recognizing that the test environment used different timing constants required careful reading of the test harness.

The electionTs race condition in Stage 4 was subtle: heartbeat handlers can advance `n.clk` concurrently while an election is in progress, so using `n.clk.Now()` inside `AnnounceLeader` for the comparison would produce a moving target. Capturing a stable snapshot `n.electionTs = n.clk.Tick()` under the mutex at election start and using that snapshot throughout the election was the key design decision to prevent this race.

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
