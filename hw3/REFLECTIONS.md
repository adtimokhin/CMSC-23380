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

Consider a three-node hw2 cluster: P=node0, B1=node1, B2=node2. Suppose the cluster has been running normally for some time: many puts have been processed, so all nodes have clock values around 10-15 from ticking and receiving Replicate RPCs.

Now B2 suffers a temporary network condition where B2 can still reach B1 for heartbeats, but P's Replicate RPCs to B2 start timing out. B1 and B2 continue exchanging heartbeats, and because each heartbeat is a Lamport event, B2's clock climbs to 50 while B1's stays around 20 (B1 receives fewer heartbeats due to being closer to P).

The client sends Put("x","v_new"). In hw2's Put handler (server/main.go), P fans out Replicate to both B1 and B2, waits on wg.Wait(), and only returns ok if ALL replications succeed. In this execution, B1 acknowledges, but the Replicate to B2 times out. P returns an error to the client. The client retries—and keeps retrying—but P eventually processes the write in a way where B1 receives it. If P (through a timeout or implementation decision) proceeds after a partial replication and returns ok, the client now has a confirmed "x"="v_new", but only P and B1 hold it. B2 still has the old value.

P then crashes. B2's heartbeat timer fires: B2 calls Tick() (clock→51) and sends AnnounceLeader to B1 and P. B1's electionTs is 0 (or lower than 51 since B1 hasn't started its own election yet), so the AnnounceLeader comparison `req.ElectionLamport > localTs` passes and B1 accepts B2 as primary. B2 wins, and it holds the stale value of "x".

When the client now issues Get("x") it observes the old value, even though a Put was acknowledged. This is a safety violation.

In Raft this cannot happen because of the election restriction. A candidate must have a log at least as up-to-date as a majority: the follower compares (lastLogTerm, lastLogIndex) and grants a vote only if the candidate's log is not behind. Because B2 missed the Replicate for "v_new", its log is shorter (lower lastLogIndex) than B1's. When B2 sends RequestVote, B1 will deny the vote (B1's lastLogIndex > B2's lastLogIndex). B2 cannot get a majority and cannot become leader. Only a node that has all committed entries—like B1—can win, preserving the invariant that committed data is never lost.

---

### Q3 — Commit rule (Lec 8)

Why does Raft's commit rule require that the entry being committed is from the
*current term* (not just any term)?

Reconstruct the exact scenario from Figure 8 of the Raft paper step-by-step.
At each step, identify which log entries exist on which nodes, which node is
the leader, and what commitIndex each node holds. Show where a naive commit
rule (ignoring the current-term requirement) would make Raft unsafe.

**Your answer:**

The scenario involves five nodes S1–S5.

1. S1 is leader in term 2. It replicates an entry (index=2, term=2) to S2. S3–S5 do not receive it. S1 then crashes. At this point: S1 and S2 have [_, T2@2] in their logs; S3–S5 have [_, _]; commitIndex is 0 everywhere (no majority yet).

2. S5 becomes leader (term 3). A client write arrives; S5 appends entry (index=2, term=3) to its own log but crashes before replicating it. Log: S5 has [_, T3@2]; nobody else gets this entry. S1 recovers.

3. S1 becomes leader again (term 4). S1 replicates its old entry (index=2, term=2) to S2 and S3—now a majority of three nodes (S1, S2, S3) hold this entry. Under a naive commit rule—"commit if a majority holds the entry, regardless of term"—S1 would set commitIndex=2 and consider this entry committed. S1 then crashes again before it can replicate a new term-4 entry.

4. S5 is now eligible to become leader (S5's last entry has term=3, which is higher than S2's and S3's term=2 last entry). S5 wins the election and overwrites index=2 on S1, S2, and S3 with its own (index=2, term=3) entry. The entry that S1 "committed" in step (c) is now gone.

This is the safety violation: a committed entry was overwritten. The problem is that S5's last entry (term=3) looked more current than S2's and S3's (term=2), so S5 could collect enough votes and overwrite history.

Raft's fix is to require that a leader can only commit an entry from the CURRENT term. In step 3, S1 cannot commit the (term=2) entry directly even if a majority holds it. Instead, S1 must first append a new entry in term 4 (in my implementation this is the no-op appended in becomeLeader). Once that term-4 entry reaches a majority, commitIndex advances past index=2, making both entries durable. Now S2 and S3 have a term-4 entry in their logs: their lastLogTerm=4 > S5's lastLogTerm=3, so they deny S5's vote. S5 can never win and can never overwrite the committed data.

---

### Q4 — Consistency model (Lec 5)

Are your Get operations linearizable?

If you implemented stale-leader reads (Get does not go through Raft):
construct a 3-operation history — one Put and two Gets from two different
clients — that violates linearizability. Be precise about timing.

What would the read-index protocol add to make reads linearizable? Would it
require an extra round-trip?

**Your answer:**

It depends on which node the client reads from. Reads from the leader are actually linearizable: the server's `applyLoop` in server/main.go calls `s.st.Put` and *then* closes `pp.ch`, so by the time `Put` returns `ok=true` to the client, the leader's store already holds the new value. Any subsequent `Get` to the leader will see it.

Reads from a follower, however, are not linearizable. A follower's store is updated asynchronously by its own applyLoop, which only fires once it receives a LeaderCommit update via AppendEntries. There is a window where the follower has the log entry but its store has not yet reflected it.

Here is a concrete 3-operation history that violates linearizability using a follower read. Three nodes: leader L (node 0), follower F1 (node 1), follower F2 (node 2). Client A and Client B are separate processes.

At T=1, Client A sends Put("x","1") to L. The write is replicated to a majority, committed, and L returns ok. L's store has x=1.

At T=2, Client B sends Get("x") to F1. F1's applyLoop has not yet applied the commit because the AppendEntries carrying LeaderCommit is still in flight or the applyLoop hasn't ticked yet. F1 returns x="" (not found).

At T=3, Client A sends Get("x") to L and receives x=1.

In a linearizable history both Gets must see x=1 because the Put completed before them. But Client B got "". No valid serial order exists — linearizability is violated.

The read-index protocol makes all reads linearizable(including from followers and partitioned leaders) by having the serving node record its current commitIndex as readIndex, send a heartbeat to confirm it still holds a majority, wait until applyIndex >= readIndex, and only then read from the store. This requires one extra round-trip (the confirmation heartbeat).

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

Three-node cluster: L (node 0, current leader), F1 (node 1), F2 (node 2). Initially x="old" is committed everywhere.

A network partition forms: L loses connectivity to both F1 and F2. From L's point of view the heartbeats it sends receive no replies, but it does not step down (it simply keeps trying). F1 and F2 can reach each other—they hold a majority. After their election timer fires, F1 wins the election (term 2). Client A sends Put("x","new") to F1. F1 replicates it to F2, commits it (majority: F1+F2), and returns ok. x="new" is now durably committed cluster-wide.

Meanwhile, L still believes it is the term-1 leader. Its local store has x="old". Client B happens to contact L with Get("x"). Our implementation returns x="old" immediately from the local store. Client B reads stale data even though "new" was already durably committed.

The read-index protocol prevents this. Before serving the Get, L would record readIndex = currentCommitIndex, then broadcast a heartbeat and wait for acknowledgements from a majority. Because L is partitioned, it cannot collect a majority reply. The read stalls until the leader confirms it still holds a majority—which it cannot do in this partition. L would either time out and return an error, or the client would redirect to the real leader. Either way, no stale read is served.

The cost is one extra round-trip per read: the leader-confirmation heartbeat to a majority before the value is returned. Lease-based reads can eliminate this round-trip by relying on bounded clock drift, but that requires a synchronized clock assumption and is not part of the basic Raft protocol.

---

## Challenges and Surprises

_What was the hardest bug you had to debug? How did you find it?
What would you do differently if you started over?_

**Your answer:**

The hardest bug was a subtle interaction between `sendHeartbeats` and `commitIndex`. My first implementation sent empty AppendEntries from the heartbeat loop without including `LeaderCommit`, which meant that followers received heartbeats with `leaderCommit=0` and never advanced their commit index, even though the leader had already committed entries. Tests for Stage 2 were passing for the leader node but failing for followers because their stores never got updated. I found it by adding log lines to print `commitIndex` on every AppendEntries send and receive: the mismatch between what the leader sent and what followers recorded was immediately visible.

A second hard bug involved the timing of `applyLoop`. The loop used `time.Sleep(10ms)` between iterations. This created a race where the test's `waitForLeader` would detect the new leader (whose `state == Leader` flag was set) and immediately call `Get`, but the 10ms sleep meant `applyLoop` had not yet applied the newly committed entries. I replaced the sleep with a `sync.Cond` that signals whenever `commitIndex` advances—this makes the applyLoop react within microseconds rather than waiting for the next timer tick, and eliminated the race completely.

If I started over I would write the applyLoop with a `sync.Cond` from day one, and I would run the stability check (multiple test runs) much earlier rather than only at the end. Most of my debugging time was spent on bugs that a two-run stability check would have exposed immediately.
