# HW3 — Raft-Based Key-Value Store

Replaces HW2's heuristic primary-backup replication with a full Raft consensus implementation. The client-facing interface (`kvctl put/get/primary`) is unchanged — only the replication backend differs.

| Problem in HW2 | Fix in HW3 |
|----------------|------------|
| Split-brain: a backup that missed the latest write could still win an election | Raft's election restriction (§5.4.1): a candidate must prove its log is at least as up-to-date as a majority before it can collect votes |
| Lost writes: a write ACKed to the client could vanish if the acking backup wasn't the one that survived | Raft only commits an entry once it's on a majority of logs, and only ever commits entries from the leader's *current* term (Figure 8 safety argument) |

Stages 1–3 are complete; the log-compaction/`InstallSnapshot` extension is not implemented.

## Quick start

```bash
go test ./internal/... -v
go build ./...
```

### Docker (recommended)

```bash
make docker-up                       # build + start all 3 nodes
make docker-logs
go run ./client primary localhost:17000   # wait ~300ms for election first
go run ./client put foo bar localhost:17000
go run ./client get foo localhost:17001
make docker-partition NODE=node0     # simulate a network partition
make docker-heal NODE=node0
make docker-down
```

### Local processes

```bash
go run ./server --id=0 --config=nodeconfig.json   # :17000 client / :17100 peer
go run ./server --id=1 --config=nodeconfig.json   # :17001 / :17101
go run ./server --id=2 --config=nodeconfig.json   # :17002 / :17102
```

## Architecture

`raft/raft.go` is the Raft state machine — `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, mirroring Figure 2 of the Raft paper. `server/main.go` wires it to the KVStore gRPC service: an apply loop reads committed entries off `commitCh` (`ApplyMsg{Index, Term, Command}`, where `Command` is `"put:key:value"`) and applies them to the store. `internal/log/log.go` (1-indexed, sentinel at index 0) and the proto definitions are provided and unmodified.

| File | Role |
|------|------|
| `raft/raft.go` | Leader election, log replication, safety — the implementation |
| `server/main.go` | Raft ↔ KVStore glue, apply loop |
| `internal/log/log.go` | Append-only log (provided) |
| `proto/raft.proto` | `RequestVote` / `AppendEntries` (provided) |
| `client/main.go` | `kvctl` CLI — identical to HW2, unmodified |

## Design decisions actually implemented

- **`becomeLeader` appends a no-op entry in the new term immediately on election.** This is what lets the leader commit *prior*-term entries safely: Raft's commit rule only counts a majority once an entry from the *current* term has also reached a majority, otherwise a later leader with a higher term but a shorter log could legally overwrite "committed" data (the Figure 8 scenario — see REFLECTIONS.md Q3 for a full 4-step trace).
- **`applyLoop` is driven by `sync.Cond`, not a polling sleep.** An earlier version slept 10ms between checks of `commitIndex`; this raced with tests calling `Get` immediately after detecting a new leader, before the apply loop had caught up. Signaling the condition variable whenever `commitIndex` advances applies committed entries within microseconds instead of on the next timer tick.
- **`AppendEntries` heartbeats always carry `LeaderCommit`**, even when empty. Omitting it from heartbeat-only sends (vs. sends carrying new entries) silently stalled followers' `commitIndex` even though the leader's own state was advancing correctly — passing per-node but not cluster-wide tests was the symptom.
- **Election timeout is randomized** per node within `[ElectionTimeoutMin, ElectionTimeoutMax]`, strictly above `HeartbeatInterval`. A single fixed timeout shared by all nodes would let every follower start an election in the same round, splitting votes with no majority — a liveness failure, not a safety one.
- **Reads are served from the local store without going through Raft**, so they are linearizable from the leader (the apply loop writes to the store, *then* signals the waiting RPC) but not from followers, and a partitioned leader that hasn't yet stepped down can still serve a stale read. The read-index protocol (§6.4) would fix this at the cost of one extra round-trip per read — not implemented here.

## Common bugs this implementation had to avoid

1. Election livelock from non-randomized timeouts.
2. Holding `mu` across an RPC call — the remote handler needs the same lock, so this deadlocks.
3. Off-by-one in `nextIndex` (`lastIndex + 1`, not `lastIndex`).
4. Not stepping down to follower immediately on seeing a higher term, even inside `RequestVote`.
5. Two goroutines racing to write `commitCh` / advance `lastApplied`.

## Stages & ports

| Stage | Scope | Points |
|-------|-------|--------|
| 1 | Leader election: RequestVote, election timer, heartbeats | 35 |
| 2 | Log replication: AppendEntries, commitIndex, applyLoop, Put | 40 |
| 3 | Safety: redirect on non-leader, partition recovery | 25 |

Ports (17000–17002 client, 17100–17102 peer) differ from HW2 to avoid collisions when running both locally.

See [REFLECTIONS.md](REFLECTIONS.md) for the full Figure-8 walkthrough, the HW2-vs-Raft lost-write comparison, and the linearizability analysis of leader vs. follower reads.
