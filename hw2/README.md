# HW2 — Primary-Backup Replicated Key-Value Store

A 3-node primary-backup KV store over gRPC, with synchronous replication, heartbeat-based failure detection, and Lamport-clock-ordered leader election on primary crash. All 5 stages plus the reflection are complete.

## Quick start

```bash
go test ./internal/... -v          # provided-package tests
go build ./...

# 3 terminals — node 0 starts as primary
go run ./server --id=0 --config=nodeconfig.json   # client :7000, peer :7100
go run ./server --id=1 --config=nodeconfig.json   # client :7001, peer :7101
go run ./server --id=2 --config=nodeconfig.json   # client :7002, peer :7102

go run ./client put hello world localhost:7000
go run ./client get hello localhost:7001   # reads are served locally, may be stale
go run ./client primary localhost:7001     # discover current primary
```

## Architecture

Each `Node` runs **two gRPC servers** behind one `sync.Mutex` guarding all shared state: a client-facing `KVStore` service (`Put`, `Get`, `GetPrimary`) and a peer-facing `Replication` service (`Replicate`, `Heartbeat`, `AnnounceLeader`). `internal/clock` (Lamport clock), `internal/store` (in-memory KV), and `config` (cluster topology loader) are provided and unmodified; `server/main.go` and `client/main.go` are the implementation.

| Layer | Files |
|-------|-------|
| Client-facing RPCs | `server/main.go` — `Put`, `Get`, `GetPrimary` |
| Peer RPCs | `server/main.go` — `Replicate`, `Heartbeat`, `AnnounceLeader` |
| Background loops | `startHeartbeatLoop`, `monitorPeers` |
| CLI | `client/main.go` — `put` / `get` / `primary`, with redirect-on-wrong-node handling |

## Design decisions actually implemented

- **Replication fan-out is parallel**, not sequential: `Put` spawns one goroutine per backup behind a `sync.WaitGroup`, so latency is bounded by `max(RTT)` across backups rather than their sum. The primary still waits for *both* ACKs before responding to the client — this is what makes the write durable on 2-of-3 nodes, at the cost of the slowest backup setting the floor on write latency.
- **Lamport clock tick happens before replication and before the local store write** — ticking after would let a replicated message carry a timestamp the receiver's clock has already passed, violating causal ordering.
- **Heartbeat interval 500 ms / timeout 1000 ms** (2× interval): tolerates one dropped heartbeat without a false-positive failure suspicion, while bounding detection latency to ~1.5 s. The `Replicate` RPC timeout is a fixed 1000 ms; the `Heartbeat` RPC timeout is `HeartbeatInterval × 4/5` rather than a fixed value, so it stays valid even under the much faster timing constants used by the test harness.
- **Election tiebreak**: a backup that suspects the primary ticks its clock to capture a stable `electionTs` snapshot under the lock, then broadcasts `AnnounceLeader`. The receiver accepts the new leader if `electionTs` is strictly higher, or — on a tie — if the proposer's node ID is higher. Using a snapshot (rather than re-reading the live clock) avoids a race where concurrent heartbeats move the comparison target mid-election.
- **Consistency**: `Get` is served from whichever node receives the request, with no read redirect. This gives sequential — not linearizable — consistency: a `Get` issued to a backup mid-replication can return a stale value even though the primary has already started (but not finished) acknowledging the write. See REFLECTIONS.md Q3 for the execution trace.

## Known limitation (motivates HW3)

The election protocol is a heuristic tiebreak, not a correctness proof: nothing prevents a backup that *missed* the last replicated write from winning an election, and a primary that misses heartbeats during a GC pause but is still alive may keep accepting writes after losing the role. Both are fixed in HW3 by replacing this layer with Raft.

## Stages & ports

| Stage | Scope | Points |
|-------|-------|--------|
| 1 | Single-node Put/Get/GetPrimary + Lamport timestamps | 30 |
| 2 | Replicate fan-out to both backups before ACK | 25 |
| 3 | Heartbeat sender + failure detection | 15 |
| 4 | Leader election on primary crash | 20 |
| 5 | Client redirect handling | 10 |

| Node | Client port | Peer port |
|------|-------------|-----------|
| 0 | 7000 | 7100 |
| 1 | 7001 | 7101 |
| 2 | 7002 | 7102 |

## Regenerating proto files

```bash
make proto   # requires protoc, protoc-gen-go, protoc-gen-go-grpc
```

`.pb.go` files are checked in and don't need to be regenerated for normal use.

See [REFLECTIONS.md](REFLECTIONS.md) for the full design write-up, including the Lamport-clock and simultaneous-election walkthroughs, and [requirements/STAGES.md](requirements/STAGES.md) for the original stage specs.
