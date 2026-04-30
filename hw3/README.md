# HW3: Raft-Based Key-Value Store

HW2 had two correctness problems. HW3 fixes both:

| Problem in HW2 | Fix in HW3 |
|----------------|------------|
| Split-brain: two backups can both think they're primary (no term numbers, no quorum) | Raft's term numbers + majority quorum prevent split-brain |
| Lost writes: write ACKed to client can disappear on leader crash (no log reconciliation) | Raft's log replication ensures committed entries survive |

The client interface (`kvctl put/get/primary`) is **identical to HW2** — only the backend changes.

## Quick Start

```bash
# Run unit tests for provided code — must pass before you start
go test ./internal/... -v

# Build everything (stubs compile cleanly)
go build ./...
```

## Running the Cluster

### Option A — Docker (recommended)

```bash
make docker-up          # build image and start all 3 nodes in the background
make docker-logs        # stream logs from all nodes
make docker-down        # stop and remove containers
```

Wait ~300ms for leader election, then use the client against the exposed ports:

```bash
go run ./client primary localhost:17000  # discover who won the election
go run ./client put foo bar localhost:17000
go run ./client get foo localhost:17001
```

To simulate a network partition and recovery:

```bash
make docker-partition NODE=node0   # isolate node0 from the cluster
make docker-heal      NODE=node0   # reconnect node0
```

### Option B — Local processes

```bash
# Terminal 1 — node 0
go run ./server --id=0 --config=nodeconfig.json

# Terminal 2 — node 1
go run ./server --id=1 --config=nodeconfig.json

# Terminal 3 — node 2
go run ./server --id=2 --config=nodeconfig.json
```

Wait ~300ms for leader election, then:

```bash
go run ./client primary localhost:17000  # discover who won the election
go run ./client put foo bar localhost:17000
go run ./client get foo localhost:17001
```

## Files

| File | Status | Description |
|------|--------|-------------|
| `internal/clock/` | PROVIDED | Lamport clock (same as HW2) |
| `internal/store/` | PROVIDED | KV store (same as HW2) |
| `internal/log/log.go` | PROVIDED | Raft append-only log — do not modify |
| `config/` | PROVIDED | Cluster config loader |
| `proto/kvstore.proto` | PROVIDED | Client gRPC service (same as HW2) |
| `proto/raft.proto` | PROVIDED | RequestVote + AppendEntries definitions |
| `proto/*.pb.go` | PROVIDED | Generated — do not modify |
| `nodeconfig.json` | PROVIDED | 3-node config (see Ports table below) |
| **`raft/raft.go`** | **YOU IMPLEMENT** | Raft state machine |
| **`server/main.go`** | **YOU IMPLEMENT** | Raft ↔ KVStore integration |
| `client/main.go` | PROVIDED | kvctl CLI — no changes needed |
| `REFLECTIONS.md` | YOU FILL IN | Design decisions + theory questions |

## Stages

| Stage | What you implement | Points |
|-------|--------------------|--------|
| 1 | Leader election: RequestVote, election timer, heartbeats | 35 |
| 2 | Log replication: AppendEntries, commitIndex, applyLoop, Put handler | 40 |
| 3 | Safety: redirect on non-leader, partition recovery | 25 |
| Reflection + code quality | | 15 |
| **Extension** | Log compaction + InstallSnapshot | +20 |

## Common Bugs

1. **Election livelock:** All nodes time out at the same instant every round → no leader. Fix: randomize election timeout.
2. **Holding `mu` during RPCs:** The RPC handler on the other side needs `mu` too → deadlock. Always release the lock before calling any RPC.
3. **Off-by-one in nextIndex:** `nextIndex` starts at `lastIndex+1`, not `lastIndex`. Double-check with the Figure 2 definition.
4. **Not stepping down:** When any RPC arrives with a higher term, you must immediately update `currentTerm` and become a follower — even in `RequestVote`.
5. **Apply loop race:** Only one goroutine should write to `commitCh` and update `lastApplied`.

## Ports

Ports differ from HW2 to avoid conflicts when running via Docker.

| Node | Client port | Peer port |
|------|-------------|-----------|
| 0    | 17000       | 17100     |
| 1    | 17001       | 17101     |
| 2    | 17002       | 17102     |
