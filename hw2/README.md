# HW2: Primary-Backup Replicated Key-Value Store

## Quick Start

```bash
# Run unit tests for provided code (must pass before you start)
go test ./internal/... -v

# Build everything (stubs compile cleanly out of the box)
go build ./...
```

## Running the Cluster

Open three terminals. Each runs one node of the 3-node cluster.

```bash
# Terminal 1 — node 0 (starts as primary, client port 7000, peer port 7100)
go run ./server --id=0 --config=nodeconfig.json

# Terminal 2 — node 1 (backup, client port 7001, peer port 7101)
go run ./server --id=1 --config=nodeconfig.json

# Terminal 3 — node 2 (backup, client port 7002, peer port 7102)
go run ./server --id=2 --config=nodeconfig.json
```

## Using the Client

```bash
# Write a key-value pair to the primary (node 0)
go run ./client put hello world localhost:7000

# Read from any node
go run ./client get hello localhost:7000   # primary
go run ./client get hello localhost:7001   # backup 1 (may be stale)
go run ./client get hello localhost:7002   # backup 2 (may be stale)

# Discover the current primary
go run ./client primary localhost:7001
```

## Files

| File | Status | Description |
|------|--------|-------------|
| `internal/clock/clock.go` | PROVIDED | Lamport clock — do not modify |
| `internal/store/store.go` | PROVIDED | In-memory KV store — do not modify |
| `config/config.go` | PROVIDED | Cluster config loader — do not modify |
| `proto/kvstore.proto` | PROVIDED | gRPC service definitions — do not modify |
| `proto/*.pb.go` | PROVIDED | Generated — do not modify |
| `nodeconfig.json` | PROVIDED | 3-node cluster config — do not modify |
| **`server/main.go`** | **YOU IMPLEMENT** | Node logic: Put, Replicate, Heartbeat, election |
| **`client/main.go`** | **YOU IMPLEMENT** | CLI client: put, get, primary, redirect |
| `REFLECTIONS.md` | YOU FILL IN | Design decisions and reflection questions |

## Stages

Build the assignment incrementally — each stage has concrete verification tests in the assignment writeup.

| Stage | What you implement | Points |
|-------|--------------------|--------|
| 1 | Single-node Put/Get/GetPrimary + Lamport timestamps | 30 |
| 2 | Replicate fan-out to both backups before ACK | 25 |
| 3 | Heartbeat sender + failure detection | 15 |
| 4 | Leader election on primary crash | 20 |
| 5 | Client redirect handling | 10 |
| Reflection + code quality | Design decisions + theory connections | 15 |

## Ports

| Node | Client port | Peer port |
|------|-------------|-----------|
| 0    | 7000        | 7100      |
| 1    | 7001        | 7101      |
| 2    | 7002        | 7102      |

## Regenerating Proto Files

The `.pb.go` files are pre-generated and checked in. If you need to regenerate them:

```bash
make proto
```

This requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`. See the [gRPC Go quickstart](https://grpc.io/docs/languages/go/quickstart/).
