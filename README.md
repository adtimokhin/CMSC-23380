# CMSC 23380 — Foundations of Distributed Systems (Spring 2026)

Homework repository for CMSC 23380. Four assignments, each an independent Go module, building from a single networked game up to a Raft-replicated KV store and a distributed embedding pipeline. All four are complete, with design write-ups in each homework's `REFLECTIONS.md`.

## Assignments

| | Assignment | Core idea |
|---|------------|-----------|
| [hw1](hw1/) | Connect-M | TCP client/server game; goroutine-per-connection concurrency, a hand-designed JSON wire protocol |
| [hw2](hw2/) | Primary-backup KV store | gRPC replication, Lamport clocks, heartbeat failure detection, ad-hoc leader election — and where that election protocol breaks |
| [hw3](hw3/) | Raft KV store | Same client interface as HW2, but replication via real Raft — fixes the split-brain and lost-write cases HW2 couldn't |
| [hw4](hw4/) | Embedding pipeline | Broker/worker/producer pipeline with at-least-once delivery via idempotent re-execution, instead of a replicated log |

Each module is self-contained (own `go.mod`); run all commands from inside the relevant subdirectory.

## The arc

HW1 establishes the baseline: one server, no replication, just concurrent I/O over a hand-rolled protocol. HW2 introduces replication and failure detection, but its leader election is a Lamport-clock heuristic — REFLECTIONS.md for both HW2 and HW3 work through a concrete execution where this lets an election pick a backup that's missing the last acknowledged write. HW3 replaces that layer with Raft, which closes the gap via the election restriction (§5.4.1) and the current-term commit rule (Figure 8) — same client-facing `kvctl put/get/primary` interface, provably-safe backend. HW4 turns to a different point in the design space: tasks (chunk → vector) are idempotent and side-effect-free, so at-least-once delivery via re-execution is sufficient and a full consensus log would be overkill — REFLECTIONS.md makes that comparison explicit.

## Repo layout

```
hw1/   game/ tui/ server/ client/        — Connect-M
hw2/   internal/ config/ proto/ server/ client/   — primary-backup KV store
hw3/   internal/ raft/ proto/ server/ client/     — Raft KV store
hw4/   broker/ worker/ producer/ query/ tools/     — embedding pipeline
```

See [CLAUDE.md](CLAUDE.md) for per-assignment commands, ports, and architectural notes used when working in this repo with Claude Code.
