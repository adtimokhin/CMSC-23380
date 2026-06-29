# HW1 — Connect-M

A networked Connect-4 variant with configurable board size and win condition (`M` in a row). The core game logic is a dependency-free Go package shared by a local two-player TUI, a TCP server, and a TCP client.

## Module layout

```
hw1/
├── game/     — package game: board state, move validation, win detection (no I/O)
├── tui/      — local two-player terminal game
├── server/   — TCP server hosting one game between two networked clients
└── client/   — CLI client: connects, claims a piece, sends moves
```

`game` is the only package the other three depend on. It is pure logic — no networking, no stdin/stdout — which is what makes it usable from the TUI, the server, and (potentially) an AI agent without modification.

### `game` API

| Symbol | Description |
|--------|-------------|
| `Board` | Game state: grid, dimensions, last move |
| `Player`, `Player1`, `Player2`, `Empty` | Cell values |
| `NewBoard(rows, cols, m)` | Create an empty board |
| `MakeMove(col, player)` | Drop a piece; returns landing row or error |
| `CheckWin()` | True if the *last* move won the game (checks only the 4 lines through it) |
| `IsFull()` / `IsColumnFull(col)` | Draw / full-column checks |
| `GetGrid()` | Copy of the current grid |
| `String()` | ASCII rendering |

Columns are 0-indexed inside `game`; the TUI, server, and client all convert from 1-indexed user input at the boundary.

## Running

**Local TUI** (two players, one terminal):
```bash
go run ./tui
```

**Networked play:**
```bash
go run ./server 8080 6 7 4           # port rows cols M
go run ./client X 4 localhost:8080   # player col server_addr — connects and immediately drops in column 4
go run ./client O 3 localhost:8080
```

The client's first invocation both connects *and* plays the opening move; there is no separate "connect" step from the user's perspective.

## Wire protocol

Client and server speak newline-delimited JSON over TCP — one `{"type": "...", "data": {...}}` object per line, in both directions. Full message catalogue (8 message types covering connect, move, win, draw, invalid-move, and disconnect/shutdown notifications) is in [wire_protocol.md](../wire_protocol.md) at the repo root.

JSON was chosen over a binary or ad-hoc text format because every message shares one envelope, so the server dispatches on a single `json.Unmarshal` into a `Message` struct rather than per-message parsing — and new fields (e.g. adding `board` to a notification) require no framing changes since both sides ignore unknown fields.

## Concurrency model

The server spawns one goroutine per accepted connection. All shared state (board, player registry, taken pieces, connections, turn, game-over flag) is protected by a single `sync.Mutex`. The key discipline enforced throughout the handlers: **state is read/written only while holding the lock, and the lock is released before any network I/O** — handlers snapshot the values they need into locals, unlock, then send. This avoids a slow write to one client blocking the other client's turn from being processed.

## Error handling

Invalid input is rejected at two layers: malformed CLI args (`<X|O>`, `<column>`) fail fast client-side before opening a connection; everything else (taken piece, wrong turn, full column, out-of-range column, game already over) is rejected server-side via `sc_ack_invalid` and reported to the user without dropping the connection, so a bad move doesn't end the session.

## Testing

```bash
go test ./game                              # all game-logic tests
go test ./game -run TestFunctionName        # a single test
```

See [REFLECTIONS.md](REFLECTIONS.md) for the full protocol design rationale and concurrency write-up.
