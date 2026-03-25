# Connect-M

A flexible Connect-M game implementation in Go, supporting variable board sizes and win conditions.

## Module structure

```
soln/
├── go.mod              # module connectm
├── game/               # package game — importable core logic, no I/O
│   ├── game.go
│   └── game_test.go
├── tui/                # local two-player terminal game
├── server/             # TCP game server
└── client/             # one-shot move client
```

## Running

**Local TUI** (two players, one terminal):
```bash
go run ./tui
```
Prompts for board dimensions and M, then runs an interactive game.

**Networked** (to be completed by students):
```bash
go run ./server 8080 6 7 4           # start server on :8080, 6x7 board with 4 connected pieces to win
go run ./client X 4 localhost:8080   # Player X drops in column 4
go run ./client O 3 localhost:8080   #Player O drops in column 3
```

## game package API

`package game` contains all game logic with no I/O dependencies, making it straightforward to import in any client, server, or AI implementation:

```go
import "connectm/game"

board := game.NewBoard(6, 7, 4)       // rows, cols, M

row, err := board.MakeMove(col, game.Player1)  // col is 0-indexed within the API
if board.CheckWin()  { /* Player1 won */ }
if board.IsFull()    { /* draw */ }

fmt.Print(board.String())             // ASCII rendering
```

Key types and functions:

| Symbol | Description |
|--------|-------------|
| `Board` | Game state (grid, dimensions, last move) |
| `Player` / `Player1` / `Player2` / `Empty` | Cell values |
| `Move` | Records col, row, and player of the last move |
| `NewBoard(rows, cols, m)` | Create an empty board |
| `MakeMove(col, player)` | Drop a piece; returns landing row or error |
| `CheckWin()` | True if the last move won the game |
| `IsFull()` | True if no empty cells remain |
| `IsColumnFull(col)` | True if the column cannot accept another piece |
| `GetGrid()` | Returns a copy of the current grid |
| `String()` | ASCII rendering of the board |
