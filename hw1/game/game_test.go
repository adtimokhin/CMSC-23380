package game

import (
	"fmt"
	"testing"
)

// Example of how to use the game logic programmatically
// This demonstrates how you would use it in a client/server implementation

func TestBasicGame(t *testing.T) {
	// Create a new board
	board := NewBoard(6, 7, 4)

	// Player 1 makes a move in column 3
	row, err := board.MakeMove(3, Player1)
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}
	if row != 5 {
		t.Errorf("Expected piece to land in row 5, got %d", row)
	}

	// Player 2 makes a move in column 3
	row, err = board.MakeMove(3, Player2)
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}
	if row != 4 {
		t.Errorf("Expected piece to land in row 4, got %d", row)
	}

	// Verify no win yet
	if board.CheckWin() {
		t.Error("Should not have a winner yet")
	}
}

func TestWinConditionHorizontal(t *testing.T) {
	board := NewBoard(6, 7, 4)

	// Create a horizontal win for Player 1
	for col := 0; col < 4; col++ {
		board.MakeMove(col, Player1)
	}

	if !board.CheckWin() {
		t.Error("Should detect horizontal win")
	}

	if board.LastMove.Player != Player1 {
		t.Error("Player 1 should be the winner")
	}
}

func TestWinConditionVertical(t *testing.T) {
	board := NewBoard(6, 7, 4)

	// Create a vertical win for Player 2
	for i := 0; i < 4; i++ {
		board.MakeMove(3, Player2)
	}

	if !board.CheckWin() {
		t.Error("Should detect vertical win")
	}
}

func TestWinConditionDiagonal(t *testing.T) {
	board := NewBoard(6, 7, 4)

	// Create a diagonal win (down-right)
	// Row 5, Col 0
	board.MakeMove(0, Player1)

	// Row 5, Col 1; Row 4, Col 1
	board.MakeMove(1, Player2)
	board.MakeMove(1, Player1)

	// Row 5, Col 2; Row 4, Col 2; Row 3, Col 2
	board.MakeMove(2, Player2)
	board.MakeMove(2, Player2)
	board.MakeMove(2, Player1)

	// Row 5, Col 3; Row 4, Col 3; Row 3, Col 3; Row 2, Col 3
	board.MakeMove(3, Player2)
	board.MakeMove(3, Player2)
	board.MakeMove(3, Player2)
	board.MakeMove(3, Player1)

	if !board.CheckWin() {
		t.Error("Should detect diagonal win")
	}
}

func TestInvalidMoves(t *testing.T) {
	board := NewBoard(6, 7, 4)

	// Test out of bounds
	_, err := board.MakeMove(-1, Player1)
	if err == nil {
		t.Error("Should reject negative column")
	}

	_, err = board.MakeMove(7, Player1)
	if err == nil {
		t.Error("Should reject column >= cols")
	}

	// Test invalid player
	_, err = board.MakeMove(0, Player(99))
	if err == nil {
		t.Error("Should reject invalid player")
	}
}

func TestFullColumn(t *testing.T) {
	board := NewBoard(6, 7, 4)

	// Fill column 0
	for i := 0; i < 6; i++ {
		_, err := board.MakeMove(0, Player1)
		if err != nil {
			t.Fatalf("Move %d failed: %v", i, err)
		}
	}

	// Try to add another piece
	_, err := board.MakeMove(0, Player1)
	if err == nil {
		t.Error("Should reject move in full column")
	}

	if !board.IsColumnFull(0) {
		t.Error("Column 0 should be marked as full")
	}
}

func TestBoardFull(t *testing.T) {
	board := NewBoard(3, 3, 4) // Small board

	if board.IsFull() {
		t.Error("Empty board should not be full")
	}

	// Fill the board
	for col := 0; col < 3; col++ {
		for row := 0; row < 3; row++ {
			board.MakeMove(col, Player1)
		}
	}

	if !board.IsFull() {
		t.Error("Completely filled board should report as full")
	}
}

// Example: Simulating a client/server move exchange
func Example() {
	board := NewBoard(6, 7, 4)

	// Server receives move from client 1
	clientMove := 3
	playerID := Player1

	// Server validates and applies move
	row, err := board.MakeMove(clientMove, playerID)
	if err != nil {
		fmt.Printf("Server: Invalid move - %v\n", err)
		return
	}

	fmt.Printf("Server: Player %d dropped piece in column %d, landed at row %d\n",
		playerID, clientMove, row)

	// Server checks win condition
	if board.CheckWin() {
		fmt.Printf("Server: Player %d wins!\n", playerID)
		return
	}

	// Server would broadcast updated board state to all clients
	// Clients can use board.String() for display or board.GetGrid() for custom rendering
	fmt.Println("\nCurrent board state:")
	fmt.Print(board.String())

	// Output:
	// Server: Player 1 dropped piece in column 3, landed at row 5
	//
	// Current board state:
	//   1 2 3 4 5 6 7
	//  +--------------+
	//  |. . . . . . . |
	//  |. . . . . . . |
	//  |. . . . . . . |
	//  |. . . . . . . |
	//  |. . . . . . . |
	//  |. . . X . . . |
	//  +--------------+
}
