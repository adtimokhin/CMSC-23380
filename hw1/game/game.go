package game

import (
	"errors"
	"fmt"
)

// Player represents a player in the game
type Player int

const (
	Empty   Player = 0
	Player1 Player = 1
	Player2 Player = 2
)

// Board represents the game state
type Board struct {
	Rows     int
	Cols     int
	M        int // Number of pieces to connect to win
	Grid     [][]Player
	LastMove *Move // Track last move for efficient win checking
}

// Move represents a single move in the game
type Move struct {
	Col    int
	Row    int
	Player Player
}

// NewBoard creates a new game board
func NewBoard(rows, cols, m int) *Board {
	grid := make([][]Player, rows)
	for i := range grid {
		grid[i] = make([]Player, cols)
	}
	return &Board{
		Rows: rows,
		Cols: cols,
		M:    m,
		Grid: grid,
	}
}

// MakeMove drops a piece in the specified column for the given player
// Returns the row where the piece landed, or an error if the move is invalid
func (b *Board) MakeMove(col int, player Player) (int, error) {
	// Validate column
	if col < 0 || col >= b.Cols {
		return -1, errors.New("column out of bounds")
	}

	// Validate player
	if player != Player1 && player != Player2 {
		return -1, errors.New("invalid player")
	}

	// Find the lowest empty row in the column
	for row := b.Rows - 1; row >= 0; row-- {
		if b.Grid[row][col] == Empty {
			b.Grid[row][col] = player
			b.LastMove = &Move{
				Col:    col,
				Row:    row,
				Player: player,
			}
			return row, nil
		}
	}

	return -1, errors.New("column is full")
}

// CheckWin checks if the last move resulted in a win
func (b *Board) CheckWin() bool {
	if b.LastMove == nil {
		return false
	}

	row := b.LastMove.Row
	col := b.LastMove.Col
	player := b.LastMove.Player

	// Check horizontal
	if b.checkDirection(row, col, player, 0, 1) {
		return true
	}

	// Check vertical
	if b.checkDirection(row, col, player, 1, 0) {
		return true
	}

	// Check diagonal (down-right)
	if b.checkDirection(row, col, player, 1, 1) {
		return true
	}

	// Check diagonal (down-left)
	if b.checkDirection(row, col, player, 1, -1) {
		return true
	}

	return false
}

// checkDirection checks if there are M consecutive pieces in a given direction
func (b *Board) checkDirection(row, col int, player Player, dRow, dCol int) bool {
	count := 1 // Count the piece that was just placed

	// Check in the positive direction
	for i := 1; i < b.M; i++ {
		r := row + i*dRow
		c := col + i*dCol
		if !b.isValid(r, c) || b.Grid[r][c] != player {
			break
		}
		count++
	}

	// Check in the negative direction
	for i := 1; i < b.M; i++ {
		r := row - i*dRow
		c := col - i*dCol
		if !b.isValid(r, c) || b.Grid[r][c] != player {
			break
		}
		count++
	}

	return count >= b.M
}

// isValid checks if a position is within the board bounds
func (b *Board) isValid(row, col int) bool {
	return row >= 0 && row < b.Rows && col >= 0 && col < b.Cols
}

// IsFull checks if the board is completely full
func (b *Board) IsFull() bool {
	for col := 0; col < b.Cols; col++ {
		if b.Grid[0][col] == Empty {
			return false
		}
	}
	return true
}

// IsColumnFull checks if a specific column is full
func (b *Board) IsColumnFull(col int) bool {
	if col < 0 || col >= b.Cols {
		return true
	}
	return b.Grid[0][col] != Empty
}

// GetGrid returns a copy of the current grid state
func (b *Board) GetGrid() [][]Player {
	grid := make([][]Player, b.Rows)
	for i := range grid {
		grid[i] = make([]Player, b.Cols)
		copy(grid[i], b.Grid[i])
	}
	return grid
}

// String returns a string representation of the board
func (b *Board) String() string {
	var result string

	// Column numbers
	result += "  "
	for col := 0; col < b.Cols; col++ {
		result += fmt.Sprintf("%d", col+1)
		if col < b.Cols-1 {
			result += " "
		}
	}
	result += "\n"

	// Top border
	result += " +"
	for col := 0; col < b.Cols; col++ {
		result += "--"
	}
	result += "+\n"

	// Board rows
	for row := 0; row < b.Rows; row++ {
		result += " |"
		for col := 0; col < b.Cols; col++ {
			switch b.Grid[row][col] {
			case Empty:
				result += ". "
			case Player1:
				result += "X "
			case Player2:
				result += "O "
			}
		}
		result += "|\n"
	}

	// Bottom border
	result += " +"
	for col := 0; col < b.Cols; col++ {
		result += "--"
	}
	result += "+\n"

	return result
}
