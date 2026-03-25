package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"connectm/game"
)

// Game manages the overall game flow
type Game struct {
	Board         *game.Board
	CurrentPlayer game.Player
}

// NewGame creates a new game instance
func NewGame(rows, cols, m int) *Game {
	return &Game{
		Board:         game.NewBoard(rows, cols, m),
		CurrentPlayer: game.Player1,
	}
}

// SwitchPlayer switches to the other player
func (g *Game) SwitchPlayer() {
	if g.CurrentPlayer == game.Player1 {
		g.CurrentPlayer = game.Player2
	} else {
		g.CurrentPlayer = game.Player1
	}
}

// GetPlayerSymbol returns the symbol for a player
func GetPlayerSymbol(player game.Player) string {
	if player == game.Player1 {
		return "X"
	}
	return "O"
}

// GetPlayerName returns the name for a player
func GetPlayerName(player game.Player) string {
	if player == game.Player1 {
		return "Player 1"
	}
	return "Player 2"
}

// RenderBoard displays the current board state
func RenderBoard(board *game.Board) {
	fmt.Print("\033[H\033[2J") // Clear screen
	fmt.Println("\n=== Connect-M ===")
	fmt.Printf("(Connect %d to win)\n\n", board.M)
	fmt.Print(board.String())
}

// GetPlayerInput prompts for and validates player input
func GetPlayerInput(board *game.Board, player game.Player) int {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("\n%s (%s), choose column (1-%d) or 'q' to quit: ",
			GetPlayerName(player),
			GetPlayerSymbol(player),
			board.Cols)

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input. Please try again.")
			continue
		}

		input = strings.TrimSpace(input)

		// Check for quit
		if strings.ToLower(input) == "q" {
			return -1
		}

		// Parse column number
		col, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Invalid input. Please enter a number.")
			continue
		}

		// Convert to 0-indexed
		col--

		// Validate column
		if col < 0 || col >= board.Cols {
			fmt.Printf("Column must be between 1 and %d.\n", board.Cols)
			continue
		}

		if board.IsColumnFull(col) {
			fmt.Println("That column is full. Choose another.")
			continue
		}

		return col
	}
}

// PlayGame runs the main game loop with TUI
func PlayGame(rows, cols, m int) {
	g := NewGame(rows, cols, m)

	for {
		RenderBoard(g.Board)

		// Get player input
		col := GetPlayerInput(g.Board, g.CurrentPlayer)

		// Check for quit
		if col == -1 {
			fmt.Println("\nGame ended. Thanks for playing!")
			return
		}

		// Make the move
		_, err := g.Board.MakeMove(col, g.CurrentPlayer)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			fmt.Println("Press Enter to continue...")
			bufio.NewReader(os.Stdin).ReadString('\n')
			continue
		}

		// Check for win
		if g.Board.CheckWin() {
			RenderBoard(g.Board)
			fmt.Printf("\n🎉 %s (%s) wins!\n\n",
				GetPlayerName(g.CurrentPlayer),
				GetPlayerSymbol(g.CurrentPlayer))
			return
		}

		// Check for draw
		if g.Board.IsFull() {
			RenderBoard(g.Board)
			fmt.Println("\n🤝 It's a draw!")
			return
		}

		// Switch players
		g.SwitchPlayer()
	}
}

func main() {
	fmt.Println("=== Connect-M Game ===")
	fmt.Println()

	// Get game configuration
	reader := bufio.NewReader(os.Stdin)

	// Get number of rows
	var rows int
	for {
		fmt.Print("Enter number of rows (default 6): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			rows = 6
			break
		}
		var err error
		rows, err = strconv.Atoi(input)
		if err != nil || rows < 4 {
			fmt.Println("Please enter a valid number (minimum 4).")
			continue
		}
		break
	}

	// Get number of columns
	var cols int
	for {
		fmt.Print("Enter number of columns (default 7): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			cols = 7
			break
		}
		var err error
		cols, err = strconv.Atoi(input)
		if err != nil || cols < 4 {
			fmt.Println("Please enter a valid number (minimum 4).")
			continue
		}
		break
	}

	// Get M (number of pieces to connect)
	var m int
	for {
		fmt.Print("Enter pieces to connect to win (default 4): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			m = 4
			break
		}
		var err error
		m, err = strconv.Atoi(input)
		if err != nil || m < 3 || m > rows || m > cols {
			fmt.Printf("Please enter a valid number (3 to %d).\n", min(rows, cols))
			continue
		}
		break
	}

	fmt.Println("\nStarting game...")
	fmt.Println("Press Enter to continue...")
	reader.ReadString('\n')

	PlayGame(rows, cols, m)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
