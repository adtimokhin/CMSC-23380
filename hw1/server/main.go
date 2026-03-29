package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"connectm/game"
)

// gameMu protects all shared game state below.
var gameMu sync.Mutex

// board is the single game instance, created when the first client connects.
var board *game.Board

// playerRegistry maps client id → assigned piece ("X" or "O").
var playerRegistry = map[string]string{}

// takenPieces tracks which pieces are currently claimed.
var takenPieces = map[string]bool{}

// playerConns maps piece ("X" or "O") → the connection of that player,
// used for broadcasting messages to both clients.
var playerConns = map[string]net.Conn{}

// firstPlayer is the piece of the client that connected first; they move first.
var firstPlayer = ""

// currentTurn holds which piece ("X" or "O") must move next.
// Set to firstPlayer once both clients have connected.
var currentTurn = ""

// gameOver is true after a win or draw; further moves are rejected.
var gameOver = false

// boardRows, boardCols, boardM are parsed from command-line arguments.
var boardRows, boardCols, boardM int

// Message is the common envelope for all wire protocol messages.
type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// sendMessage encodes msg as JSON and writes it to conn followed by a newline.
func sendMessage(conn net.Conn, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(conn, "%s\n", data)
	return err
}

// broadcast sends msg to the provided connections. Errors are printed but do
// not abort delivery to the other connection. The caller must collect the
// target connections before calling broadcast (do not hold gameMu when calling,
// as sendMessage performs I/O that must not block under the lock).
func broadcast(msg Message, conns []net.Conn) {
	for _, c := range conns {
		if c != nil {
			if err := sendMessage(c, msg); err != nil {
				fmt.Fprintln(os.Stderr, "broadcast error:", err)
			}
		}
	}
}

// sendAckConnect sends a sc_ack_connect message to a client, confirming whether
// the connection was accepted or rejected.
//
// Format:
//
//	{"type": "sc_ack_connect", "data": {"success": <bool>, "player": "<X|O|>", "reason": "<string>"}}
//
// On success, player is the piece assigned to this client; reason is empty.
// On failure, player is empty; reason is one of:
//   - "player X is already taken"
//   - "player O is already taken"
//   - "invalid player, must be X or O"
func sendAckConnect(conn net.Conn, success bool, player, reason string) error {
	data, err := json.Marshal(map[string]any{
		"success": success,
		"player":  player,
		"reason":  reason,
	})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_ack_connect", Data: data})
}

// sendNotifyStart sends a sc_notify_start message to a client when both players
// have connected and the game is ready to begin. Must be sent to both clients.
//
// Format:
//
//	{"type": "sc_notify_start", "data": {"opponent": "<X|O>", "first_turn": "<X|O>", "board": "<string>"}}
//
// opponent is the piece of the opposing player ("X" or "O").
// first_turn is whose turn it is next (the first player has already moved).
// board is the current board state after the first player's opening move.
func sendNotifyStart(conn net.Conn, opponent string, firstTurn string, boardStr string) error {
	data, err := json.Marshal(map[string]string{"opponent": opponent, "first_turn": firstTurn, "board": boardStr})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_notify_start", Data: data})
}

// sendAckMove sends a sc_ack_move message to both clients after a valid move is
// applied, containing the updated board state.
//
// Format:
//
//	{"type": "sc_ack_move", "data": {"status": "<OK|DRAW>", "next": "<X|O|>", "board": "<string>"}}
//
// status is "OK" for a normal move, or "DRAW" if the board is full with no winner.
// next is whose turn it is next; empty string if the game is over.
// board is the full board string as produced by game.go's Board.String() method.
func sendAckMove(conn net.Conn, status string, next string, boardStr string) error {
	data, err := json.Marshal(map[string]string{"status": status, "next": next, "board": boardStr})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_ack_move", Data: data})
}

// sendNotifyWin sends a sc_notify_win message to both clients when a winning
// move is made.
//
// Format:
//
//	{"type": "sc_notify_win", "data": {"winner": "<X|O>", "board": "<string>"}}
//
// winner is the piece that won ("X" or "O").
// board is the final board state as produced by game.go's Board.String() method.
func sendNotifyWin(conn net.Conn, winner string, boardStr string) error {
	data, err := json.Marshal(map[string]string{"winner": winner, "board": boardStr})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_notify_win", Data: data})
}

// sendAckInvalid sends a sc_ack_invalid message only to the client that
// submitted an invalid or out-of-turn move. The game state is unchanged.
//
// Format:
//
//	{"type": "sc_ack_invalid", "data": {"reason": "<string>"}}
//
// Possible reasons: "it is not your turn", "column is full",
// "column out of range", "game is already over".
func sendAckInvalid(conn net.Conn, reason string) error {
	data, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_ack_invalid", Data: data})
}

// sendNotifyError sends a sc_notify_error message to the remaining client when
// an unexpected event terminates the session.
//
// Format:
//
//	{"type": "sc_notify_error", "data": {"reason": "<string>"}}
//
// Possible reasons: "opponent disconnected", "server shutting down".
func sendNotifyError(conn net.Conn, reason string) error {
	data, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "sc_notify_error", Data: data})
}

// handleSendConnect handles a cs_send_connect message from a client.
// On the first connection the game board is created. On the second connection
// sc_notify_start is broadcast to both players.
func handleSendConnect(conn net.Conn, data json.RawMessage) {
	var payload struct {
		Player string `json:"player"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleSendConnect] failed to parse payload:", err)
		sendAckConnect(conn, false, "", "malformed connect message")
		return
	}

	if payload.Player != "X" && payload.Player != "O" {
		fmt.Printf("[handleSendConnect] invalid piece %q from id %s\n", payload.Player, payload.ID)
		sendAckConnect(conn, false, "", "invalid player, must be X or O")
		return
	}

	gameMu.Lock()

	if takenPieces[payload.Player] {
		gameMu.Unlock()
		fmt.Printf("[handleSendConnect] piece %s already taken, rejecting id %s\n", payload.Player, payload.ID)
		sendAckConnect(conn, false, "", "player "+payload.Player+" is already taken")
		return
	}

	// Register the player.
	playerRegistry[payload.ID] = payload.Player
	takenPieces[payload.Player] = true
	playerConns[payload.Player] = conn

	// Create the board and record the first player on first connection.
	// currentTurn is set immediately so the first player can move before the
	// second player connects.
	if board == nil {
		board = game.NewBoard(boardRows, boardCols, boardM)
		firstPlayer = payload.Player
		currentTurn = payload.Player
		fmt.Printf("[handleSendConnect] game created (%dx%d, M=%d), first player: %s\n", boardRows, boardCols, boardM, firstPlayer)
	}

	bothConnected := takenPieces["X"] && takenPieces["O"]
	connX := playerConns["X"]
	connO := playerConns["O"]
	var boardStr, nextTurn string
	if bothConnected {
		boardStr = board.String()
		nextTurn = currentTurn
	}
	gameMu.Unlock()

	fmt.Printf("[handleSendConnect] registered id %s as player %s\n", payload.ID, payload.Player)
	sendAckConnect(conn, true, payload.Player, "")

	// If both players are now connected, send sc_notify_start to each.
	// The board already reflects the first player's opening move.
	if bothConnected {
		fmt.Println("[handleSendConnect] both players connected, broadcasting start")
		sendNotifyStart(connX, "O", nextTurn, boardStr)
		sendNotifyStart(connO, "X", nextTurn, boardStr)
	}
}

// pieceToPlayer converts a string piece to a game.Player value.
func pieceToPlayer(piece string) game.Player {
	if piece == "X" {
		return game.Player1
	}
	return game.Player2
}

// playerToPiece converts a game.Player value back to its string piece.
func playerToPiece(p game.Player) string {
	if p == game.Player1 {
		return "X"
	}
	return "O"
}

// handleSendMove handles a cs_send_move message from a client.
// Validates turn order, applies the move, then broadcasts sc_ack_move or
// sc_notify_win to both clients.
func handleSendMove(conn net.Conn, data json.RawMessage) {
	var payload struct {
		ID     string `json:"id"`
		Column int    `json:"column"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleSendMove] failed to parse payload:", err)
		return
	}

	gameMu.Lock()

	piece, ok := playerRegistry[payload.ID]
	if !ok {
		gameMu.Unlock()
		fmt.Printf("[handleSendMove] unknown client id %s\n", payload.ID)
		sendAckInvalid(conn, "client not registered")
		return
	}

	if gameOver {
		gameMu.Unlock()
		sendAckInvalid(conn, "game is already over")
		return
	}

	if piece != currentTurn {
		gameMu.Unlock()
		sendAckInvalid(conn, "it is not your turn")
		return
	}

	// Convert 1-indexed column from client to 0-indexed for game package.
	col0 := payload.Column - 1
	_, err := board.MakeMove(col0, pieceToPlayer(piece))
	if err != nil {
		gameMu.Unlock()
		// Translate game-level errors to protocol reasons.
		reason := err.Error()
		if reason == "column out of bounds" {
			reason = "column out of range"
		}
		sendAckInvalid(conn, reason)
		return
	}

	fmt.Printf("[handleSendMove] player %s played column %d\n", piece, payload.Column)

	boardStr := board.String()
	allConns := []net.Conn{playerConns["X"], playerConns["O"]}

	if board.CheckWin() {
		gameOver = true
		gameMu.Unlock()
		winData, _ := json.Marshal(map[string]string{"winner": piece, "board": boardStr})
		broadcast(Message{Type: "sc_notify_win", Data: winData}, allConns)
		return
	}

	if board.IsFull() {
		gameOver = true
		gameMu.Unlock()
		ackData, _ := json.Marshal(map[string]string{"status": "DRAW", "next": "", "board": boardStr})
		broadcast(Message{Type: "sc_ack_move", Data: ackData}, allConns)
		return
	}

	// Advance turn.
	if currentTurn == "X" {
		currentTurn = "O"
	} else {
		currentTurn = "X"
	}
	next := currentTurn
	gameMu.Unlock()

	ackData, _ := json.Marshal(map[string]string{"status": "OK", "next": next, "board": boardStr})
	broadcast(Message{Type: "sc_ack_move", Data: ackData}, allConns)
}

// handleMessages reads newline-delimited JSON messages from conn and dispatches
// each to the appropriate handler based on the "type" field.
func handleMessages(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			fmt.Fprintln(os.Stderr, "failed to parse message:", err)
			continue
		}
		switch msg.Type {
		case "cs_send_connect":
			handleSendConnect(conn, msg.Data)
		case "cs_send_move":
			handleSendMove(conn, msg.Data)
		default:
			fmt.Fprintln(os.Stderr, "unknown message type:", msg.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "read error:", err)
	}
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: server <port> <rows> <cols> <M>")
		os.Exit(1)
	}

	var err error
	boardRows, err = strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid rows:", os.Args[2])
		os.Exit(1)
	}
	boardCols, err = strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid cols:", os.Args[3])
		os.Exit(1)
	}
	boardM, err = strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid M:", os.Args[4])
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Printf("Listening on port %s (board %dx%d, M=%d)\n", os.Args[1], boardRows, boardCols, boardM)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		fmt.Println("User Connected")
		go func(c net.Conn) {
			defer c.Close()
			handleMessages(c)
		}(conn)
	}
}
