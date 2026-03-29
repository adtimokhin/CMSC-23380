package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

// playerRegistry maps client id → assigned piece ("X" or "O").
var playerRegistry = map[string]string{}

// takenPieces tracks which pieces are currently claimed.
var takenPieces = map[string]bool{}

// registryMu protects playerRegistry and takenPieces from concurrent access.
var registryMu sync.Mutex

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
	data, err := json.Marshal(map[string]interface{}{
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
//	{"type": "sc_notify_start", "data": {"opponent": "<X|O>", "first_turn": "X"}}
//
// opponent is the piece of the opposing player ("X" or "O").
// first_turn is always "X" per Connect-M rules.
func sendNotifyStart(conn net.Conn, opponent string, firstTurn string) error {
	data, err := json.Marshal(map[string]string{"opponent": opponent, "first_turn": firstTurn})
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
func sendAckMove(conn net.Conn, status string, next string, board string) error {
	data, err := json.Marshal(map[string]string{"status": status, "next": next, "board": board})
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
func sendNotifyWin(conn net.Conn, winner string, board string) error {
	data, err := json.Marshal(map[string]string{"winner": winner, "board": board})
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
// Parses the requested piece and client id, checks whether the piece is
// available, and registers the id → piece mapping on success.
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

	registryMu.Lock()
	defer registryMu.Unlock()

	if takenPieces[payload.Player] {
		fmt.Printf("[handleSendConnect] piece %s already taken, rejecting id %s\n", payload.Player, payload.ID)
		sendAckConnect(conn, false, "", "player "+payload.Player+" is already taken")
		return
	}

	playerRegistry[payload.ID] = payload.Player
	takenPieces[payload.Player] = true
	fmt.Printf("[handleSendConnect] registered id %s as player %s\n", payload.ID, payload.Player)
	sendAckConnect(conn, true, payload.Player, "")
}

// handleSendMove handles a cs_send_move message from a client.
// Parses the client id and column, looks up the player piece from the registry,
// and processes the move.
func handleSendMove(conn net.Conn, data json.RawMessage) {
	var payload struct {
		ID     string `json:"id"`
		Column int    `json:"column"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleSendMove] failed to parse payload:", err)
		return
	}

	registryMu.Lock()
	piece, ok := playerRegistry[payload.ID]
	registryMu.Unlock()

	if !ok {
		fmt.Printf("[handleSendMove] unknown client id %s\n", payload.ID)
		sendAckInvalid(conn, "client not registered")
		return
	}

	fmt.Printf("[handleSendMove] player %s (id %s) wants column %d\n", piece, payload.ID, payload.Column)
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
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: server <port>")
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Println("Listening on port", os.Args[1])

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
