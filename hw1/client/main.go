package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// playerPiece stores which piece ("X" or "O") this client requested.
var playerPiece string

// clientID is the unique identifier sent to the server, derived from the
// local TCP address assigned at dial time (e.g. "127.0.0.1:54321").
var clientID string

// initialCol is the column supplied on the command line, sent as the first
// move immediately after sc_ack_connect confirms the connection.
var initialCol int

// serverConn is the active connection to the server, stored so that
// handlers can send messages without passing conn through the dispatcher chain.
var serverConn net.Conn

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

// sendConnect sends a cs_send_connect message to the server, declaring which
// player piece this client wants and providing a unique client identifier.
// Must be sent immediately after the TCP connection is established.
//
// Format:
//
//	{"type": "cs_send_connect", "data": {"player": "<X|O>", "id": "<string>"}}
//
// player must be "X" or "O".
// id is a unique string identifying this client (e.g. its local TCP address).
// The server maps the id to the requested piece; subsequent messages use id
// instead of player so that the server is the authority on piece assignment.
func sendConnect(conn net.Conn, player, id string) error {
	data, err := json.Marshal(map[string]string{"player": player, "id": id})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "cs_send_connect", Data: data})
}

// sendMove sends a cs_send_move message to the server to drop a piece into a
// column. The client is identified by its id; the server resolves the piece.
//
// Format:
//
//	{"type": "cs_send_move", "data": {"id": "<string>", "column": <int>}}
//
// id must match the id used in cs_send_connect.
// column is 1-indexed (1 through board width).
func sendMove(conn net.Conn, id string, column int) error {
	data, err := json.Marshal(map[string]interface{}{"id": id, "column": column})
	if err != nil {
		return err
	}
	return sendMessage(conn, Message{Type: "cs_send_move", Data: data})
}

// parseAndSend reads a line of terminal input and dispatches to the appropriate
// send function. Supported commands:
//
//	connect <X|O>   — sends a cs_send_connect message
//	move <column>   — sends a cs_send_move message using the stored clientID
//	<column>        — shorthand for move <column>
func parseAndSend(conn net.Conn, line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}

	parts := strings.Fields(line)

	switch strings.ToLower(parts[0]) {
	case "connect":
		if len(parts) < 2 {
			fmt.Fprintln(os.Stderr, "usage: connect <X|O>")
			return
		}
		if err := sendConnect(conn, parts[1], clientID); err != nil {
			fmt.Fprintln(os.Stderr, "sendConnect error:", err)
		}

	case "move":
		if len(parts) < 2 {
			fmt.Fprintln(os.Stderr, "usage: move <column>")
			return
		}
		col, err := strconv.Atoi(parts[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid column:", parts[1])
			return
		}
		if err := sendMove(conn, clientID, col); err != nil {
			fmt.Fprintln(os.Stderr, "sendMove error:", err)
		}

	default:
		// Bare number is shorthand for a move.
		col, err := strconv.Atoi(parts[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, "unknown command:", parts[0])
			return
		}
		if err := sendMove(conn, clientID, col); err != nil {
			fmt.Fprintln(os.Stderr, "sendMove error:", err)
		}
	}
}

// handleAckConnect handles a sc_ack_connect message from the server.
// On success, immediately sends the initial move so the server can process it
// before the second player connects. On failure, prints INVALID and exits.
func handleAckConnect(data json.RawMessage) {
	var payload struct {
		Success bool   `json:"success"`
		Player  string `json:"player"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse sc_ack_connect:", err)
		os.Exit(1)
	}
	if !payload.Success {
		fmt.Println("INVALID")
		os.Exit(1)
	}
	if err := sendMove(serverConn, clientID, initialCol); err != nil {
		fmt.Fprintln(os.Stderr, "sendMove error:", err)
		os.Exit(1)
	}
}

// handleNotifyStart handles a sc_notify_start message from the server.
// Signals that both players have connected. The initial move was already sent
// in handleAckConnect; nothing needs to be printed here.
func handleNotifyStart(data json.RawMessage) {
	// Both players now connected; output will arrive via sc_ack_move.
}

// handleAckMove handles a sc_ack_move message from the server.
// Prints the updated board state after a valid move.
//
// Output:
//
//	OK NEXT <X|O>    (status "OK")
//	OK DRAW          (status "DRAW")
//	<board>
func handleAckMove(data json.RawMessage) {
	var payload struct {
		Status string `json:"status"`
		Next   string `json:"next"`
		Board  string `json:"board"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse sc_ack_move:", err)
		return
	}
	switch payload.Status {
	case "OK":
		fmt.Printf("OK NEXT %s\n%s", payload.Next, payload.Board)
	case "DRAW":
		fmt.Printf("OK DRAW\n%s", payload.Board)
	}
}

// handleNotifyWin handles a sc_notify_win message from the server.
// Prints the winning player and final board state.
//
// Output:
//
//	OK WIN <X|O>
//	<board>
func handleNotifyWin(data json.RawMessage) {
	var payload struct {
		Winner string `json:"winner"`
		Board  string `json:"board"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse sc_notify_win:", err)
		return
	}
	fmt.Printf("OK WIN %s\n%s", payload.Winner, payload.Board)
}

// handleAckInvalid handles a sc_ack_invalid message from the server.
// Prints INVALID for rejected moves, or a descriptive ERROR if the game is over.
func handleAckInvalid(data json.RawMessage) {
	var payload struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse sc_ack_invalid:", err)
		return
	}
	if payload.Reason == "game is already over" {
		fmt.Println("ERROR game is already over, restart the server to play again")
		os.Exit(1)
	}
	fmt.Println("INVALID")
}

// handleNotifyError handles a sc_notify_error message from the server.
// Sent when an unexpected event (e.g. opponent disconnected) terminates the session.
func handleNotifyError(data json.RawMessage) {
	var payload struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse sc_notify_error:", err)
		os.Exit(1)
	}
	fmt.Printf("ERROR %s\n", payload.Reason)
	os.Exit(1)
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
		case "sc_ack_connect":
			handleAckConnect(msg.Data)
		case "sc_notify_start":
			handleNotifyStart(msg.Data)
		case "sc_ack_move":
			handleAckMove(msg.Data)
		case "sc_notify_win":
			handleNotifyWin(msg.Data)
		case "sc_ack_invalid":
			handleAckInvalid(msg.Data)
		case "sc_notify_error":
			handleNotifyError(msg.Data)
		default:
			fmt.Fprintln(os.Stderr, "unknown message type:", msg.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "read error:", err)
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: client <X|O> <column> <server_addr>")
		os.Exit(1)
	}

	playerPiece = os.Args[1]
	if playerPiece != "X" && playerPiece != "O" {
		fmt.Println("INVALID")
		os.Exit(1)
	}

	var err error
	initialCol, err = strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("INVALID")
		os.Exit(1)
	}
	serverAddr := os.Args[3]

	serverConn, err = net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer serverConn.Close()

	// Use the local TCP address as a stable unique identifier for this session.
	clientID = serverConn.LocalAddr().String()

	// Announce desired piece and identity. The initial move is sent immediately
	// upon receiving sc_ack_connect, before the second player connects.
	if err := sendConnect(serverConn, playerPiece, clientID); err != nil {
		fmt.Fprintln(os.Stderr, "sendConnect error:", err)
		os.Exit(1)
	}

	// Read subsequent moves from the terminal in the background.
	// When stdin closes, close the connection so handleMessages returns.
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			parseAndSend(serverConn, scanner.Text())
		}
		serverConn.Close()
	}()

	// Block on receiving server messages. Returns when the connection closes.
	handleMessages(serverConn)
}
