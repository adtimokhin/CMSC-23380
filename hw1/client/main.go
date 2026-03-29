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
// move once sc_notify_start confirms the game has started and it is our turn.
var initialCol int

// serverConn is the active connection to the server, stored so that
// handlers can send messages without passing conn through the dispatcher chain.
var serverConn net.Conn

// initialMoveSent tracks whether the startup column argument has been played.
// Once true, subsequent moves must be entered manually via stdin.
var initialMoveSent bool

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
// Confirms whether the connection was accepted or rejected, and if accepted,
// immediately sends the initial move so the server can process it before the
// second player connects.
func handleAckConnect(data json.RawMessage) {
	var payload struct {
		Success bool   `json:"success"`
		Player  string `json:"player"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleAckConnect] failed to parse payload:", err)
		return
	}
	if !payload.Success {
		fmt.Fprintf(os.Stderr, "[handleAckConnect] connection rejected: %s\n", payload.Reason)
		return
	}
	fmt.Printf("[handleAckConnect] connected as player %s, sending initial move (column %d)\n", payload.Player, initialCol)
	if err := sendMove(serverConn, clientID, initialCol); err != nil {
		fmt.Fprintln(os.Stderr, "[handleAckConnect] sendMove error:", err)
		return
	}
	initialMoveSent = true
}

// handleNotifyStart handles a sc_notify_start message from the server.
// Signals that both players have connected and the game is ready to begin.
// The initial move was already sent in handleAckConnect; this just logs the state.
func handleNotifyStart(data json.RawMessage) {
	var payload struct {
		Opponent  string `json:"opponent"`
		FirstTurn string `json:"first_turn"`
		Board     string `json:"board"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleNotifyStart] failed to parse payload:", err)
		return
	}
	fmt.Printf("[handleNotifyStart] game started — opponent: %s, next turn: %s\n%s\n", payload.Opponent, payload.FirstTurn, payload.Board)
}

// handleAckMove handles a sc_ack_move message from the server.
// Contains the updated board state after a valid move was applied.
func handleAckMove(data json.RawMessage) {
	var payload struct {
		Status string `json:"status"`
		Next   string `json:"next"`
		Board  string `json:"board"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Fprintln(os.Stderr, "[handleAckMove] failed to parse payload:", err)
		return
	}
	fmt.Printf("[handleAckMove] status: %s, next: %s\n%s\n", payload.Status, payload.Next, payload.Board)
}

// handleNotifyWin handles a sc_notify_win message from the server.
// Signals that a winning move has been made and the game is over.
func handleNotifyWin(data json.RawMessage) {
	fmt.Println("[handleNotifyWin] Processing win notification from server")
}

// handleAckInvalid handles a sc_ack_invalid message from the server.
// Sent when this client submitted an invalid or out-of-turn move.
func handleAckInvalid(data json.RawMessage) {
	fmt.Println("[handleAckInvalid] Processing invalid move response from server")
}

// handleNotifyError handles a sc_notify_error message from the server.
// Sent when an unexpected event (e.g. opponent disconnected) terminates the session.
func handleNotifyError(data json.RawMessage) {
	fmt.Println("[handleNotifyError] Processing error notification from server")
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
	var err error
	initialCol, err = strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid column:", os.Args[2])
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
	fmt.Printf("Connected to %s as player %s (id: %s)\n", serverAddr, playerPiece, clientID)

	// Announce desired piece and identity. The initial move is deferred until
	// sc_notify_start confirms both players are connected and it is our turn.
	if err := sendConnect(serverConn, playerPiece, clientID); err != nil {
		fmt.Fprintln(os.Stderr, "sendConnect error:", err)
		os.Exit(1)
	}

	// Receive server messages in the background.
	go handleMessages(serverConn)

	// Read subsequent moves from the terminal.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		parseAndSend(serverConn, scanner.Text())
	}
}
