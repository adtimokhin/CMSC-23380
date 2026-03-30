As part of your submission, please answer the following questions:

## Design Decisions

1. What wire protocol did you choose for client-server communication, and why? 
What are the advantages and disadvantages of your chosen protocol compared to other options you considered?

**Protocol: newline-delimited JSON over TCP.**

Every message — in both directions — is a single JSON object followed by a `\n` byte. All messages share a two-field format:

```json
{"type": "<message_type>", "data": { ... }}
```

The `type` field encodes direction and intent using the convention `<cs|sc>_<verb>_<noun>`. The eight message types, with their exact `data` fields as implemented, are:

**Client → Server**

`cs_send_connect` carries `player` (string `"X"` or `"O"`) and `id` (string). `id` is the client's local TCP address, used as a stable unique identifier for the session so the server can map subsequent moves to the right piece without the client re-sending its piece on every message.

`cs_send_move` carries `id` (string) and `column` (int, 1-indexed). It uses `id` rather than `player`; the server resolves the piece from its registry.

**Server → Client**

`sc_ack_connect` carries `success` (bool), `player` (string `"X"`, `"O"`, or `""`), and `reason` (string). On success, `player` echoes the assigned piece and `reason` is empty. On failure, `player` is empty and `reason` is one of `"player X is already taken"`, `"player O is already taken"`, `"invalid player, must be X or O"`.

`sc_notify_start` carries `opponent` (string), `first_turn` (string), and `board` (string). Broadcast to both clients when the second player connects. `first_turn` is the piece whose turn it is **next** — i.e., the second player's piece, because the first player has already made their opening move before the second player connected. `board` is the current board state after that opening move.

`sc_ack_move` carries `status` (string `"OK"` or `"DRAW"`), `next` (string `"X"`, `"O"`, or `""`), and `board` (string). Broadcast to both clients after every valid move. `next` is empty when the game is over. `board` is the full board rendered by `game.Board.String()`.

`sc_notify_win` carries `winner` (string `"X"` or `"O"`) and `board` (string). Broadcast to both clients when a winning move is detected.

`sc_ack_invalid` carries `reason` (string). Sent only to the client that submitted a bad move. Possible reasons: `"it is not your turn"`, `"column is full"`, `"column out of range"`, `"game is already over"`. Game state is unchanged.

`sc_notify_error` carries `reason` (string). Sent to the surviving client when a session-terminating event occurs. Possible reasons: `"opponent disconnected"`, `"server shutting down"`.

**Why JSON?**

JSON is human-readable, which made iterative development and debugging straightforward. Adding a field (e.g., `board` to `sc_notify_start`, or `id` to `cs_send_connect`) required no changes to the framing layer; both sides just ignore unknown fields. Go's `encoding/json` package handles marshalling and unmarshalling with minimal boilerplate.

**Advantages over alternatives:**
- *vs. a binary protocol (e.g. raw bytes):* far easier to inspect with basic tools.
- *vs. a custom text protocol:* the envelope/type/data structure means all message parsing is uniform — a single `json.Unmarshal` into the `Message` struct dispatches to the right handler, with no per-message parsing logic at the framing level.

**Disadvantages:**
- JSON is verbose; a binary format would be more bandwidth-efficient at scale.

2. How does your server handle concurrent requests? What mechanisms did you
use to ensure that the game state is updated correctly when multiple clients are
connected?

The server spawns one goroutine per accepted TCP connection (`go handleMessages(c)` in `main`). Each goroutine reads messages in a loop and calls the appropriate handler (`handleSendConnect` or `handleSendMove`).

All shared game state — `board`, `playerRegistry`, `takenPieces`, `playerConns`, `firstPlayer`, `currentTurn`, `gameOver` — is protected by a single `sync.Mutex` named `gameMu`. Every handler acquires `gameMu` before reading or writing any of these variables and releases it before performing any I/O.

The key discipline is that **I/O is never done while holding the lock**. Before unlocking, handlers snapshot the values they need (connection handles, board string, turn string) into local variables. The actual `sendMessage` and `broadcast` calls happen after `gameMu.Unlock()`. This prevents a blocked write from starving the other goroutine.

Example from `handleSendConnect`: both `connX`/`connO` and the board snapshot are captured inside the lock, then `sendNotifyStart` is called outside it:

```go
var boardStr, nextTurn string
if bothConnected {
    boardStr = board.String()
    nextTurn = currentTurn
}
gameMu.Unlock()
// I/O happens here, after the lock is released
if bothConnected {
    sendNotifyStart(connX, "O", nextTurn, boardStr)
    sendNotifyStart(connO, "X", nextTurn, boardStr)
}
```

Because the game only ever has two clients and all state changes are serialised through `gameMu`, a single mutex is sufficient — there is no risk of deadlock and no need for finer-grained locking.

3. How does your client handle invalid moves or errors from the server? What feedback does it provide to the user in these cases?

Invalid input is caught at two layers:

**Client-side (before contacting the server):**
- If the `<X|O>` argument is not exactly `"X"` or `"O"`, the client prints `INVALID` to stdout and exits with code 1 immediately, without opening a TCP connection.
- If the `<column>` argument is not a valid integer, the client also prints `INVALID` and exits with code 1.

**Server-side responses:**
- `sc_ack_connect` with `success: false` — printed as `INVALID` to stdout; the client exits with code 1 (e.g., if the requested piece is already taken).
- `sc_ack_invalid` — the client prints `INVALID` if the reason is anything other than game over (e.g., wrong turn, column full, column out of range). For the `"game is already over"` reason specifically, it prints `ERROR game is already over, restart the server to play again` and exits with code 1, since there is no recovery possible without restarting the server.
- `sc_notify_error` — printed as `ERROR <reason>` to stdout (e.g., `ERROR opponent disconnected`); the client exits with code 1.

After a recoverable invalid move (`INVALID` without exit), the client remains connected and continues reading from stdin, allowing the user to enter a corrected column number.

## Reflections
1. What was the most challenging aspect of this assignment, and how did you overcome it?

Developing the communication schema. Considering all of the scenarios and appropriate message formats was the most involved part of the homework.

2. How long did you spend on this assignment, and how did you allocate your time across different tasks (e.g., designing the protocol, implementing the server, implementing the client, testing)?

Desining protocol ~2hrs
Implementing the server and client ~1.5 hr

3. How is Go different from other programming languages you have used in the past? What features of Go did you find particularly useful or challenging to work with?

Compared to Python, Go feels much more structured — explicit types, no implicit duck typing, and errors returned as values rather than raised as exceptions.

Goroutines were the standout feature. Spawning one per connection with `go handleMessages(c)` and letting the runtime schedule them was simpler than managing threads manually. The main challenge was internalising the rule that I/O must not happen while holding `gameMu` — that discipline has to come from the programmer, not the language.
