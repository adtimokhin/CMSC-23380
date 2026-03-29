# Connect-M Wire Protocol Specification

## Overview

All messages exchanged between the client and server are JSON objects sent over a TCP connection, delimited by newlines (`\n`). Every message follows a common envelope structure:

```json
{
  "type": "<message_type>",
  "data": { ... }
}
```

- `type` — a string identifying the message type. Naming convention: `<direction>_<verb>_<noun>`, where direction is either `cs` (client → server) or `sc` (server → client).
- `data` — an object whose fields depend on the message type. Always present, even if empty (`{}`).

---

## Client → Server Messages

### `cs_send_connect`

Sent by the client immediately after establishing a TCP connection. Declares which player the client wants to play as.

```json
{
  "type": "cs_send_connect",
  "data": {
    "player": "X"
  }
}
```

| Field    | Type   | Values     | Description                  |
|----------|--------|------------|------------------------------|
| `player` | string | `"X"`, `"O"` | The piece this client claims |

---

### `cs_send_move`

Sent by the client to make a move. The column is 1-indexed (matching the TUI display).

```json
{
  "type": "cs_send_move",
  "data": {
    "player": "X",
    "column": 4
  }
}
```

| Field    | Type   | Description                                      |
|----------|--------|--------------------------------------------------|
| `player` | string | The piece making the move (`"X"` or `"O"`)       |
| `column` | int    | 1-indexed column to drop the piece into          |

---

## Server → Client Messages

### `sc_ack_connect`

Sent by the server in response to `cs_send_connect`. Confirms whether the connection was accepted or rejected.

```json
{
  "type": "sc_ack_connect",
  "data": {
    "success": true,
    "reason": ""
  }
}
```

**On failure:**

```json
{
  "type": "sc_ack_connect",
  "data": {
    "success": false,
    "reason": "game is full, only two players are allowed"
  }
}
```

| Field     | Type    | Description                                                   |
|-----------|---------|---------------------------------------------------------------|
| `success` | bool    | Whether the connection was accepted                           |
| `reason`  | string  | Empty string on success; human-readable error message on failure. Possible values include: `"game is full"`, `"player X is already taken"`, `"invalid player, must be X or O"` |

---

### `sc_notify_start`

Broadcast to both clients when both players have connected and the game is ready to begin.

```json
{
  "type": "sc_notify_start",
  "data": {
    "opponent": "O",
    "first_turn": "X"
  }
}
```

| Field        | Type   | Description                                         |
|--------------|--------|-----------------------------------------------------|
| `opponent`   | string | The piece of the opposing player (`"X"` or `"O"`)  |
| `first_turn` | string | Which player moves first (always `"X"` per Connect-M rules) |

---

### `sc_ack_move`

Sent to both clients after a valid move is applied. Contains the updated board state.

```json
{
  "type": "sc_ack_move",
  "data": {
    "status": "OK",
    "next": "O",
    "board": "  1 2 3 4 5 6 7\n +--------------+\n |. . . . . . . |\n ..."
  }
}
```

| Field    | Type   | Values                   | Description                                                        |
|----------|--------|--------------------------|--------------------------------------------------------------------|
| `status` | string | `"OK"`, `"DRAW"`         | `"OK"` for a normal move; `"DRAW"` if the board is full with no winner |
| `next`   | string | `"X"`, `"O"`, `""`      | Whose turn it is next; empty string if the game is over            |
| `board`  | string | —                        | Full board string as produced by `game.go`'s `String()` method     |

---

### `sc_notify_win`

Sent to both clients when a winning move is made.

```json
{
  "type": "sc_notify_win",
  "data": {
    "winner": "X",
    "board": "  1 2 3 4 5 6 7\n +--------------+\n ..."
  }
}
```

| Field    | Type   | Description                                                     |
|----------|--------|-----------------------------------------------------------------|
| `winner` | string | The piece that won (`"X"` or `"O"`)                             |
| `board`  | string | Final board state as produced by `game.go`'s `String()` method  |

---

### `sc_ack_invalid`

Sent only to the client that submitted an invalid or out-of-turn move. The game state is unchanged.

```json
{
  "type": "sc_ack_invalid",
  "data": {
    "reason": "it is not your turn"
  }
}
```

| Field    | Type   | Description                                                                                                        |
|----------|--------|--------------------------------------------------------------------------------------------------------------------|
| `reason` | string | Human-readable explanation. Possible values include: `"it is not your turn"`, `"column is full"`, `"column out of range"`, `"game is already over"` |

---

### `sc_notify_error`

Sent to the remaining client when an unexpected event occurs that terminates the session.

```json
{
  "type": "sc_notify_error",
  "data": {
    "reason": "opponent disconnected"
  }
}
```

| Field    | Type   | Description                                                                                          |
|----------|--------|------------------------------------------------------------------------------------------------------|
| `reason` | string | Human-readable explanation. Possible values include: `"opponent disconnected"`, `"server shutting down"` |

---

## Message Flow

### Normal game (X wins)

```
Client X                    Server                    Client O
   |                           |                           |
   |-- cs_send_connect ------->|                           |
   |<- sc_ack_connect (ok) ----|                           |
   |                           |<------ cs_send_connect ---|
   |                           |------- sc_ack_connect --->|
   |                           |                           |
   |<- sc_notify_start --------|------- sc_notify_start -->|
   |                           |                           |
   |-- cs_send_move(col=4) --->|                           |
   |<- sc_ack_move(next=O) ----|------- sc_ack_move ------>|
   |                           |                           |
   |                           |<------ cs_send_move ------|
   |<- sc_ack_move(next=X) ----|------- sc_ack_move ------>|
   |                           |                           |
   |      ... more moves ...   |                           |
   |                           |                           |
   |-- cs_send_move(win) ----->|                           |
   |<- sc_notify_win(X) -------|------- sc_notify_win(X) ->|
```

### Rejected connection (game full)

```
Client Z                    Server
   |                           |
   |-- cs_send_connect ------->|
   |<- sc_ack_connect(fail) ---|
   | (connection closed)       |
```

### Invalid move

```
Client X                    Server
   |                           |
   |-- cs_send_move(col=99) -->|
   |<- sc_ack_invalid ----------|
   | (game continues)          |
```

---

## Design Notes

- The server is the **sole source of truth** for game state. Clients hold no game logic.
- The server waits until **both X and O have connected** before sending `sc_notify_start` to either.
- After `sc_notify_win` or a `"DRAW"` status in `sc_ack_move`, all further `cs_send_move` messages are rejected with `sc_ack_invalid` and reason `"game is already over"`.
- Column values in `cs_send_move` are **1-indexed**. The server converts to 0-indexed internally before passing to `game.go`.
- The `board` field in `sc_ack_move` and `sc_notify_win` is the exact string output of `game.go`'s `Board.String()` method, with newlines represented as `\n` within the JSON string.
