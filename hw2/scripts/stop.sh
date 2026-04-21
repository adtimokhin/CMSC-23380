#!/usr/bin/env bash
# Stop all kvstore nodes started by start.sh.
# Falls back to port-based kill if .pids is missing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HW2_DIR="$(dirname "$SCRIPT_DIR")"

PIDS_FILE="$HW2_DIR/.pids"

if [[ -f "$PIDS_FILE" ]]; then
    while IFS= read -r pid; do
        kill "$pid" 2>/dev/null && echo "Killed PID $pid" || echo "PID $pid already gone"
    done < "$PIDS_FILE"
    rm -f "$PIDS_FILE"
else
    echo "No .pids file found, falling back to port-based kill..."
fi

# Always clean up any remaining processes on kvstore ports.
KILLED=$(lsof -ti tcp:7000,7001,7002,7100,7101,7102 2>/dev/null | xargs kill -9 2>/dev/null; echo $?)
echo "Ports 7000-7002 / 7100-7102 cleared."
