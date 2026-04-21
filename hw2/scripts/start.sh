#!/usr/bin/env bash
# Start all three kvstore nodes in the background.
# Logs go to logs/node{0,1,2}.log; PIDs are saved to .pids for stop.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HW2_DIR="$(dirname "$SCRIPT_DIR")"

cd "$HW2_DIR"

mkdir -p logs

go build -o /tmp/kvstore_server ./server

echo "Starting nodes..."
/tmp/kvstore_server --id=0 --config=nodeconfig.json > logs/node0.log 2>&1 &
echo $! > .pids
/tmp/kvstore_server --id=1 --config=nodeconfig.json > logs/node1.log 2>&1 &
echo $! >> .pids
/tmp/kvstore_server --id=2 --config=nodeconfig.json > logs/node2.log 2>&1 &
echo $! >> .pids

sleep 0.5
echo "Nodes started (PIDs: $(tr '\n' ' ' < .pids))"
echo "Logs: hw2/logs/node{0,1,2}.log"
