#!/usr/bin/env bash
# eval/sync_kb.sh — Copy the current prod KB snapshot to local for eval runs.
# Run this before each eval session to ensure you're testing against real atom data.
#
# Usage:
#   bash eval/sync_kb.sh
#   bash eval/sync_kb.sh /path/to/ssh-key.key   # optional: custom SSH key

set -euo pipefail

OCI_HOST="ubuntu@132.145.33.75"
REMOTE_DB="/opt/trading-galaxy/data/trading_knowledge.db"
LOCAL_DB="./eval/trading_knowledge_eval.db"
SSH_KEY="${1:-$HOME/Downloads/ssh-key-2026-02-27.key}"

echo "Syncing KB from prod ($OCI_HOST) …"

if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found at $SSH_KEY"
    echo "Usage: bash eval/sync_kb.sh [/path/to/key]"
    exit 1
fi

scp -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    "$OCI_HOST:$REMOTE_DB" \
    "$LOCAL_DB"

ATOM_COUNT=$(sqlite3 "$LOCAL_DB" "SELECT COUNT(*) FROM facts;" 2>/dev/null || echo "?")
echo "KB synced → $LOCAL_DB  ($ATOM_COUNT atoms)"
echo ""
echo "To use for eval, set in your local .env:"
echo "  TRADING_KB_DB=$(pwd)/$LOCAL_DB"
