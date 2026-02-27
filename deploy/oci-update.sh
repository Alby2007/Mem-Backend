#!/usr/bin/env bash
# deploy/oci-update.sh
#
# Pull latest code from GitHub and restart the service.
# Run on the OCI server whenever you push new changes.
#
# Usage:
#   bash ~/trading-galaxy/deploy/oci-update.sh

set -euo pipefail

APP_DIR="$HOME/trading-galaxy"
VENV_DIR="$APP_DIR/.venv"
SERVICE_NAME="trading-galaxy"

echo "[1/4] Pulling latest code..."
git -C "$APP_DIR" pull --ff-only

echo "[2/4] Updating Python dependencies..."
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt" -q

echo "[3/4] Restarting service..."
sudo systemctl restart "$SERVICE_NAME"

echo "[4/4] Checking status..."
sleep 2
sudo systemctl status "$SERVICE_NAME" --no-pager -l

echo ""
echo "Health check:"
curl -s http://localhost:5050/health | python3 -m json.tool || echo "  (API not yet ready — check logs)"
