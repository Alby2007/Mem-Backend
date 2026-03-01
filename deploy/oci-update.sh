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

# Load env vars (for CF_API_TOKEN, CF_ZONE_ID, etc.)
set -a
[ -f "$APP_DIR/.env" ] && source "$APP_DIR/.env"
set +a

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

# Purge Cloudflare cache for app.trading-galaxy.uk
if [ -n "${CF_API_TOKEN:-}" ] && [ -n "${CF_ZONE_ID:-}" ]; then
    echo ""
    echo "Purging Cloudflare cache for app.trading-galaxy.uk..."
    PURGE_RESULT=$(curl -s -X POST \
        "https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/purge_cache" \
        -H "Authorization: Bearer ${CF_API_TOKEN}" \
        -H "Content-Type: application/json" \
        --data '{"files":["https://app.trading-galaxy.uk/","https://app.trading-galaxy.uk/index.html"]}')
    echo "$PURGE_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print('Cache purge:', 'OK' if d.get('success') else 'FAILED — ' + str(d.get('errors')))"
else
    echo ""
    echo "Skipping Cloudflare cache purge (CF_API_TOKEN / CF_ZONE_ID not set in .env)"
fi
