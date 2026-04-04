#!/bin/bash
# Load the shared KB seed into a running Docker container
# Run this after: docker-compose up
#
# Usage:
#   bash scripts/load_seed.sh

set -e

SEED=tests/fixtures/kb_seed.sql
SERVICE=trading-galaxy
DB_PATH=/data/trading_knowledge.db
API=http://localhost:5050

if [ ! -f "$SEED" ]; then
    echo "ERROR: seed file not found at '$SEED'"
    echo "       Make sure you are running this from the repo root."
    exit 1
fi

# Check container is running
if ! docker-compose ps --services --filter "status=running" 2>/dev/null | grep -q "^${SERVICE}$"; then
    echo "ERROR: '${SERVICE}' container is not running."
    echo "       Start it first: docker-compose up -d"
    exit 1
fi

echo "Loading KB seed into ${SERVICE}:${DB_PATH}..."
docker-compose exec -T "$SERVICE" sqlite3 "$DB_PATH" < "$SEED"
echo "Seed loaded."

# Verify
echo ""
echo "Verifying via GET /stats..."
STATS=$(curl -s --max-time 10 "${API}/stats" || true)
if [ -z "$STATS" ]; then
    echo "WARNING: Could not reach ${API}/stats — is the API healthy?"
    echo "         Run: docker-compose logs ${SERVICE}"
else
    echo "$STATS" | python3 -m json.tool 2>/dev/null || echo "$STATS"
fi

echo ""
echo "Next steps:"
echo "  1. Register your test user  : curl -s -X POST ${API}/auth/register -H 'Content-Type: application/json' -d '{...}'"
echo "  2. Submit your portfolio    : curl -s -X POST ${API}/users/{id}/portfolio ..."
echo "  3. Explore real signal data : curl -s ${API}/tickers/RELIANCE.NS/summary"
