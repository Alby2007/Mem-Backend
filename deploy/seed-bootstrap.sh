#!/usr/bin/env bash
# deploy/seed-bootstrap.sh
#
# First-boot seed loader. Downloads the latest kb_seed.sql from GitHub
# Releases and loads it into the kb-data Docker volume.
#
# Run ONCE after first `docker compose up -d`, before the API takes traffic.
# Safe to re-run — all seed INSERTs use INSERT OR IGNORE / upsert semantics.
#
# Usage:
#   bash deploy/seed-bootstrap.sh
#   bash deploy/seed-bootstrap.sh --force   # re-apply even if seed flag exists

set -euo pipefail

REPO="Alby2007/Mem-Backend"
ASSET="kb_seed.sql"
VOLUME_CONTAINER="trading-galaxy-trading-galaxy-1"   # adjust if compose project name differs
DB_PATH="/data/trading_knowledge.db"
FLAG_FILE="/data/.seed_loaded"
TMPDIR_SEED="$(mktemp -d)"

FORCE=0
for arg in "$@"; do [[ "$arg" == "--force" ]] && FORCE=1; done

cleanup() { rm -rf "$TMPDIR_SEED"; }
trap cleanup EXIT

echo "=== Trading Galaxy — KB Seed Bootstrap ==="

# ── 1. Check if seed already loaded ───────────────────────────────────────────
if [[ "$FORCE" -eq 0 ]]; then
    ALREADY=$(docker exec "$VOLUME_CONTAINER" \
        python -c "import os; print('yes' if os.path.exists('$FLAG_FILE') else 'no')" 2>/dev/null || echo "no")
    if [[ "$ALREADY" == "yes" ]]; then
        echo "Seed already loaded (flag exists at $FLAG_FILE). Use --force to re-apply."
        exit 0
    fi
fi

# ── 2. Discover latest release URL ────────────────────────────────────────────
echo "Fetching latest release from github.com/$REPO ..."
RELEASE_URL=$(curl -fsSL \
    "https://api.github.com/repos/${REPO}/releases/latest" \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
assets = data.get('assets', [])
for a in assets:
    if a['name'] == '${ASSET}':
        print(a['browser_download_url'])
        break
")

if [[ -z "$RELEASE_URL" ]]; then
    echo "ERROR: Could not find ${ASSET} in latest release of ${REPO}"
    exit 1
fi

echo "Downloading: $RELEASE_URL"
curl -fsSL -o "${TMPDIR_SEED}/${ASSET}" "$RELEASE_URL"
SIZE=$(du -sh "${TMPDIR_SEED}/${ASSET}" | cut -f1)
echo "Downloaded ${ASSET} (${SIZE})"

# ── 3. Copy SQL into the running container and load it ────────────────────────
echo "Loading seed into $DB_PATH ..."
docker cp "${TMPDIR_SEED}/${ASSET}" "${VOLUME_CONTAINER}:/tmp/${ASSET}"

docker exec "$VOLUME_CONTAINER" python3 - <<'PYEOF'
import sqlite3, sys, os

db_path  = os.environ.get('TRADING_KB_DB', '/data/trading_knowledge.db')
sql_path = '/tmp/kb_seed.sql'
flag     = '/data/.seed_loaded'

print(f'  DB: {db_path}')
conn = sqlite3.connect(db_path, timeout=30)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')

with open(sql_path, 'r', encoding='utf-8') as f:
    sql = f.read()

statements = [s.strip() for s in sql.split(';') if s.strip()]
ok = err = 0
for stmt in statements:
    try:
        conn.execute(stmt)
        ok += 1
    except sqlite3.IntegrityError:
        pass   # INSERT OR IGNORE — expected on re-runs
    except Exception as e:
        err += 1
        if err <= 5:
            print(f'  WARN: {e}')

conn.commit()
conn.close()

# Write flag so subsequent boots skip re-loading
with open(flag, 'w') as f:
    import datetime
    f.write(datetime.datetime.utcnow().isoformat())

print(f'  Loaded {ok} statements, {err} errors (IntegrityError skipped)')
print(f'  Flag written: {flag}')
PYEOF

echo ""
echo "=== Seed bootstrap complete ==="
echo "Verify with: curl https://api.yourdomain.com/health"
echo "             curl https://api.yourdomain.com/stats"
