#!/usr/bin/env bash
# deploy/backup_db.sh — nightly SQLite online backup
#
# Cron (install once on OCI):
#   (crontab -l 2>/dev/null; echo "0 2 * * * bash ~/trading-galaxy/deploy/backup_db.sh >> ~/logs/backup.log 2>&1") | crontab -
#
# Keeps 7 days of gzipped backups in ~/backups/trading-galaxy/

set -euo pipefail

DB="${TRADING_GALAXY_DB:-/opt/trading-galaxy/data/trading_knowledge.db}"
BACKUP_DIR="$HOME/backups/trading-galaxy"
KEEP_DAYS=7
LOG_TAG="[$(date -u +%FT%TZ)]"

mkdir -p "$BACKUP_DIR"
mkdir -p "$HOME/logs"

if [ ! -f "$DB" ]; then
    echo "$LOG_TAG ERROR: DB not found at $DB" >&2
    exit 1
fi

DEST="$BACKUP_DIR/kb_$(date +%Y%m%d_%H%M%S).db"

# sqlite3 .backup uses the online backup API — safe for live DBs (no lock needed)
sqlite3 "$DB" ".backup '$DEST'"
gzip "$DEST"

SIZE=$(du -sh "${DEST}.gz" | cut -f1)
echo "$LOG_TAG backup OK → ${DEST}.gz (${SIZE})"

# Prune backups older than KEEP_DAYS
find "$BACKUP_DIR" -name "*.db.gz" -mtime "+${KEEP_DAYS}" -delete
REMAINING=$(find "$BACKUP_DIR" -name "*.db.gz" | wc -l)
echo "$LOG_TAG pruned to last ${KEEP_DAYS} days (${REMAINING} files remaining)"
