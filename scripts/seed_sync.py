#!/usr/bin/env python3
"""
scripts/seed_sync.py — Client-side KB seed synchroniser.

Polls GitHub Releases every CHECK_INTERVAL seconds.
When a newer seed tag is detected, downloads and applies it to the local DB.

Usage:
    python scripts/seed_sync.py              # run continuously (daemon mode)
    python scripts/seed_sync.py --once       # check once and exit
    python scripts/seed_sync.py --status     # print current tag and exit

Required env var:
    GITHUB_TOKEN=<PAT with read:releases scope>  (optional — avoids rate limits)

Optional env vars:
    TRADING_KB_DB   path to local SQLite DB  (default: trading_knowledge.db)
    SEED_SYNC_INTERVAL  poll interval in seconds (default: 600 = 10 min)
"""

from __future__ import annotations

import os
import sys
import time
import sqlite3
import pathlib
import logging
import argparse
import tempfile
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [seed_sync] %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ',
)
log = logging.getLogger('seed_sync')

REPO            = 'Alby2007/Mem-Backend'
API_BASE        = f'https://api.github.com/repos/{REPO}'
TAG_FILE        = pathlib.Path('.seed_tag')          # tracks last applied tag
CHECK_INTERVAL  = int(os.environ.get('SEED_SYNC_INTERVAL', 600))   # 10 min default
DB_PATH         = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')


def _headers() -> dict:
    token = os.environ.get('GITHUB_TOKEN', '').strip()
    h = {'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
    if token:
        h['Authorization'] = f'token {token}'
    return h


def get_latest_seed_release() -> dict | None:
    """Return the most recent seed-* prerelease dict, or None."""
    try:
        resp = requests.get(f'{API_BASE}/releases', headers=_headers(),
                            params={'per_page': 20}, timeout=15)
        resp.raise_for_status()
        releases = [r for r in resp.json() if r.get('tag_name', '').startswith('seed-')]
        if not releases:
            return None
        releases.sort(key=lambda r: r['created_at'], reverse=True)
        return releases[0]
    except Exception as e:
        log.warning('GitHub API error: %s', e)
        return None


def get_local_tag() -> str:
    if TAG_FILE.exists():
        return TAG_FILE.read_text().strip()
    return ''


def save_local_tag(tag: str) -> None:
    TAG_FILE.write_text(tag)


def download_seed(release: dict) -> pathlib.Path | None:
    """Download kb_seed.sql asset from a release to a temp file. Returns path."""
    assets = release.get('assets', [])
    asset = next((a for a in assets if a['name'] == 'kb_seed.sql'), None)
    if not asset:
        log.warning('Release %s has no kb_seed.sql asset', release['tag_name'])
        return None

    url = asset['browser_download_url']
    log.info('Downloading seed from %s ...', url)
    try:
        resp = requests.get(url, headers=_headers(), timeout=120, stream=True)
        resp.raise_for_status()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.sql')
        for chunk in resp.iter_content(chunk_size=65536):
            tmp.write(chunk)
        tmp.close()
        size_kb = pathlib.Path(tmp.name).stat().st_size // 1024
        log.info('Downloaded %d KB → %s', size_kb, tmp.name)
        return pathlib.Path(tmp.name)
    except Exception as e:
        log.error('Download failed: %s', e)
        return None


def apply_seed(sql_path: pathlib.Path) -> bool:
    """Execute the seed SQL against the local DB."""
    if not os.path.exists(DB_PATH):
        log.warning('DB not found at %s — seed will create it', DB_PATH)
    try:
        sql = sql_path.read_text(encoding='utf-8')
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.executescript(sql)
        conn.close()
        count = sqlite3.connect(DB_PATH).execute('SELECT COUNT(*) FROM facts').fetchone()[0]
        log.info('Seed applied — facts in DB: %d', count)
        return True
    except Exception as e:
        log.error('Seed apply failed: %s', e)
        return False
    finally:
        try:
            sql_path.unlink()
        except Exception:
            pass


def sync_once() -> bool:
    """Check for a new seed and apply it if found. Returns True if updated."""
    release = get_latest_seed_release()
    if not release:
        log.info('No seed releases found on GitHub')
        return False

    latest_tag = release['tag_name']
    local_tag  = get_local_tag()

    if latest_tag == local_tag:
        log.debug('Seed current: %s', local_tag)
        return False

    log.info('New seed available: %s  (local: %s)', latest_tag, local_tag or 'none')
    sql_path = download_seed(release)
    if not sql_path:
        return False

    if apply_seed(sql_path):
        save_local_tag(latest_tag)
        log.info('Seed updated to %s', latest_tag)
        return True

    return False


def print_status() -> None:
    release = get_latest_seed_release()
    local   = get_local_tag()
    latest  = release['tag_name'] if release else 'unknown'
    up_to_date = '✓ up to date' if local == latest else '✗ STALE'
    print(f'Local tag : {local or "(none)"}')
    print(f'Latest tag: {latest}')
    print(f'Status    : {up_to_date}')
    if release:
        print(f'Published : {release.get("created_at", "?")}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Trading Galaxy seed synchroniser')
    parser.add_argument('--once',   action='store_true', help='Check once and exit')
    parser.add_argument('--status', action='store_true', help='Print status and exit')
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.once:
        updated = sync_once()
        sys.exit(0 if updated else 1)

    log.info('Seed sync daemon started — polling every %ds', CHECK_INTERVAL)
    log.info('DB: %s', DB_PATH)
    while True:
        try:
            sync_once()
        except Exception as e:
            log.error('Unexpected error: %s', e)
        time.sleep(CHECK_INTERVAL)


if __name__ == '__main__':
    main()
