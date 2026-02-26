#!/usr/bin/env python3
"""
Export a fresh KB seed and push it to GitHub Releases.

Run on your live machine 3x daily (pre-market, midday, post-market IST).

Usage:
    python scripts/push_seed.py

Required env var (your machine only):
    GITHUB_TOKEN=<PAT with write:releases scope>

Schedule (Windows Task Scheduler or cron):
    08:30 IST → 03:00 UTC  →  python scripts/push_seed.py
    12:30 IST → 07:00 UTC  →  python scripts/push_seed.py
    16:30 IST → 11:00 UTC  →  python scripts/push_seed.py

Crontab equivalent (UTC):
    0  3 * * *  cd /path/to/trading-galaxy && python scripts/push_seed.py
    0  7 * * *  cd /path/to/trading-galaxy && python scripts/push_seed.py
    0 11 * * *  cd /path/to/trading-galaxy && python scripts/push_seed.py
"""

from __future__ import annotations

import os
import sys
import subprocess
import pathlib
from datetime import datetime, timezone

import requests

REPO      = 'Alby2007/Mem-Backend'
SEED_PATH = pathlib.Path('tests/fixtures/kb_seed.sql')
KEEP_RELEASES = 21   # 7 days × 3 pushes/day — full rollback window

API_BASE  = f'https://api.github.com/repos/{REPO}'
HEADERS   = {}       # populated in main() after token check


def die(msg: str) -> None:
    print(f'\nERROR: {msg}', file=sys.stderr)
    sys.exit(1)


def gh(method: str, path: str, **kwargs) -> requests.Response:
    url = f'{API_BASE}{path}' if path.startswith('/') else path
    resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
    if not resp.ok:
        die(f'GitHub API {method} {url} → {resp.status_code}: {resp.text[:400]}')
    return resp


def export_seed() -> None:
    print('Exporting fresh seed …')
    result = subprocess.run(
        [sys.executable, 'scripts/export_seed.py'],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        die('export_seed.py failed — aborting push.')


def create_release(tag: str) -> dict:
    print(f'Creating release {tag} …')
    resp = gh('POST', '/releases', json={
        'tag_name':   tag,
        'name':       f'KB Seed {tag}',
        'body':       'Automatic KB seed update — shared market intelligence only, no user data.',
        'prerelease': True,
    })
    return resp.json()


def upload_asset(release: dict, path: pathlib.Path) -> None:
    upload_url = release['upload_url'].replace('{?name,label}', '')
    print(f'Uploading {path.name} ({path.stat().st_size // 1024} KB) …')
    with path.open('rb') as f:
        resp = requests.post(
            upload_url,
            headers={**HEADERS, 'Content-Type': 'application/octet-stream'},
            params={'name': path.name},
            data=f,
            timeout=120,
        )
    if not resp.ok:
        die(f'Asset upload failed → {resp.status_code}: {resp.text[:400]}')
    print(f'Asset uploaded: {resp.json().get("browser_download_url", "?")}')


def prune_old_releases() -> None:
    resp = gh('GET', '/releases', params={'per_page': 100})
    releases = [r for r in resp.json() if r.get('tag_name', '').startswith('seed-')]
    releases.sort(key=lambda r: r['created_at'], reverse=True)

    to_delete = releases[KEEP_RELEASES:]
    if not to_delete:
        print(f'Release count = {len(releases)} — no pruning needed.')
        return

    print(f'Pruning {len(to_delete)} release(s) older than last {KEEP_RELEASES} …')
    for rel in to_delete:
        gh('DELETE', f'/releases/{rel["id"]}')
        # Also delete the git tag so the repo stays clean
        try:
            gh('DELETE', f'/git/refs/tags/{rel["tag_name"]}')
        except SystemExit:
            pass   # tag deletion is best-effort
        print(f'  Deleted: {rel["tag_name"]}')


def main() -> None:
    global HEADERS

    token = os.environ.get('GITHUB_TOKEN', '').strip()
    if not token:
        die('GITHUB_TOKEN is not set.\n'
            '       Create a Personal Access Token with the "write:releases" scope\n'
            '       and set it in your environment: export GITHUB_TOKEN=ghp_...')

    HEADERS = {
        'Authorization': f'token {token}',
        'Accept':        'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }

    if not SEED_PATH.parent.exists():
        die(f'Directory {SEED_PATH.parent} not found — run from repo root.')

    # 1. Export fresh seed (quality-gated)
    export_seed()

    if not SEED_PATH.exists():
        die(f'Seed file not found at {SEED_PATH} after export.')

    # 2. Create release
    tag = datetime.now(timezone.utc).strftime('seed-%Y%m%d-%H%M')
    release = create_release(tag)

    # 3. Upload seed asset
    upload_asset(release, SEED_PATH)

    # 4. Prune old releases (keep last 21)
    prune_old_releases()

    print(f'\nDone. Tag: {tag}')
    print(f'Release URL: {release.get("html_url", "?")}')


if __name__ == '__main__':
    main()
