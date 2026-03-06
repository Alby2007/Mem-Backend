"""
ingest/seed_sync.py — Centralised KB seed sync client.

Polls GitHub Releases hourly. If a newer seed is available, downloads
and applies it — shared KB tables only. Personal KB (user_*) is never
touched regardless of seed contents.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone

_log = logging.getLogger(__name__)

REPO            = 'Alby2007/Mem-Backend'
CHECK_INTERVAL  = 3600   # seconds between polls (hourly)

# Hard-coded allowlist — INSERT statements for any other table are silently dropped.
# This is the structural guarantee that personal KB is never overwritten by a sync.
_ALLOWED_TABLES = frozenset({
    'facts',
    'fact_conflicts',
    'causal_edges',
    'pattern_signals',
    'signal_calibration',
    'extraction_queue',
    'edgar_realtime_seen',
    'taxonomy',
    'fact_categories',
    'predicate_vocabulary',
    'working_state',
    'governance_metrics',
    'domain_refresh_queue',
    'synthesis_queue',
    'consolidation_log',
    'kb_insufficient_log',
    'repair_execution_log',
    'repair_rollback_log',
})


class SeedSyncClient:
    """
    Background thread that polls GitHub Releases and applies new KB seeds.

    Usage:
        client = SeedSyncClient(db_path='/data/trading_knowledge.db')
        client.start()   # non-blocking
    """

    def __init__(self, db_path: str, check_interval: int = CHECK_INTERVAL) -> None:
        self.db_path        = db_path
        self.check_interval = check_interval
        self._last_tag      = self._read_tag()
        self._stop_event    = threading.Event()
        self._thread: threading.Thread | None = None

    # ── Public ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name='seed-sync',
        )
        self._thread.start()
        _log.info('SeedSyncClient started — polling every %ds, current tag: %s',
                  self.check_interval, self._last_tag)

    def stop(self) -> None:
        self._stop_event.set()

    # ── Background loop ───────────────────────────────────────────────────────

    def _loop(self) -> None:
        # Run an immediate check on startup
        self._safe_check()
        while not self._stop_event.wait(self.check_interval):
            self._safe_check()

    def _safe_check(self) -> None:
        try:
            self._check_and_apply()
        except Exception as exc:
            _log.warning('SeedSync check failed: %s', exc)
        # Always purge fake atoms — runs even if no new seed was applied.
        # This closes the window where a previous-session seed re-injected
        # test atoms before the manual cleanup script could run.
        self._purge_fake_atoms()

    # ── Core logic ────────────────────────────────────────────────────────────

    def _check_and_apply(self) -> None:
        import requests  # lazy — avoids import error if requests missing

        resp = requests.get(
            f'https://api.github.com/repos/{REPO}/releases',
            headers={'Accept': 'application/vnd.github+json'},
            params={'per_page': 1},
            timeout=15,
        )
        if not resp.ok:
            _log.warning('SeedSync: GitHub API returned %d', resp.status_code)
            return

        releases = resp.json()
        if not releases:
            return

        latest = releases[0]
        latest_tag: str = latest.get('tag_name', '')

        if not latest_tag.startswith('seed-'):
            return   # not a seed release

        if latest_tag == self._last_tag:
            _log.debug('SeedSync: already on latest tag %s', latest_tag)
            return

        _log.info('SeedSync: new seed available — %s (current: %s)',
                  latest_tag, self._last_tag)

        # Find the kb_seed.sql asset
        asset = next(
            (a for a in latest.get('assets', []) if a['name'] == 'kb_seed.sql'),
            None,
        )
        if asset is None:
            _log.warning('SeedSync: release %s has no kb_seed.sql asset', latest_tag)
            return

        # Download
        dl_url = asset['browser_download_url']
        _log.info('SeedSync: downloading %s …', dl_url)
        dl = requests.get(dl_url, timeout=120)
        if not dl.ok:
            _log.warning('SeedSync: download failed — %d', dl.status_code)
            return

        sql_text = dl.text
        self._apply_seed(sql_text, latest_tag)

    # ── Apply ─────────────────────────────────────────────────────────────────

    def _apply_seed(self, sql: str, tag: str) -> None:
        """
        Apply only INSERT statements for allowed shared KB tables.
        Uses INSERT OR IGNORE — idempotent, safe to re-apply.
        Personal KB tables are never touched regardless of seed contents.
        """
        allowed_statements: list[str] = []

        for line in sql.splitlines():
            stripped = line.strip()
            if not stripped.upper().startswith('INSERT'):
                continue
            # Extract table name — handles both quoted and unquoted forms
            # e.g. INSERT INTO facts (...) or INSERT INTO [facts] (...)
            try:
                after_into = stripped.upper().split('INSERT')[1].split('INTO')[1].strip()
                raw_name   = after_into.split()[0].strip('"[]`\'')
                table_name = raw_name.lower()
            except (IndexError, AttributeError):
                continue

            if table_name not in _ALLOWED_TABLES:
                continue

            # Rewrite INSERT → INSERT OR REPLACE so fresher atoms from the live machine
            # overwrite stale local data. Idempotent: re-applying same seed is safe.
            rewritten = stripped.replace('INSERT INTO', 'INSERT OR REPLACE INTO', 1)
            allowed_statements.append(rewritten)

        if not allowed_statements:
            _log.warning('SeedSync: no allowed INSERT statements found in %s', tag)
            self._write_tag(tag)   # still mark as applied to avoid re-download
            self._last_tag = tag
            return

        conn = sqlite3.connect(self.db_path, timeout=15)
        try:
            # Ensure all target tables exist before executing seed statements
            try:
                from knowledge.kb_validation import _ensure_governance_metrics_table
                _ensure_governance_metrics_table(conn)
            except Exception:
                pass
            conn.executescript('\n'.join(allowed_statements))
            after_count = conn.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
        except sqlite3.Error as exc:
            _log.error('SeedSync: DB write failed for %s: %s', tag, exc)
            conn.close()
            return
        finally:
            conn.close()

        self._write_tag(tag)
        self._last_tag = tag
        _log.info('SeedSync: applied %s — %d statements, facts now %d',
                  tag, len(allowed_statements), after_count)
        self._purge_fake_atoms()

    # ── Fake-atom guard ───────────────────────────────────────────────────────

    def _purge_fake_atoms(self) -> None:
        """Delete known fake/eval ticker atoms from the KB after seed application.
        Prevents test data baked into a seed from persisting in production."""
        _FAKE_SUBJECTS = (
            'notreal99', 'fakeco', 'madeupticker', 'randomticker123',
            'xyz corp', 'xyzco', 'fakecorp', 'testco', 'badticker',
            'blobcorp99',
        )
        _FAKE_OBJECT_LIKE = (
            '%notreal99%', '%madeupticker%', '%fakeco%', '%randomticker123%', '%blobcorp99%',
        )
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            total = 0
            for subj in _FAKE_SUBJECTS:
                cur = conn.execute(
                    'DELETE FROM facts WHERE LOWER(subject) = ?', (subj,)
                )
                total += cur.rowcount
            for pat in _FAKE_OBJECT_LIKE:
                cur = conn.execute(
                    'DELETE FROM facts WHERE LOWER(object) LIKE ?', (pat,)
                )
                total += cur.rowcount
            conn.commit()
            conn.close()
            if total:
                _log.info('SeedSync: purged %d fake-ticker atoms after seed apply', total)
        except Exception as exc:
            _log.warning('SeedSync: fake-atom purge failed: %s', exc)

    # ── kb_meta persistence ───────────────────────────────────────────────────

    def _read_tag(self) -> str:
        try:
            from users.user_store import get_kb_meta
            return get_kb_meta(self.db_path, 'seed_tag') or 'none'
        except Exception:
            return 'none'

    def _write_tag(self, tag: str) -> None:
        try:
            from users.user_store import set_kb_meta
            set_kb_meta(self.db_path, 'seed_tag', tag)
        except Exception as exc:
            _log.warning('SeedSync: could not persist seed_tag: %s', exc)
