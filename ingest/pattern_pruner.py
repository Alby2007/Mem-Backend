"""ingest/pattern_pruner.py — Daily pattern signal pruner + WAL checkpoint.

Deletes broken/expired pattern_signals older than 14 days.
At ~40k broken patterns/day the table grows ~280k rows/week without pruning.
Also forces a WAL checkpoint so the on-disk DB file doesn't balloon.

Registered in api_v2.py at 86400s (daily) interval.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)


class PatternPrunerAdapter(BaseIngestAdapter):
    """
    Deletes stale broken/expired pattern_signals and forces WAL checkpoint.
    Returns no atoms — side-effect only adapter.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='pattern_pruner')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=30000')

            # Count before
            before = conn.execute(
                "SELECT COUNT(*) FROM pattern_signals WHERE status IN ('broken','expired')"
            ).fetchone()[0]

            # Delete broken/expired older than 14 days
            conn.execute("""
                DELETE FROM pattern_signals
                WHERE status IN ('broken', 'expired')
                AND detected_at < datetime('now', '-14 days')
            """)
            deleted = conn.total_changes
            conn.commit()

            # WAL checkpoint — keeps file size manageable
            try:
                conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            except Exception as _wc_e:
                _logger.warning('pattern_pruner: WAL checkpoint failed: %s', _wc_e)
                try:
                    conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
                except Exception:
                    pass

            conn.close()

            _logger.info(
                'pattern_pruner: deleted %d stale patterns (was %d broken/expired), WAL checkpointed',
                deleted, before,
            )
        except Exception as e:
            _logger.error('pattern_pruner: failed: %s', e)

        return []  # no atoms — side-effect only
