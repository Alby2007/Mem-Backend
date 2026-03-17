"""
ingest/kb_cleanup_adapter.py — Scheduled KB Garbage Collection

Runs every 6 hours. Prunes four categories of bloat:

  1. Stale pattern decay facts
     Predicates: pattern_hours_remaining, pattern_decay_pct, pattern_estimated_expiry
     Recalculated every 900s — anything older than 2 hours is dead weight.
     Expected recovery: ~75,000 rows/week

  2. Old thesis objects
     Subject pattern: thesis_{ticker}_{user}_{YYYYMMDD}
     Keeps only today's date suffix. Expected recovery: ~46,000 rows/week

  3. Duplicate macro facts
     Predicates: inflation_environment, regime_label, central_bank_stance
     Keeps lowest rowid per (subject, predicate). Recovery: ~1,000 rows/week

  4. Stale refresh tokens
     Tokens expired >7 days ago or revoked >30 days ago serve no purpose.
     Without cleanup this table grows unbounded (~1k rows/month).

INTERVAL: 21600s (every 6 hours)
Runs VACUUM after each cycle to reclaim disk space from deleted rows.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_DECAY_PREDICATES = (
    'pattern_hours_remaining',
    'pattern_decay_pct',
    'pattern_estimated_expiry',
)

_DUPLICATE_MACRO_PREDICATES = (
    'inflation_environment',
    'regime_label',
    'central_bank_stance',
)


class KBCleanupAdapter(BaseIngestAdapter):
    """
    Scheduled garbage collector for the facts table and auth tables.
    Runs four targeted DELETE passes then VACUUMs the DB.
    """

    name = 'kb_cleanup_adapter'

    def __init__(self, db_path: str) -> None:
        super().__init__(self.name)
        self._db = db_path

    def fetch(self) -> dict:
        conn = sqlite3.connect(self._db, timeout=60)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=60000')

        counts = {}
        before = conn.execute('SELECT COUNT(*) FROM facts').fetchone()[0]

        try:
            # ── Pass 1: stale pattern decay facts ─────────────────────────
            placeholders = ','.join('?' * len(_DECAY_PREDICATES))
            r1 = conn.execute(
                f"""DELETE FROM facts
                    WHERE predicate IN ({placeholders})
                    AND timestamp < datetime('now', '-2 hours')""",
                _DECAY_PREDICATES,
            )
            conn.commit()
            counts['decay_facts_deleted'] = r1.rowcount

            # ── Pass 2: old thesis objects ─────────────────────────────────
            today_suffix = datetime.now(timezone.utc).strftime('%Y%m%d')
            r2 = conn.execute(
                """DELETE FROM facts
                   WHERE subject LIKE 'thesis_%'
                   AND SUBSTR(subject, -8) < ?""",
                (today_suffix,),
            )
            conn.commit()
            counts['old_theses_deleted'] = r2.rowcount

            # ── Pass 3: duplicate macro facts ─────────────────────────────
            placeholders = ','.join('?' * len(_DUPLICATE_MACRO_PREDICATES))
            r3 = conn.execute(
                f"""DELETE FROM facts
                    WHERE predicate IN ({placeholders})
                    AND subject NOT LIKE 'thesis_%'
                    AND rowid NOT IN (
                        SELECT MIN(rowid) FROM facts
                        WHERE predicate IN ({placeholders})
                        AND subject NOT LIKE 'thesis_%'
                        GROUP BY subject, predicate
                    )""",
                _DUPLICATE_MACRO_PREDICATES * 2,
            )
            conn.commit()
            counts['duplicate_macro_deleted'] = r3.rowcount

            # ── Pass 4: stale refresh tokens ──────────────────────────────
            # Tokens expired >7 days ago are permanently dead.
            # Revoked tokens >30 days old are audit-safe to remove.
            try:
                r4 = conn.execute(
                    """DELETE FROM refresh_tokens
                       WHERE expires_at < datetime('now', '-7 days')
                       OR (revoked = 1 AND issued_at < datetime('now', '-30 days'))"""
                )
                conn.commit()
                counts['refresh_tokens_deleted'] = r4.rowcount
            except Exception as e:
                _logger.debug('[kb_cleanup] refresh_tokens pass skipped: %s', e)
                counts['refresh_tokens_deleted'] = 0

            # ── Pass 5: prediction ledger bulk resolution ────────────────────
            pl_resolved = 0
            try:
                unresolved = conn.execute(
                    """SELECT DISTINCT LOWER(pl.ticker) as t, f.object as price
                       FROM prediction_ledger pl
                       JOIN facts f ON LOWER(f.subject) = LOWER(pl.ticker)
                           AND f.predicate = 'last_price'
                       WHERE pl.outcome IS NULL"""
                ).fetchall()
                if unresolved:
                    from analytics.prediction_ledger import PredictionLedger
                    _pl = PredictionLedger(self._db)
                    for ticker_l, price_str in unresolved:
                        try:
                            price = float(str(price_str).split()[0].replace(',', ''))
                            _pl.on_price_written(ticker_l, price)
                            pl_resolved += 1
                        except Exception:
                            pass
                conn.execute(
                    """UPDATE prediction_ledger SET outcome='expired', resolved_at=datetime('now')
                       WHERE outcome IS NULL AND expires_at < datetime('now')"""
                )
                conn.commit()
            except Exception as e:
                _logger.debug('[kb_cleanup] prediction ledger pass skipped: %s', e)
            counts['prediction_ledger_swept'] = pl_resolved

            after = conn.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
            counts['facts_before'] = before
            counts['facts_after'] = after
            counts['total_deleted'] = before - after

            _logger.info(
                '[kb_cleanup] %d → %d facts (-%d): '
                'decay=%d, theses=%d, dupes=%d | refresh_tokens=%d | pl_swept=%d',
                before, after, before - after,
                counts['decay_facts_deleted'],
                counts['old_theses_deleted'],
                counts['duplicate_macro_deleted'],
                counts['refresh_tokens_deleted'],
                counts['prediction_ledger_swept'],
            )

        finally:
            # VACUUM reclaims disk space — run outside transaction
            try:
                conn.execute('VACUUM')
            except Exception as e:
                _logger.warning('[kb_cleanup] VACUUM failed: %s', e)
            conn.close()

        return counts

    def transform(self, raw: dict) -> List[RawAtom]:
        """Emit a single audit atom recording the cleanup run."""
        if not raw or raw.get('total_deleted', 0) == 0:
            return []
        now = datetime.now(timezone.utc).isoformat()
        return [RawAtom(
            subject    = 'system',
            predicate  = 'kb_cleanup_last_run',
            object     = now,
            confidence = 1.0,
            source     = 'kb_cleanup_adapter',
            metadata   = {
                'facts_before':            str(raw.get('facts_before', 0)),
                'facts_after':             str(raw.get('facts_after', 0)),
                'total_deleted':           str(raw.get('total_deleted', 0)),
                'decay_facts_deleted':     str(raw.get('decay_facts_deleted', 0)),
                'old_theses_deleted':      str(raw.get('old_theses_deleted', 0)),
                'duplicate_macro_deleted': str(raw.get('duplicate_macro_deleted', 0)),
                'refresh_tokens_deleted':  str(raw.get('refresh_tokens_deleted', 0)),
                'prediction_ledger_swept': str(raw.get('prediction_ledger_swept', 0)),
            },
            upsert = True,
        )]
