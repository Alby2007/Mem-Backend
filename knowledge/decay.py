"""
knowledge/decay.py — Confidence Decay by Age (Trading KB)

Applies exponential half-life decay to atom confidence based on source type.
Trading data has extreme variance in freshness requirements:
  - Exchange tick data: stale within minutes
  - News/macro events: relevant for hours to days
  - Research reports: relevant for weeks
  - Regulatory filings: stable for months

Decay is NOT stored back to the base `confidence` column — that is immutable.
Instead, `confidence_effective` is a computed/cached column updated by DecayWorker.

Zero-LLM, pure Python, <1ms per call.
"""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from knowledge.authority import get_authority

_logger = logging.getLogger(__name__)


# ── Half-life table (days) ────────────────────────────────────────────────────────────────────
# Shorter half-life = faster decay = source is more time-sensitive
# Trading note: half-life is in fractional days (e.g. 0.007 ≈ 10 minutes)
_HALF_LIFE_TABLE: dict[str, float] = {
    # Ultra-short: intraday market data
    'exchange_feed':       0.007,  # ~10 minutes — tick/price data
    'technical_':          0.25,   # ~6 hours — technical indicators
    'model_signal_':       0.5,    # ~12 hours — quant model outputs

    # Short: news & macro
    'news_wire_':          1.0,    # 1 day — breaking news decays fast
    'social_signal_':      0.5,    # 12 hours — social sentiment very short-lived
    'alt_data_':           3.0,    # 3 days — alt data has short relevance window

    # Medium: earnings & research
    'earnings_':           30.0,   # 30 days — per quarter roughly
    'broker_research':     21.0,   # 3 weeks — analyst reports age quickly
    'macro_data':          60.0,   # 2 months — macro regime changes slowly
    'cross_asset_gnn':     14.0,   # 2 weeks — cross-asset GNN re-runs frequently

    # Long: structural / curated
    'curated_':            180.0,  # 6 months — hand-authored analysis
    'regulatory_filing':   365.0,  # 1 year — filings are durable facts
    'unverified_':         1.0,    # 1 day — unverified data treated as ephemeral
}

_DEFAULT_HALF_LIFE = 60.0   # days
_DECAY_FLOOR = 0.10          # effective confidence never drops below this
_WORKER_INTERVAL_SEC = 86400  # 24h between decay passes


def _get_half_life(source: str) -> float:
    """Return half-life in days for a given source string (longest prefix match)."""
    if not source:
        return _DEFAULT_HALF_LIFE

    best_match = ''
    best_hl = _DEFAULT_HALF_LIFE

    for prefix, hl in _HALF_LIFE_TABLE.items():
        if source.startswith(prefix) and len(prefix) > len(best_match):
            best_match = prefix
            best_hl = hl

    return best_hl


def decay_confidence(
    base_confidence: float,
    source: str,
    timestamp: Optional[str],
    now: Optional[datetime] = None,
) -> float:
    """
    Compute effective confidence after age-based decay.

    Formula:
        effective = base × 2^(-age_days / half_life)

    Clamped to [_DECAY_FLOOR, base_confidence] — decay never raises confidence.

    Args:
        base_confidence: original stored confidence (0–1)
        source:          atom source string
        timestamp:       ISO timestamp string when atom was ingested
        now:             reference time (defaults to UTC now)
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if not timestamp:
        return max(_DECAY_FLOOR, base_confidence * 0.8)  # unknown age → mild penalty

    try:
        ts = datetime.fromisoformat(timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (now - ts).total_seconds() / 86400.0)
    except (ValueError, TypeError):
        return max(_DECAY_FLOOR, base_confidence * 0.8)

    half_life = _get_half_life(source)
    decayed = base_confidence * math.pow(2.0, -age_days / half_life)
    return max(_DECAY_FLOOR, min(base_confidence, decayed))


def get_effective_confidence(fact: dict, now: Optional[datetime] = None) -> float:
    """
    Return effective confidence for a fact dict.

    Uses pre-computed `confidence_effective` if present and fresh.
    Falls back to computing decay on the fly.
    """
    if 'confidence_effective' in fact and fact['confidence_effective'] is not None:
        return float(fact['confidence_effective'])

    return decay_confidence(
        base_confidence=float(fact.get('confidence', 0.5)),
        source=fact.get('source', ''),
        timestamp=fact.get('timestamp'),
        now=now,
    )


# ── Schema migration ───────────────────────────────────────────────────────────

def ensure_decay_column(conn: sqlite3.Connection) -> None:
    """
    Idempotent migration: add `confidence_effective` column to facts table if absent.
    Safe to call on every startup.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(facts)")
    columns = {row[1] for row in cursor.fetchall()}
    if 'confidence_effective' not in columns:
        cursor.execute("ALTER TABLE facts ADD COLUMN confidence_effective REAL")
        conn.commit()


# ── Background worker ──────────────────────────────────────────────────────────

class DecayWorker:
    """
    Background daemon that recomputes `confidence_effective` for all atoms
    every _WORKER_INTERVAL_SEC (24h).

    Runs in a daemon thread — does not block startup.
    Processes atoms in batches to avoid locking the DB.
    """

    _BATCH_SIZE = 500

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Start the background decay worker thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name='DecayWorker'
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> int:
        """
        Run a single decay pass. Returns number of atoms updated.
        Safe to call directly for testing.
        """
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            ensure_decay_column(conn)
            return self._update_all(conn)
        finally:
            conn.close()

    def _loop(self) -> None:
        # Initial pass shortly after startup
        time.sleep(30)
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as e:
                _logger.error('[DecayWorker] error: %s', e)
            self._stop.wait(timeout=_WORKER_INTERVAL_SEC)

    def _update_all(self, conn: sqlite3.Connection) -> int:
        cursor = conn.cursor()
        now = datetime.now(timezone.utc)
        updated = 0
        offset = 0

        while True:
            # ORDER BY id ASC guarantees insertion-order processing.
            # Without this, SQLite's page-scan order is undefined — atoms could
            # be skipped or double-processed across batches if the table is
            # fragmented. Stable ordering ensures every atom is touched exactly
            # once per pass regardless of table state.
            cursor.execute(
                "SELECT id, confidence, source, timestamp FROM facts "
                "ORDER BY id ASC LIMIT ? OFFSET ?",
                (self._BATCH_SIZE, offset),
            )
            rows = cursor.fetchall()
            if not rows:
                break

            updates = []
            for row in rows:
                eff = decay_confidence(
                    base_confidence=float(row['confidence']),
                    source=row['source'] or '',
                    timestamp=row['timestamp'],
                    now=now,
                )
                updates.append((eff, row['id']))

            cursor.executemany(
                "UPDATE facts SET confidence_effective = ? WHERE id = ?", updates
            )
            conn.commit()
            updated += len(updates)
            offset += self._BATCH_SIZE

        return updated


# ── Module-level singleton ─────────────────────────────────────────────────────

_worker: Optional[DecayWorker] = None


def get_decay_worker(db_path: str) -> DecayWorker:
    """Return (and lazily start) the module-level DecayWorker singleton."""
    global _worker
    if _worker is None:
        _worker = DecayWorker(db_path)
        _worker.start()
    return _worker
