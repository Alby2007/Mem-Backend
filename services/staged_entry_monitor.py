"""services/staged_entry_monitor.py — Staged → Active auto-advance monitor.

Polls tip_followups WHERE status='staged' every 60 seconds.
For each item with a target_entry, reads the last_price KB atom.
If price crosses target_entry (direction-aware), advances status to 'active'.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)


def _get_latest_price(db_path: str, ticker: str) -> Optional[float]:
    """Read last_price atom from KB facts table."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            """SELECT object FROM facts
               WHERE LOWER(subject) = ? AND predicate = 'last_price'
               ORDER BY rowid DESC LIMIT 1""",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _advance_to_active(db_path: str, followup_id: int, ticker: str, price: float) -> None:
    """Directly advance a staged item to active in tip_followups."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute(
            """UPDATE tip_followups
               SET status = 'active',
                   entry_price = COALESCE(entry_price, ?),
                   opened_at = COALESCE(opened_at, ?)
               WHERE id = ? AND status = 'staged'""",
            (price, datetime.now(timezone.utc).isoformat(), followup_id),
        )
        conn.commit()
        conn.close()
        _log.info(
            "StagedEntryMonitor: advanced followup_id=%d (%s) to ACTIVE at price=%.4f",
            followup_id, ticker, price,
        )
    except Exception as e:
        _log.warning("StagedEntryMonitor: failed to advance %d: %s", followup_id, e)


def _run_cycle(db_path: str) -> None:
    """One poll cycle — check all staged items and advance those whose price is hit."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, ticker, direction, target_entry
               FROM tip_followups
               WHERE status = 'staged'
                 AND target_entry IS NOT NULL
                 AND target_entry > 0"""
        ).fetchall()
        conn.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: DB read error: %s", e)
        return

    if not rows:
        return

    for row in rows:
        followup_id = row["id"]
        ticker = row["ticker"]
        target_entry = float(row["target_entry"])
        direction = (row["direction"] or "bullish").lower()

        live_price = _get_latest_price(db_path, ticker)
        if live_price is None:
            continue

        hit = False
        if direction == "bearish":
            # Enter short when price falls to or below target entry
            hit = live_price <= target_entry
        else:
            # Enter long when price rises to or above target entry
            hit = live_price >= target_entry

        if hit:
            _log.info(
                "StagedEntryMonitor: ENTRY HIT — %s %s target=%.4f live=%.4f (id=%d)",
                ticker, direction, target_entry, live_price, followup_id,
            )
            _advance_to_active(db_path, followup_id, ticker, live_price)


class StagedEntryMonitor:
    """Background thread that polls staged pipeline items and auto-advances them."""

    def __init__(self, db_path: str, interval_sec: int = 60) -> None:
        self._db_path = db_path
        self._interval_sec = interval_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="staged-entry-monitor",
            daemon=True,
        )
        self._thread.start()
        _log.info("StagedEntryMonitor started (interval=%ds)", self._interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                _run_cycle(self._db_path)
            except Exception as e:
                _log.error("StagedEntryMonitor: cycle error: %s", e)
            self._stop_event.wait(self._interval_sec)
