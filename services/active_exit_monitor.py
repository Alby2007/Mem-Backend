"""services/active_exit_monitor.py — Active → Assessing auto-advance monitor.

Polls tip_followups WHERE status='active' every 60 seconds.
For each item checks live price against target_exit (or target_1/target_2) and stop_loss.
- Price hits target_exit or target_1 → status='hit_t1', assessing
- Price hits target_2          → status='hit_t2', assessing
- Price hits stop_loss         → status='stopped_out', assessing
All transitions record exit_price, closed_at, and r_multiple.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
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


def _calc_r_multiple(entry: Optional[float], stop: Optional[float], exit_p: float) -> Optional[float]:
    if not entry or not stop:
        return None
    risk = abs(entry - stop)
    if risk == 0:
        return None
    return round((exit_p - entry) / risk, 2)


def _advance_to_assessing(
    db_path: str,
    followup_id: int,
    ticker: str,
    outcome_status: str,
    exit_price: float,
    r_multiple: Optional[float],
) -> None:
    """Write assessing transition directly to tip_followups."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        now = datetime.now(timezone.utc).isoformat()
        params = [outcome_status, exit_price, now]
        r_clause = ""
        if r_multiple is not None:
            r_clause = ", r_multiple = ?"
            params.append(r_multiple)
        params.append(followup_id)
        conn.execute(
            f"""UPDATE tip_followups
               SET status = ?,
                   exit_price = ?,
                   closed_at = ?,
                   initiated_by = 'system'
                   {r_clause}
               WHERE id = ? AND status = 'active'""",
            params,
        )
        conn.commit()
        conn.close()
        _log.info(
            "ActiveExitMonitor: advanced followup_id=%d (%s) → %s at exit=%.4f R=%.2f",
            followup_id, ticker, outcome_status, exit_price,
            r_multiple if r_multiple is not None else 0,
        )
    except Exception as e:
        _log.warning("ActiveExitMonitor: failed to advance %d: %s", followup_id, e)


def _run_cycle(db_path: str) -> None:
    """One poll cycle — check all active items and advance those whose exit/stop is hit."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, ticker, direction, entry_price, stop_loss,
                      target_exit, target_1, target_2
               FROM tip_followups
               WHERE status = 'active'"""
        ).fetchall()
        conn.close()
    except Exception as e:
        _log.warning("ActiveExitMonitor: DB read error: %s", e)
        return

    if not rows:
        return

    for row in rows:
        followup_id  = row["id"]
        ticker       = row["ticker"]
        direction    = (row["direction"] or "bullish").lower()
        entry        = row["entry_price"]
        stop         = row["stop_loss"]
        target_exit  = row["target_exit"]
        target_1     = row["target_1"]
        target_2     = row["target_2"]

        live = _get_latest_price(db_path, ticker)
        if live is None:
            continue

        is_bear = direction == "bearish"

        # ── Stop-loss check (highest priority — overrides profit targets) ──────
        if stop and stop > 0:
            stopped = (live <= stop) if not is_bear else (live >= stop)
            if stopped:
                r = _calc_r_multiple(entry, stop, live)
                _log.info(
                    "ActiveExitMonitor: STOP HIT — %s %s stop=%.4f live=%.4f (id=%d)",
                    ticker, direction, stop, live, followup_id,
                )
                _advance_to_assessing(db_path, followup_id, ticker, "stopped_out", live, r)
                continue

        # ── Target 2 check ────────────────────────────────────────────────────
        if target_2 and target_2 > 0:
            hit_t2 = (live >= target_2) if not is_bear else (live <= target_2)
            if hit_t2:
                r = _calc_r_multiple(entry, stop, live)
                _log.info(
                    "ActiveExitMonitor: T2 HIT — %s %s t2=%.4f live=%.4f (id=%d)",
                    ticker, direction, target_2, live, followup_id,
                )
                _advance_to_assessing(db_path, followup_id, ticker, "hit_t2", live, r)
                continue

        # ── Target exit / Target 1 check ─────────────────────────────────────
        exit_target = target_exit if (target_exit and target_exit > 0) else target_1
        if exit_target and exit_target > 0:
            hit_t1 = (live >= exit_target) if not is_bear else (live <= exit_target)
            if hit_t1:
                r = _calc_r_multiple(entry, stop, live)
                _log.info(
                    "ActiveExitMonitor: T1/EXIT HIT — %s %s target=%.4f live=%.4f (id=%d)",
                    ticker, direction, exit_target, live, followup_id,
                )
                _advance_to_assessing(db_path, followup_id, ticker, "hit_t1", live, r)


class ActiveExitMonitor:
    """Background thread that polls active pipeline items and auto-advances them to assessing."""

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
            name="active-exit-monitor",
            daemon=True,
        )
        self._thread.start()
        _log.info("ActiveExitMonitor started (interval=%ds)", self._interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                _run_cycle(self._db_path)
            except Exception as e:
                _log.error("ActiveExitMonitor: cycle error: %s", e)
            self._stop_event.wait(self._interval_sec)
