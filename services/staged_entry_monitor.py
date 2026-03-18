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
    """Read latest close from ohlcv_cache (15m bars, updated every 300s).
    Falls back to last_price KB atom if no cache entry exists."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        # Primary: 15m ohlcv_cache — covers all 534 tickers, max 15min stale
        row = conn.execute(
            """SELECT close FROM ohlcv_cache
               WHERE ticker = ? AND interval = '15m'
               ORDER BY ts DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
        if not row:
            # Fallback: try uppercase ticker (some tickers stored differently)
            row = conn.execute(
                """SELECT close FROM ohlcv_cache
                   WHERE UPPER(ticker) = UPPER(?) AND interval = '15m'
                   ORDER BY ts DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        if not row:
            # Last resort: KB last_price atom (may be up to 1800s stale for non-US)
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


def _get_user_for_followup(db_path: str, followup_id: int):
    """Return (user_id, telegram_chat_id) for the followup's owner."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute("""
            SELECT tf.user_id, up.telegram_chat_id
            FROM tip_followups tf
            JOIN user_preferences up ON up.user_id = tf.user_id
            WHERE tf.id = ?
        """, (followup_id,)).fetchone()
        conn.close()
        return (row[0], row[1]) if row else (None, None)
    except Exception:
        return (None, None)


def _send_entry_alert(db_path: str, followup_id: int, ticker: str,
                      direction: str, live_price: float, target_entry: float) -> None:
    """
    Send Telegram inline-keyboard alert when entry price is hit.
    Sets status = 'pending_confirmation'. Reverts to 'staged' after 30min.
    Falls back to silent advance if user has no Telegram connected.
    """
    user_id, chat_id = _get_user_for_followup(db_path, followup_id)
    if not chat_id:
        _advance_to_active_silent(db_path, followup_id, ticker, live_price)
        return

    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        row = dict(conn.execute("SELECT * FROM tip_followups WHERE id=?", (followup_id,)).fetchone())
        conn.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: fetch failed %d: %s", followup_id, e)
        return

    dir_emoji = "📈" if direction == "bullish" else "📉"
    dir_label = "LONG" if direction == "bullish" else "SHORT"
    stop    = row.get("stop_loss")
    t1      = row.get("target_1")
    t2      = row.get("target_2")
    size    = row.get("position_size")
    pattern = (row.get("pattern_type") or "").replace("_", " ").upper()
    tf      = (row.get("timeframe") or "").upper()
    conv    = (row.get("conviction_at_entry") or "").upper()

    def esc(s):
        special = r'\_*[]()~`>#+-=|{}.!'
        return ''.join(('\\' + c if c in special else c) for c in str(s))

    lines = [
        f"{dir_emoji} *ENTRY ALERT — {esc(ticker)}*",
        f"_{esc(pattern)} {esc(tf)} · {esc(conv)} conviction_",
        "",
        f"Price hit: `{live_price:.4f}`",
        f"Direction: *{dir_label}*",
    ]
    if stop:  lines.append(f"Stop: `{float(stop):.4f}`")
    if t1:    lines.append(f"T1: `{float(t1):.4f}`")
    if t2:    lines.append(f"T2: `{float(t2):.4f}`")
    if size:  lines.append(f"Size: `£{float(size):,.0f}`")
    lines += ["", "_Execute this trade?_"]
    message = "\n".join(lines)

    keyboard = {"inline_keyboard": [[
        {"text": "✅ YES — Execute", "callback_data": f"exec:{followup_id}:yes"},
        {"text": "❌ Skip",           "callback_data": f"exec:{followup_id}:skip"},
        {"text": "⏰ +1h",            "callback_data": f"exec:{followup_id}:delay"},
    ]]}

    import os
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if token:
        try:
            import requests as _rq
            _rq.post(f"https://api.telegram.org/bot{token}/sendMessage", json={
                "chat_id":      chat_id,
                "text":         message,
                "parse_mode":   "MarkdownV2",
                "reply_markup": keyboard,
            }, timeout=10)
        except Exception as e:
            _log.warning("StagedEntryMonitor: Telegram send failed: %s", e)

    # Mark pending_confirmation — blocks re-alert until user responds or 30min elapses
    try:
        conn2 = sqlite3.connect(db_path, timeout=10)
        conn2.execute(
            "UPDATE tip_followups SET status='pending_confirmation', updated_at=? WHERE id=? AND status='staged'",
            (datetime.now(timezone.utc).isoformat(), followup_id),
        )
        conn2.commit()
        conn2.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: status update failed: %s", e)


def _advance_to_active_silent(db_path: str, followup_id: int,
                               ticker: str, price: float) -> None:
    """Silent advance — used when no Telegram connected."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute(
            """UPDATE tip_followups
               SET status='active', entry_price=COALESCE(entry_price,?),
                   opened_at=COALESCE(opened_at,?), initiated_by='system'
               WHERE id=? AND status='staged'""",
            (price, datetime.now(timezone.utc).isoformat(), followup_id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: silent advance failed %d: %s", followup_id, e)


def _expire_pending_confirmations(db_path: str) -> None:
    """Revert pending_confirmation → staged if 30min has elapsed with no response."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute(
            """UPDATE tip_followups
               SET status='staged', updated_at=?
               WHERE status='pending_confirmation'
                 AND updated_at < datetime('now', '-30 minutes')""",
            (datetime.now(timezone.utc).isoformat(),),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: expire pending failed: %s", e)


def _run_cycle(db_path: str) -> None:
    _expire_pending_confirmations(db_path)
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, ticker, direction, target_entry
               FROM tip_followups
               WHERE status = 'staged'
                 AND target_entry IS NOT NULL AND target_entry > 0"""
        ).fetchall()
        conn.close()
    except Exception as e:
        _log.warning("StagedEntryMonitor: DB read error: %s", e)
        return

    for row in rows:
        followup_id  = row["id"]
        ticker       = row["ticker"]
        target_entry = float(row["target_entry"])
        direction    = (row["direction"] or "bullish").lower()
        live_price   = _get_latest_price(db_path, ticker)
        if live_price is None:
            continue
        hit = (live_price <= target_entry if direction == "bearish"
               else live_price >= target_entry)
        if hit:
            _log.info("StagedEntryMonitor: ENTRY HIT %s %s target=%.4f live=%.4f (id=%d)",
                      ticker, direction, target_entry, live_price, followup_id)
            _send_entry_alert(db_path, followup_id, ticker, direction, live_price, target_entry)


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
