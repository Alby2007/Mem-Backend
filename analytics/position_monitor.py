"""
analytics/position_monitor.py — Background Position Monitor

Watches every tip-originated open position every 5 minutes.
Checks trigger conditions in priority order (CRITICAL → HIGH → MEDIUM → LOW)
and fires alerts via Telegram and chat-surface queuing.

Rate limits per position:
  CRITICAL: no limit
  HIGH:     max 1 alert per 4 hours
  MEDIUM:   max 1 alert per 24 hours
  LOW:      batched into morning tip only (not fired here)

Alert queue:
  Alerts are written to the `position_alerts` table.
  The /chat endpoint polls this table at session open to surface pending alerts.
  Telegram sends immediately if the user has a telegram_chat_id configured.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

_log = logging.getLogger(__name__)

_CRITICAL_COOLDOWN_H  = 0        # no cooldown
_HIGH_COOLDOWN_H      = 4
_MEDIUM_COOLDOWN_H    = 24

_DDL_POSITION_ALERTS = """
CREATE TABLE IF NOT EXISTS position_alerts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    followup_id    INTEGER NOT NULL,
    user_id        TEXT    NOT NULL,
    ticker         TEXT    NOT NULL,
    alert_type     TEXT    NOT NULL,
    priority       TEXT    NOT NULL,
    current_price  REAL,
    entry_price    REAL,
    pnl_pct        REAL,
    message        TEXT,
    surfaced_chat  INTEGER DEFAULT 0,
    surfaced_tg    INTEGER DEFAULT 0,
    created_at     TEXT    NOT NULL
)
"""

_DDL_POSITION_ALERTS_IDX = """
CREATE INDEX IF NOT EXISTS idx_position_alerts_user_unsurfaced
ON position_alerts(user_id, surfaced_chat)
"""


def _ensure_alerts_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_POSITION_ALERTS)
    conn.execute(_DDL_POSITION_ALERTS_IDX)
    conn.commit()


def _get_latest_price(db_path: str, ticker: str) -> Optional[float]:
    """Read last_price atom from KB facts table."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            """SELECT object FROM facts
               WHERE subject = ? AND predicate = 'last_price'
               ORDER BY rowid DESC LIMIT 1""",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _get_kb_atom(db_path: str, ticker: str, predicate: str) -> Optional[str]:
    """Read a single KB atom value for a ticker."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            """SELECT object FROM facts
               WHERE subject = ? AND predicate = ?
               ORDER BY rowid DESC LIMIT 1""",
            (ticker.lower(), predicate),
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _cooldown_ok(last_alert_at: Optional[str], cooldown_hours: int) -> bool:
    """Return True if enough time has passed since the last alert of this level."""
    if cooldown_hours == 0:
        return True
    if not last_alert_at:
        return True
    try:
        last = datetime.fromisoformat(last_alert_at.replace('Z', '+00:00'))
        return (datetime.now(timezone.utc) - last) >= timedelta(hours=cooldown_hours)
    except Exception:
        return True


def _pnl_pct(current: float, entry: float, direction: str) -> float:
    if not entry or entry == 0:
        return 0.0
    raw = (current - entry) / entry * 100
    return raw if direction != 'bearish' else -raw


def _check_triggers(pos: dict, price: float, db_path: str) -> Optional[tuple]:
    """
    Evaluate trigger conditions for a position.
    Returns (alert_type, priority) or None.
    Priority: CRITICAL > HIGH > MEDIUM
    """
    ticker     = pos['ticker']
    direction  = pos.get('direction', 'bullish')
    entry      = pos.get('entry_price') or 0.0
    stop       = pos.get('stop_loss')
    t1         = pos.get('target_1')
    t2         = pos.get('target_2')
    t3         = pos.get('target_3')
    tracking   = pos.get('tracking_target', 'T1')
    last_alert = pos.get('last_alert_at')
    alert_level = pos.get('alert_level', '')
    bullish    = direction != 'bearish'

    # ── CRITICAL ──────────────────────────────────────────────────────────────
    if stop is not None:
        at_stop = (bullish and price <= stop * 1.005) or (not bullish and price >= stop * 0.995)
        if at_stop and _cooldown_ok(last_alert if alert_level == 'CRITICAL' else None, _CRITICAL_COOLDOWN_H):
            return ('stop_loss_zone_reached', 'CRITICAL')

    # Pattern invalidated: opposing conviction_tier or signal_direction atom
    sig_dir = _get_kb_atom(db_path, ticker, 'signal_direction')
    if sig_dir:
        contradicted = (bullish and sig_dir in ('short', 'bearish', 'sell')) or \
                       (not bullish and sig_dir in ('long', 'bullish', 'buy'))
        if contradicted:
            return ('pattern_invalidated', 'CRITICAL')

    # Earnings within 2 days
    earnings_flag = _get_kb_atom(db_path, ticker, 'pre_earnings_flag')
    if earnings_flag == 'imminent':
        return ('earnings_within_2_days', 'CRITICAL')

    # ── HIGH ─────────────────────────────────────────────────────────────────
    if _cooldown_ok(last_alert if alert_level == 'HIGH' else None, _HIGH_COOLDOWN_H):

        if t1 is not None and tracking == 'T1':
            at_t1 = (bullish and price >= t1 * 0.995) or (not bullish and price <= t1 * 1.005)
            if at_t1:
                return ('t1_zone_reached', 'HIGH')

        # Conviction tier dropped
        current_conv = _get_kb_atom(db_path, ticker, 'conviction_tier')
        entry_conv   = pos.get('conviction_at_entry', '')
        tier_order   = {'high': 3, 'medium': 2, 'low': 1, 'avoid': 0}
        if current_conv and entry_conv:
            if tier_order.get(current_conv, 2) < tier_order.get(entry_conv, 2):
                return ('conviction_tier_dropped', 'HIGH')

        # Regime shift
        current_regime = _get_kb_atom(db_path, ticker, 'price_regime') or \
                         _get_kb_atom(db_path, ticker, 'macro_regime')
        entry_regime   = pos.get('regime_at_entry', '')
        if current_regime and entry_regime and current_regime != entry_regime:
            return ('regime_shift_detected', 'HIGH')

    # ── MEDIUM ───────────────────────────────────────────────────────────────
    if _cooldown_ok(last_alert if alert_level == 'MEDIUM' else None, _MEDIUM_COOLDOWN_H):

        if t2 is not None and tracking in ('T1', 'T2'):
            at_t2 = (bullish and price >= t2 * 0.995) or (not bullish and price <= t2 * 1.005)
            if at_t2:
                return ('t2_zone_reached', 'MEDIUM')

        sector_tail = _get_kb_atom(db_path, ticker, 'sector_tailwind')
        if sector_tail in ('negative', 'bearish', 'headwind'):
            if bullish:
                return ('sector_tailwind_reversed', 'MEDIUM')

        squeeze = _get_kb_atom(db_path, ticker, 'short_interest_tension')
        if squeeze == 'building':
            return ('short_squeeze_developing', 'MEDIUM')

    return None


def _write_alert(
    db_path: str,
    pos: dict,
    alert_type: str,
    priority: str,
    price: float,
) -> int:
    """Write a position alert to the DB. Returns the alert row id."""
    entry   = pos.get('entry_price') or 0.0
    pnl     = _pnl_pct(price, entry, pos.get('direction', 'bullish'))
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_alerts_table(conn)
        cur = conn.execute(
            """INSERT INTO position_alerts
               (followup_id, user_id, ticker, alert_type, priority,
                current_price, entry_price, pnl_pct, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (pos['id'], pos['user_id'], pos['ticker'], alert_type, priority,
             price, entry, round(pnl, 2), now_iso),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _send_telegram_alert(db_path: str, user_id: str, alert_id: int,
                          pos: dict, alert_type: str, price: float) -> None:
    """Format and send a Telegram position update alert."""
    try:
        import sqlite3 as _sql
        c = _sql.connect(db_path, timeout=5)
        row = c.execute(
            "SELECT telegram_chat_id FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        c.close()
        if not row or not row[0]:
            return
        chat_id = row[0]

        from notifications.tip_formatter import format_position_update
        msg = format_position_update(alert_type, pos, price)

        from notifications.telegram_notifier import TelegramNotifier
        sent = TelegramNotifier().send(chat_id, msg,
                                       reply_markup=_position_keyboard(pos['id'], alert_type))
        if sent:
            c2 = _sql.connect(db_path, timeout=5)
            c2.execute("UPDATE position_alerts SET surfaced_tg=1 WHERE id=?", (alert_id,))
            c2.commit()
            c2.close()
    except Exception as e:
        _log.debug('PositionMonitor: telegram alert failed for %s: %s', user_id, e)


def _position_keyboard(followup_id: int, alert_type: str) -> dict:
    """Build Telegram inline keyboard for a position update."""
    base = f'pos:{followup_id}'
    if 't1_zone' in alert_type:
        buttons = [
            [{'text': '✅ Closed position', 'callback_data': f'{base}:closed'},
             {'text': '📈 Holding for T2', 'callback_data': f'{base}:hold_t2'}],
            [{'text': '⚡ Partial exit',    'callback_data': f'{base}:partial'}],
        ]
    elif 'stop' in alert_type:
        buttons = [
            [{'text': '✅ Closed position',  'callback_data': f'{base}:closed'},
             {'text': '⏳ Watching — not closing', 'callback_data': f'{base}:override'}],
            [{'text': '📊 Tell me more',     'callback_data': f'{base}:more'}],
        ]
    else:
        buttons = [
            [{'text': '✅ Closed position',  'callback_data': f'{base}:closed'}],
            [{'text': '📊 Tell me more',     'callback_data': f'{base}:more'}],
        ]
    return {'inline_keyboard': buttons}


def _run_monitor_cycle(db_path: str) -> None:
    """One cycle: load all watching positions, check triggers, fire alerts."""
    from users.user_store import get_watching_followups, update_followup_status

    try:
        positions = get_watching_followups(db_path)
    except Exception as e:
        _log.error('PositionMonitor: failed to load followups: %s', e)
        return

    for pos in positions:
        ticker = pos['ticker']
        try:
            price = _get_latest_price(db_path, ticker)
            if price is None:
                continue

            result = _check_triggers(pos, price, db_path)
            if result is None:
                continue

            alert_type, priority = result
            _log.info('PositionMonitor: %s %s → %s (%s) price=%.4f',
                      pos['user_id'], ticker, alert_type, priority, price)

            alert_id = _write_alert(db_path, pos, alert_type, priority, price)

            update_followup_status(
                db_path, pos['id'],
                status=pos['status'],
                alert_level=priority,
            )

            _send_telegram_alert(db_path, pos['user_id'], alert_id, pos, alert_type, price)

        except Exception as e:
            _log.error('PositionMonitor: error processing followup %d (%s): %s',
                       pos['id'], ticker, e)


class PositionMonitor:
    """
    Background thread that monitors all tip-originated open positions.

    Parameters
    ----------
    db_path      Path to the SQLite knowledge base file.
    interval_sec Check interval in seconds. Default 300 (5 minutes).
    """

    def __init__(self, db_path: str, interval_sec: int = 300):
        self.db_path      = db_path
        self.interval_sec = interval_sec
        self._stop_event  = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name='position-monitor',
            daemon=True,
        )
        self._thread.start()
        _log.info('PositionMonitor: started (interval=%ds)', self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        _log.info('PositionMonitor: stop requested')

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                _run_monitor_cycle(self.db_path)
            except Exception as exc:
                _log.error('PositionMonitor: cycle error: %s', exc)
            self._stop_event.wait(self.interval_sec)


def get_pending_alerts(db_path: str, user_id: str) -> list:
    """Return unsurfaced chat alerts for a user. Called by /chat at session open."""
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        _ensure_alerts_table(conn)
        rows = conn.execute(
            """SELECT id, followup_id, ticker, alert_type, priority,
                      current_price, entry_price, pnl_pct, created_at
               FROM position_alerts
               WHERE user_id = ? AND surfaced_chat = 0
               ORDER BY created_at DESC""",
            (user_id,),
        ).fetchall()
        cols = ['id', 'followup_id', 'ticker', 'alert_type', 'priority',
                'current_price', 'entry_price', 'pnl_pct', 'created_at']
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def mark_alerts_surfaced(db_path: str, alert_ids: list) -> None:
    """Mark chat alerts as surfaced so they don't re-appear."""
    if not alert_ids:
        return
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        _ensure_alerts_table(conn)
        conn.execute(
            f"UPDATE position_alerts SET surfaced_chat=1 WHERE id IN ({','.join('?'*len(alert_ids))})",
            alert_ids,
        )
        conn.commit()
    finally:
        conn.close()
