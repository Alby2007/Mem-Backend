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


_CONFIDENCE_PREDICATES = (
    'conviction_tier', 'signal_direction', 'sector_tailwind', 'macro_signal',
    'price_regime', 'macro_regime', 'market_regime', 'volatility_regime',
    'uk_market_regime', 'flow_conviction', 'smart_money_signal',
    'macro_event_risk', 'regime_label',
)


def _compute_confidence(db_path: str, ticker: str, direction: str) -> Optional[float]:
    """
    Normalised confidence score: confirming_atoms / max(total_relevant_atoms, 1).
    Returns None if fewer than 2 relevant atoms found (avoids false precision on
    thinly-covered tickers).
    """
    bullish = direction != 'bearish'
    confirming = 0
    total = 0
    for pred in _CONFIDENCE_PREDICATES:
        val = _get_kb_atom(db_path, ticker, pred)
        if val is None:
            continue
        total += 1
        v = val.lower()
        if pred == 'conviction_tier' and v in ('high', 'strong', 'confirmed'):
            confirming += 1
        elif pred == 'signal_direction':
            if bullish and v in ('long', 'bullish', 'buy'):
                confirming += 1
            elif not bullish and v in ('short', 'bearish', 'sell'):
                confirming += 1
        elif pred == 'sector_tailwind':
            if bullish and v in ('positive', 'bullish', 'tailwind'):
                confirming += 1
            elif not bullish and v in ('negative', 'bearish', 'headwind'):
                confirming += 1
        elif pred == 'macro_signal':
            if bullish and v in ('risk_on', 'bullish', 'positive'):
                confirming += 1
            elif not bullish and v in ('risk_off', 'bearish', 'negative'):
                confirming += 1
    if total < 2:
        return None
    return confirming / total


def _is_market_hours() -> bool:
    """Return True if current UTC time is within approximate market hours (08:00-16:30 UTC)."""
    now = datetime.now(timezone.utc)
    return now.weekday() < 5 and (8, 0) <= (now.hour, now.minute) <= (16, 30)


def _is_actionable_hours() -> bool:
    """
    Return True if it is a reasonable time to fire HIGH/MEDIUM alerts.
    Window: Mon-Fri 06:30-18:30 UTC (covers pre-market + 2h after close).
    CRITICAL alerts bypass this gate and fire any time Mon-Fri.
    """
    now = datetime.now(timezone.utc)
    return now.weekday() < 5 and (6, 30) <= (now.hour, now.minute) <= (18, 30)


def _check_triggers(pos: dict, price: float, db_path: str) -> Optional[tuple]:
    """
    Evaluate trigger conditions for a position.
    Returns (alert_type, priority) or None.
    Priority: CRITICAL > HIGH > MEDIUM
    CRITICAL fires any time Mon-Fri; HIGH/MEDIUM gated to actionable hours.
    """
    ticker     = pos['ticker']
    direction  = pos.get('direction', 'bullish')
    entry      = pos.get('entry_price') or 0.0
    stop       = pos.get('stop_loss')
    t1         = pos.get('target_1')
    t2         = pos.get('target_2')
    t3         = pos.get('target_3')
    zone_low   = pos.get('zone_low')
    zone_high  = pos.get('zone_high')
    tracking   = pos.get('tracking_target', 'T1')
    last_alert = pos.get('last_alert_at')
    alert_level = pos.get('alert_level', '')
    bullish    = direction != 'bearish'

    # ── CRITICAL — fires any time, including weekends (gap-open stops must fire) ─
    now = datetime.now(timezone.utc)

    # ── CRITICAL ─────────────────────────────────────────────────────────────
    if stop is not None:
        at_stop = (bullish and price <= stop * 1.005) or (not bullish and price >= stop * 0.995)
        if at_stop and _cooldown_ok(last_alert if alert_level == 'CRITICAL' else None, _CRITICAL_COOLDOWN_H):
            return ('stop_loss_zone_reached', 'CRITICAL')

    # Structural invalidation: price closed back through the FVG/OB origin zone
    if zone_low is not None and zone_high is not None:
        origin_breached = (
            (bullish and price < zone_low * 0.998) or
            (not bullish and price > zone_high * 1.002)
        )
        if origin_breached:
            return ('pattern_invalidated', 'CRITICAL')

    # Signal direction contradicted by KB atom
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

    # ── HIGH/MEDIUM — only during actionable hours and weekdays ─────────────
    if now.weekday() >= 5 or not _is_actionable_hours():
        return None

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


def _check_profit_triggers(pos: dict, price: float, db_path: str,
                           confidence: Optional[float]) -> Optional[tuple]:
    """
    Evaluate profit-target and trailing-pullback conditions.
    Runs after _check_triggers() and only during market hours.
    Returns (alert_type, priority) or None.

    Profit lock alerts (HIGH, 4h cooldown):
      t1_profit_lock — price >= T1 AND KB confidence deteriorating (< 0.40)
      t2_profit_lock — price >= T2 AND KB confidence deteriorating (< 0.40)

    Trailing pullback alert (HIGH, 4h cooldown, once per peak):
      trailing_pullback — peak was set in last 60 min above T1, price has
                          since pulled back >1.5%, and this peak hasn't been
                          alerted yet (alerted_peak_price != peak_price).
    """
    direction  = pos.get('direction', 'bullish')
    bullish    = direction != 'bearish'
    entry      = pos.get('entry_price') or 0.0
    t1         = pos.get('target_1')
    t2         = pos.get('target_2')
    last_alert = pos.get('last_alert_at')
    alert_level = pos.get('alert_level', '')

    if not _cooldown_ok(last_alert if alert_level == 'HIGH' else None, _HIGH_COOLDOWN_H):
        return None

    # ── Profit lock at T1 ────────────────────────────────────────────────────
    if t1 is not None:
        past_t1 = (bullish and price >= t1 * 0.995) or (not bullish and price <= t1 * 1.005)
        if past_t1 and confidence is not None and confidence < 0.40:
            return ('t1_profit_lock', 'HIGH')

    # ── Profit lock at T2 ────────────────────────────────────────────────────
    if t2 is not None:
        past_t2 = (bullish and price >= t2 * 0.995) or (not bullish and price <= t2 * 1.005)
        if past_t2 and confidence is not None and confidence < 0.40:
            return ('t2_profit_lock', 'HIGH')

    # ── Trailing pullback ─────────────────────────────────────────────────────
    if t1 is None:
        return None

    peak_price        = pos.get('peak_price')
    peak_updated_at   = pos.get('peak_price_updated_at')
    alerted_peak      = pos.get('alerted_peak_price')

    if peak_price is None or peak_updated_at is None:
        return None

    # Peak must be above T1 to be a meaningful profit situation
    peak_above_t1 = (bullish and peak_price >= t1) or (not bullish and peak_price <= t1)
    if not peak_above_t1:
        return None

    # Recency check: peak must have been updated within the last 60 minutes
    try:
        peak_dt = datetime.fromisoformat(peak_updated_at.replace('Z', '+00:00'))
        peak_age_min = (datetime.now(timezone.utc) - peak_dt).total_seconds() / 60
        if peak_age_min > 60:
            return None
    except Exception:
        return None

    # Pullback check: price has retreated >1.5% from the peak
    pullback_pct = (peak_price - price) / peak_price * 100
    if not bullish:
        pullback_pct = (price - peak_price) / peak_price * 100
    if pullback_pct <= 1.5:
        return None

    # Per-peak dedup: only fire once per distinct peak price
    if alerted_peak is not None and abs(alerted_peak - peak_price) < 0.001:
        return None

    return ('trailing_pullback', 'HIGH')


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
    elif alert_type in ('t1_profit_lock', 't2_profit_lock'):
        buttons = [
            [{'text': '💰 Take profit',     'callback_data': f'{base}:closed'},
             {'text': '📈 Still holding',   'callback_data': f'{base}:hold_t2'}],
            [{'text': '⚡ Partial exit',    'callback_data': f'{base}:partial'}],
        ]
    elif alert_type == 'trailing_pullback':
        buttons = [
            [{'text': '💰 Take profit',     'callback_data': f'{base}:closed'},
             {'text': '📈 Still holding',   'callback_data': f'{base}:hold_t2'}],
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


def _check_expiry(pos: dict, db_path: str) -> bool:
    """
    Check if a position has passed its expires_at time.
    If so, mark it expired in DB (no Telegram — batched to next scheduled delivery).
    Returns True if expired and handled.
    """
    expires_at = pos.get('expires_at')
    if not expires_at:
        return False
    try:
        exp = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
        if datetime.now(timezone.utc) < exp:
            return False
    except Exception:
        return False

    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        from users.user_store import update_followup_status
        update_followup_status(
            db_path, pos['id'],
            status='expired',
            closed_at=now_iso,
        )
        _write_alert(db_path, pos, 'pattern_expired', 'LOW', pos.get('entry_price') or 0.0)
        _log.info('PositionMonitor: expired followup %d %s', pos['id'], pos['ticker'])
    except Exception as e:
        _log.debug('PositionMonitor: expiry update failed for followup %d: %s', pos['id'], e)
    return True


def _send_telegram_alert_with_confidence(
    db_path: str,
    user_id: str,
    alert_id: int,
    pos: dict,
    alert_type: str,
    price: float,
    confidence: Optional[float],
) -> None:
    """Format and send a Telegram position update alert with optional confidence score."""
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

        from notifications.tip_formatter import format_emergency_alert_with_confidence
        msg = format_emergency_alert_with_confidence(alert_type, pos, price, confidence)

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


def _run_monitor_cycle(db_path: str) -> None:
    """One cycle: load all watching positions, check triggers, fire alerts."""
    from users.user_store import (
        get_watching_followups, update_followup_status,
        update_peak_price, update_alerted_peak_price,
    )

    try:
        positions = get_watching_followups(db_path)
    except Exception as e:
        _log.error('PositionMonitor: failed to load followups: %s', e)
        return

    for pos in positions:
        ticker = pos['ticker']
        try:
            # Check expiry first — write to DB only, no Telegram
            if _check_expiry(pos, db_path):
                continue

            price = _get_latest_price(db_path, ticker)
            if price is None:
                continue

            direction = pos.get('direction', 'bullish')
            bullish   = direction != 'bearish'

            # Update peak price watermark every cycle (silent, no alert)
            peak_price = pos.get('peak_price')
            is_new_peak = (
                (bullish  and (peak_price is None or price > peak_price)) or
                (not bullish and (peak_price is None or price < peak_price))
            )
            if is_new_peak:
                try:
                    update_peak_price(db_path, pos['id'], price)
                    pos = dict(pos)  # detach so we can mutate for this cycle
                    pos['peak_price'] = price
                    pos['peak_price_updated_at'] = datetime.now(timezone.utc).isoformat()
                except Exception:
                    pass

            # Compute normalised confidence score (needed for both trigger paths)
            confidence = None
            try:
                confidence = _compute_confidence(db_path, ticker, direction)
            except Exception:
                pass

            # Standard trigger checks (structural invalidation, stop, regime, T1/T2)
            result = _check_triggers(pos, price, db_path)

            # Profit trigger checks (profit lock, trailing pullback) — market hours only
            if result is None and _is_market_hours():
                result = _check_profit_triggers(pos, price, db_path, confidence)

            if result is None:
                continue

            alert_type, priority = result
            _log.info('PositionMonitor: %s %s → %s (%s) price=%.4f',
                      pos['user_id'], ticker, alert_type, priority, price)

            alert_id = _write_alert(db_path, pos, alert_type, priority, price)

            now_iso = datetime.now(timezone.utc).isoformat()
            update_followup_status(
                db_path, pos['id'],
                status=pos['status'],
                alert_level=priority,
            )
            # Update pos in-memory so cooldown is respected if monitor fires again
            # before the next DB read (prevents same alert spamming every 5-min cycle)
            if not isinstance(pos, dict):
                pos = dict(pos)
            pos['alert_level'] = priority
            pos['last_alert_at'] = now_iso

            # For trailing_pullback: record which peak triggered the alert
            if alert_type == 'trailing_pullback':
                try:
                    update_alerted_peak_price(db_path, pos['id'], pos['peak_price'])
                except Exception:
                    pass

            _send_telegram_alert_with_confidence(
                db_path, pos['user_id'], alert_id, pos, alert_type, price, confidence
            )

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
