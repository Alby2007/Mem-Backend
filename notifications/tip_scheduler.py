"""
notifications/tip_scheduler.py — User Delivery-Time-Aware Tip Scheduler

Background thread that checks every 60 seconds whether any user's
tip_delivery_time has arrived in their local timezone, then:
  1. Load all open pattern_signals filtered by user's tier/timeframes/patterns
  2. Pick the highest quality_score signal not already alerted to this user
  3. calculate_position() with user account prefs
  4. format_tip() with tier context
  5. TelegramNotifier.send()
  6. mark_pattern_alerted() + log_tip_delivery()

DEDUP STRATEGY
==============
Uses tip_delivery_log.delivered_at_local_date (same as delivery_scheduler).
"Has this user already received a successful tip on today's date in their
local timezone?"

Requires Python 3.9+ (zoneinfo stdlib).
Falls back to UTC if a user's timezone string is invalid.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import List, Optional

_log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    _HAS_ZONEINFO = True
except ImportError:
    _HAS_ZONEINFO = False
    ZoneInfoNotFoundError = Exception  # type: ignore


def _get_local_now(timezone_str: str) -> datetime:
    """Return current time in user's timezone, UTC fallback."""
    if not _HAS_ZONEINFO or not timezone_str:
        return datetime.now(timezone.utc)
    try:
        return datetime.now(ZoneInfo(timezone_str))
    except (ZoneInfoNotFoundError, Exception):
        _log.warning('TipScheduler: unknown timezone %r — falling back to UTC', timezone_str)
        return datetime.now(timezone.utc)


def _should_tip(db_path: str, user_id: str, delivery_time: str, timezone_str: str) -> bool:
    """
    Return True if:
      - current local HH:MM matches tip_delivery_time
      - no successful tip delivered today (local date)
    """
    local_now  = _get_local_now(timezone_str)
    local_time = local_now.strftime('%H:%M')
    local_date = local_now.strftime('%Y-%m-%d')

    if local_time != delivery_time:
        return False

    from users.user_store import already_tipped_today
    return not already_tipped_today(db_path, user_id, local_date)


def _pick_best_pattern(
    db_path:       str,
    user_id:       str,
    tier:          str,
    tip_timeframes: List[str],
    tip_pattern_types: Optional[List[str]],
) -> Optional[dict]:
    """
    Select the highest quality open pattern that:
      - timeframe is in user's allowed timeframes (tier-gated)
      - pattern_type is in user's allowed types (tier-gated, optionally filtered)
      - user_id not already in alerted_users
      - passes calibration filter (if both personal hit_rate < 0.40 AND
        collective hit_rate_t2 < 0.45, skip it)
    """
    from notifications.tip_formatter import TIER_LIMITS, pattern_allowed_for_tier, timeframe_allowed_for_tier
    from users.user_store import get_open_patterns

    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    allowed_patterns   = tip_pattern_types or limits['patterns']
    allowed_timeframes = tip_timeframes or limits['timeframes']

    # Load personal pattern hit rates once
    personal_hit_rates: dict = {}
    try:
        from users.personal_kb import read_atoms
        atoms = read_atoms(user_id, db_path)
        for a in atoms:
            if a['predicate'].endswith('_hit_rate'):
                ptype = a['predicate'][:-len('_hit_rate')]
                try:
                    personal_hit_rates[ptype] = float(a['object'])
                except (ValueError, TypeError):
                    pass
    except Exception:
        pass

    candidates = get_open_patterns(db_path, min_quality=0.0, limit=200)
    for row in candidates:
        if row['pattern_type'] not in allowed_patterns:
            continue
        if row['timeframe'] not in allowed_timeframes:
            continue
        if not pattern_allowed_for_tier(row['pattern_type'], tier):
            continue
        if not timeframe_allowed_for_tier(row['timeframe'], tier):
            continue
        alerted = row.get('alerted_users') or []
        if user_id in alerted:
            continue

        # Calibration filter: skip only if BOTH personal AND collective are weak
        ptype = row['pattern_type']
        personal_rate = personal_hit_rates.get(ptype)
        if personal_rate is not None and personal_rate < 0.40:
            try:
                from analytics.signal_calibration import get_calibration
                cal = get_calibration(
                    ticker=row['ticker'],
                    pattern_type=ptype,
                    timeframe=row['timeframe'],
                    db_path=db_path,
                )
                if cal is not None and cal.hit_rate_t2 is not None and cal.hit_rate_t2 < 0.45:
                    _log.debug(
                        'TipScheduler: skipping %s %s — personal %.2f + collective %.2f both weak',
                        row['ticker'], ptype, personal_rate, cal.hit_rate_t2,
                    )
                    continue
            except Exception:
                pass

        return row
    return None


def _deliver_tip_to_user(db_path: str, user_id: str, user_prefs: dict) -> None:
    """Run the full tip delivery pipeline for one user."""
    from analytics.pattern_detector import PatternSignal
    from analytics.position_calculator import calculate_position
    from notifications.tip_formatter import format_tip, pattern_allowed_for_tier, timeframe_allowed_for_tier
    from notifications.telegram_notifier import TelegramNotifier
    from users.user_store import (
        get_user, log_tip_delivery, mark_pattern_alerted,
    )

    chat_id = user_prefs.get('telegram_chat_id')
    if not chat_id:
        _log.info('TipScheduler: user %s has no telegram_chat_id — skipping', user_id)
        return

    tier              = user_prefs.get('tier', 'basic')
    tip_timeframes    = user_prefs.get('tip_timeframes') or ['1h']
    tip_pattern_types = user_prefs.get('tip_pattern_types')  # None = all allowed for tier

    pattern_row = _pick_best_pattern(db_path, user_id, tier, tip_timeframes, tip_pattern_types)
    if pattern_row is None:
        _log.info('TipScheduler: no eligible patterns for user %s', user_id)
        return

    local_now  = _get_local_now(user_prefs.get('tip_delivery_timezone', 'UTC'))
    local_date = local_now.strftime('%Y-%m-%d')

    try:
        # Reconstruct PatternSignal from DB row
        sig = PatternSignal(
            pattern_type  = pattern_row['pattern_type'],
            ticker        = pattern_row['ticker'],
            direction     = pattern_row['direction'],
            zone_high     = pattern_row['zone_high'],
            zone_low      = pattern_row['zone_low'],
            zone_size_pct = pattern_row['zone_size_pct'],
            timeframe     = pattern_row['timeframe'],
            formed_at     = pattern_row['formed_at'],
            quality_score = pattern_row['quality_score'] or 0.0,
            status        = pattern_row['status'],
            kb_conviction = pattern_row.get('kb_conviction', ''),
            kb_regime     = pattern_row.get('kb_regime', ''),
            kb_signal_dir = pattern_row.get('kb_signal_dir', ''),
        )

        position = calculate_position(sig, user_prefs)

        # Fetch calibration to pass to formatter (None is safe — graceful no-op)
        calibration = None
        try:
            from analytics.signal_calibration import get_calibration
            calibration = get_calibration(
                ticker=pattern_row['ticker'],
                pattern_type=pattern_row['pattern_type'],
                timeframe=pattern_row['timeframe'],
                db_path=db_path,
            )
        except Exception:
            pass

        # Probabilistic forecast — seeded for ledger reproducibility
        forecast = None
        if position is not None:
            try:
                from analytics.signal_forecaster import SignalForecaster
                from datetime import datetime, timezone
                issued_at_iso = datetime.now(timezone.utc).isoformat()
                forecaster = SignalForecaster(db_path)
                forecast = forecaster.forecast(
                    ticker       = sig.ticker,
                    pattern_type = sig.pattern_type,
                    timeframe    = sig.timeframe,
                    account_size = position.account_size,
                    risk_pct     = position.risk_pct,
                    seed         = f'{sig.ticker}{sig.pattern_type}{issued_at_iso}',
                )
                position.forecast = forecast
            except Exception as _fe:
                _log.debug('TipScheduler: forecast failed for %s: %s', sig.ticker, _fe)

        message  = format_tip(sig, position, tier=tier, calibration=calibration)

        notifier = TelegramNotifier()
        sent     = notifier.send(chat_id, message)

        mark_pattern_alerted(db_path, pattern_row['id'], user_id)
        log_tip_delivery(
            db_path,
            user_id,
            success        = sent,
            pattern_signal_id = pattern_row['id'],
            message_length = len(message),
            local_date     = local_date,
        )

        # Record to prediction ledger after successful send
        if sent and forecast is not None and position is not None:
            try:
                from analytics.prediction_ledger import PredictionLedger
                ledger = PredictionLedger(db_path)
                ledger.record_prediction(
                    ticker         = sig.ticker,
                    pattern_type   = sig.pattern_type,
                    timeframe      = sig.timeframe,
                    entry_price    = position.suggested_entry,
                    target_1       = position.target_1,
                    target_2       = position.target_2,
                    stop_loss      = position.stop_loss,
                    p_hit_t1       = forecast.p_hit_t1,
                    p_hit_t2       = forecast.p_hit_t2,
                    p_stopped_out  = forecast.p_stopped_out,
                    market_regime  = forecast.market_regime,
                    conviction_tier = tier,
                )
            except Exception as _le:
                _log.debug('TipScheduler: ledger record failed for %s: %s', sig.ticker, _le)

        if sent:
            _log.info('TipScheduler: delivered %s %s tip to user %s',
                      sig.ticker, sig.pattern_type, user_id)
        else:
            _log.warning('TipScheduler: Telegram send failed for user %s', user_id)

    except Exception as exc:
        _log.error('TipScheduler: error delivering tip to user %s: %s', user_id, exc)
        try:
            log_tip_delivery(db_path, user_id, success=False, local_date=local_date)
        except Exception:
            pass


def _run_tip_cycle(db_path: str) -> None:
    """Check all users and dispatch tips where delivery time has arrived."""
    from users.user_store import ensure_user_tables
    import sqlite3

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            """SELECT user_id, telegram_chat_id, tier,
                      tip_delivery_time, tip_delivery_timezone,
                      tip_timeframes, tip_pattern_types,
                      account_size, max_risk_per_trade_pct, account_currency
               FROM user_preferences
               WHERE onboarding_complete = 1""",
        ).fetchall()
    except Exception as exc:
        _log.error('TipScheduler: failed to load users: %s', exc)
        return
    finally:
        conn.close()

    import json
    cols = ['user_id', 'telegram_chat_id', 'tier',
            'tip_delivery_time', 'tip_delivery_timezone',
            'tip_timeframes', 'tip_pattern_types',
            'account_size', 'max_risk_per_trade_pct', 'account_currency']

    for row in rows:
        prefs = dict(zip(cols, row))
        user_id       = prefs['user_id']
        delivery_time = prefs.get('tip_delivery_time') or '08:00'
        timezone_str  = prefs.get('tip_delivery_timezone') or 'UTC'

        for json_col in ('tip_timeframes', 'tip_pattern_types'):
            try:
                prefs[json_col] = json.loads(prefs[json_col]) if prefs[json_col] else None
            except (json.JSONDecodeError, TypeError):
                prefs[json_col] = None

        try:
            if _should_tip(db_path, user_id, delivery_time, timezone_str):
                _deliver_tip_to_user(db_path, user_id, prefs)
        except Exception as exc:
            _log.error('TipScheduler: unhandled error for user %s: %s', user_id, exc)


class TipScheduler:
    """
    Background thread that dispatches daily tips at each user's configured time.

    Parameters
    ----------
    db_path      Path to the SQLite knowledge base file.
    interval_sec Check interval in seconds. Default 60.
    """

    def __init__(self, db_path: str, interval_sec: int = 60):
        self.db_path      = db_path
        self.interval_sec = interval_sec
        self._stop_event  = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background dispatch thread (non-blocking)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name='tip-scheduler',
            daemon=True,
        )
        self._thread.start()
        _log.info('TipScheduler: started (interval=%ds)', self.interval_sec)

    def stop(self) -> None:
        """Signal the background thread to stop."""
        self._stop_event.set()
        _log.info('TipScheduler: stop requested')

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                _run_tip_cycle(self.db_path)
            except Exception as exc:
                _log.error('TipScheduler: cycle error: %s', exc)
            self._stop_event.wait(self.interval_sec)
