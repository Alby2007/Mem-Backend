"""
notifications/delivery_scheduler.py — Timezone-Aware Delivery Scheduler

Background thread that checks every 60 seconds whether any user's
delivery_time has arrived in their local timezone, then:
  1. curate_snapshot(user_id, db_path)
  2. format_snapshot(snapshot)
  3. TelegramNotifier.send(chat_id, message)
  4. log_delivery(...)

DEDUP STRATEGY
==============
Uses local-date boundary, NOT a rolling 23-hour window.
"Has this user already received a successful snapshot on today's date
in their local timezone?" — robust through DST transitions.

Requires Python 3.9+ (zoneinfo stdlib).
Falls back to UTC if a user's timezone string is invalid.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    _HAS_ZONEINFO = True
except ImportError:
    _HAS_ZONEINFO = False
    ZoneInfoNotFoundError = Exception  # type: ignore


def _get_local_now(timezone_str: str) -> datetime:
    """
    Return the current time in the user's timezone.
    Falls back to UTC if timezone_str is invalid or zoneinfo unavailable.
    """
    if not _HAS_ZONEINFO or not timezone_str:
        return datetime.now(timezone.utc)
    try:
        tz = ZoneInfo(timezone_str)
        return datetime.now(tz)
    except (ZoneInfoNotFoundError, Exception):
        _log.warning('DeliveryScheduler: unknown timezone %r — falling back to UTC', timezone_str)
        return datetime.now(timezone.utc)


def _should_deliver(
    db_path: str,
    user_id: str,
    delivery_time: str,
    timezone_str: str,
    tier: str = 'basic',
) -> bool:
    """
    Return True if delivery should fire now for this user.
    Delegates to notify_gate.should_notify — single source of truth.
    """
    from notifications.notify_gate import should_notify
    from users.user_store import already_delivered_today
    fire, _ = should_notify(
        db_path        = db_path,
        user_id        = user_id,
        tier           = tier,
        delivery_time  = delivery_time,
        timezone_str   = timezone_str,
        dedup_fn       = already_delivered_today,
        check_briefing_days = False,
    )
    return fire


def _deliver_to_user(db_path: str, user_id: str, user_prefs: dict) -> None:
    """
    Run the full delivery pipeline for one user.
    Logs success/failure to snapshot_delivery_log.
    """
    from analytics.snapshot_curator import curate_snapshot
    from notifications.snapshot_formatter import format_snapshot
    from notifications.telegram_notifier import TelegramNotifier
    from users.user_store import log_delivery

    chat_id = user_prefs.get('telegram_chat_id')
    if not chat_id:
        _log.info('DeliveryScheduler: user %s has no telegram_chat_id — skipping', user_id)
        return

    try:
        tier     = user_prefs.get('tier', 'basic')
        snapshot = curate_snapshot(user_id, db_path, tier=tier)
        message  = format_snapshot(snapshot)

        notifier = TelegramNotifier()
        success  = notifier.send(chat_id, message)

        # Compute user's local date for DST-safe dedup
        local_date = _get_local_now(
            user_prefs.get('timezone', 'UTC')
        ).strftime('%Y-%m-%d')

        log_delivery(
            db_path,
            user_id,
            success=success,
            message_length=len(message),
            regime_at_delivery=snapshot.market_regime,
            opportunities_count=len(snapshot.top_opportunities),
            local_date=local_date,
        )

        if success:
            _log.info('DeliveryScheduler: delivered to user %s (%d opportunities)',
                      user_id, len(snapshot.top_opportunities))
        else:
            _log.warning('DeliveryScheduler: Telegram send failed for user %s', user_id)

    except Exception as exc:
        _log.error('DeliveryScheduler: error delivering to user %s — %s', user_id, exc)
        try:
            log_delivery(db_path, user_id, success=False)
        except Exception:
            pass


class DeliveryScheduler:
    """
    Background thread that runs every `check_interval_sec` seconds.
    Iterates over all onboarded users and fires delivery when due.

    Usage:
        scheduler = DeliveryScheduler(db_path)
        scheduler.start()
        # ... runs until scheduler.stop() is called
    """

    def __init__(self, db_path: str, check_interval_sec: int = 60):
        self._db_path    = db_path
        self._interval   = check_interval_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background delivery thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name='DeliveryScheduler',
            daemon=True,
        )
        self._thread.start()
        _log.info('DeliveryScheduler: started (interval=%ds)', self._interval)

    def stop(self) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        _log.info('DeliveryScheduler: stopped')

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        """Main loop — runs in background thread."""
        while not self._stop_event.is_set():
            try:
                self._check_all_users()
            except Exception as exc:
                _log.error('DeliveryScheduler: unhandled error in check loop — %s', exc)
            self._stop_event.wait(timeout=self._interval)

    def _check_all_users(self) -> None:
        """
        Load all onboarded users and trigger delivery for those whose
        local delivery_time has arrived and haven't been delivered today.
        """
        from users.user_store import ensure_user_tables
        import sqlite3

        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            ensure_user_tables(conn)
            rows = conn.execute(
                """SELECT user_id, telegram_chat_id, delivery_time, timezone,
                          COALESCE(tier, 'basic')
                   FROM user_preferences
                   WHERE onboarding_complete = 1
                     AND telegram_chat_id IS NOT NULL"""
            ).fetchall()
            conn.close()
        except Exception as exc:
            _log.error('DeliveryScheduler: failed to load users — %s', exc)
            return

        for user_id, chat_id, delivery_time, tz_str, _tier in rows:
            try:
                if _should_deliver(self._db_path, user_id, delivery_time, tz_str or 'UTC', tier=_tier):
                    prefs = {
                        'user_id':          user_id,
                        'telegram_chat_id': chat_id,
                        'delivery_time':    delivery_time,
                        'timezone':         tz_str or 'UTC',
                        'tier':             _tier,
                    }
                    _deliver_to_user(self._db_path, user_id, prefs)
            except Exception as exc:
                _log.error('DeliveryScheduler: error checking user %s — %s', user_id, exc)
