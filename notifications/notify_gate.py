"""
notifications/notify_gate.py — Shared delivery gate for all schedulers

Single source of truth for "should this user receive a notification now?"
Replaces the duplicate _should_deliver() / _should_send_batch() logic that
previously lived independently in delivery_scheduler.py and tip_scheduler.py.

Usage
-----
    from notifications.notify_gate import should_notify

    fire, weekday = should_notify(
        db_path        = db_path,
        user_id        = user_id,
        tier           = tier,
        delivery_time  = delivery_time,   # 'HH:MM'
        timezone_str   = tz_str,
        dedup_fn       = already_delivered_today,   # callable(db_path, user_id, date) -> bool
    )

Returns
-------
(True, 'monday') if the scheduler should fire now, (False, '') otherwise.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

_log = logging.getLogger(__name__)

_WEEKDAY_NAMES = [
    'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
]

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    _HAS_ZONEINFO = True
except ImportError:
    _HAS_ZONEINFO = False
    ZoneInfoNotFoundError = Exception  # type: ignore


def _get_local_now(timezone_str: str) -> datetime:
    """Return current time in *timezone_str*, UTC fallback."""
    if not _HAS_ZONEINFO or not timezone_str:
        return datetime.now(timezone.utc)
    try:
        return datetime.now(ZoneInfo(timezone_str))
    except (ZoneInfoNotFoundError, Exception):
        _log.warning('notify_gate: unknown timezone %r — falling back to UTC', timezone_str)
        return datetime.now(timezone.utc)


def _normalise_days(days) -> list:
    """
    Normalise the delivery_days / briefing_days value from tier config.

    Accepts:
      - A list of weekday strings  → returned as-is
      - The string 'daily'         → expanded to all seven weekdays
      - None / missing             → ['monday'] as safe default
    """
    if days == 'daily':
        return list(_WEEKDAY_NAMES)
    if isinstance(days, list):
        return [d.lower() for d in days]
    return ['monday']


def should_notify(
    db_path: str,
    user_id: str,
    tier: str,
    delivery_time: str,
    timezone_str: str,
    dedup_fn: Callable[[str, str, str], bool],
    check_briefing_days: bool = False,
) -> Tuple[bool, str]:
    """
    Return (should_fire: bool, weekday_name: str).

    Parameters
    ----------
    db_path              Path to SQLite DB (passed through to dedup_fn).
    user_id              User identifier.
    tier                 Tier string ('basic', 'pro', 'premium', …).
    delivery_time        'HH:MM' string — user's scheduled delivery time.
    timezone_str         IANA timezone string, e.g. 'Europe/London'.
    dedup_fn             Callable(db_path, user_id, local_date_str) -> bool.
                         Should return True if already delivered today.
    check_briefing_days  When True, also allows briefing_days-only days
                         (e.g. Pro Mon–Fri monitoring) in addition to
                         delivery_days (setup days). Pass False for the
                         snapshot delivery scheduler.

    Logic
    -----
    1. Current local HH:MM must match delivery_time.
    2. Today's weekday must be in the allowed days set for this tier.
    3. dedup_fn must return False (not already sent today).
    """
    from core.tiers import get_tier as _get_tier_cfg

    local_now      = _get_local_now(timezone_str)
    local_time     = local_now.strftime('%H:%M')
    local_date_str = local_now.strftime('%Y-%m-%d')

    # Allow up to 2-hour catch-up window so server restarts don't miss the slot
    try:
        _sched_h, _sched_m = map(int, delivery_time.split(':'))
        _now_h,   _now_m   = map(int, local_time.split(':'))
        _delta = (_now_h * 60 + _now_m) - (_sched_h * 60 + _sched_m)
        if not (0 <= _delta <= 120):
            return False, ''
    except Exception:
        if local_time != delivery_time:
            return False, ''

    today_name = _WEEKDAY_NAMES[local_now.date().weekday()]

    cfg           = _get_tier_cfg(tier)
    delivery_days = _normalise_days(cfg.get('delivery_days', ['monday']))

    if check_briefing_days:
        briefing_days = _normalise_days(cfg.get('briefing_days', delivery_days))
        allowed_days  = list(dict.fromkeys(delivery_days + briefing_days))
    else:
        allowed_days = delivery_days

    if today_name not in allowed_days:
        return False, ''

    if dedup_fn(db_path, user_id, local_date_str):
        return False, ''

    return True, today_name
