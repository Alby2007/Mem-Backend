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


def _week_monday(local_date) -> str:
    """Return the ISO date string of Monday of the week containing local_date."""
    from datetime import timedelta
    return (local_date - timedelta(days=local_date.weekday())).strftime('%Y-%m-%d')


def _should_send_batch(
    db_path: str,
    user_id: str,
    tier: str,
    delivery_time: str,
    timezone_str: str,
) -> tuple:
    """
    Return (should_send: bool, weekday: str) where weekday is 'monday'/'wednesday'/'daily'.

    Premium tier: existing daily logic — HH:MM match + not tipped today.
    Basic/Pro tier: weekly cadence — HH:MM match + correct weekday for tier
                    + not already sent this ISO week on this weekday slot.
    """
    from datetime import date
    from notifications.tip_formatter import TIER_LIMITS

    local_now  = _get_local_now(timezone_str)
    local_time = local_now.strftime('%H:%M')
    local_date_obj = local_now.date()
    local_date_str = local_date_obj.strftime('%Y-%m-%d')

    if local_time != delivery_time:
        return False, ''

    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    delivery_days = limits.get('delivery_days', 'daily')

    # Premium = daily
    if delivery_days == 'daily':
        from users.user_store import already_tipped_today
        if already_tipped_today(db_path, user_id, local_date_str):
            return False, ''
        return True, 'daily'

    # Weekly cadence
    _WEEKDAY_NAMES = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    today_name = _WEEKDAY_NAMES[local_date_obj.weekday()]
    if today_name not in delivery_days:
        return False, ''

    monday_str = _week_monday(local_date_obj)
    from users.user_store import already_sent_this_week_slot
    if already_sent_this_week_slot(db_path, user_id, today_name, monday_str):
        return False, ''

    return True, today_name


# ── Sector → connected-ticker map ─────────────────────────────────────────────
# Maps a sector tag (as stored in user_portfolios.sector, lower-cased) to a
# list of US-listed proxy tickers that are correlated to that sector.
_SECTOR_CONNECTED: dict = {
    'technology':        ['XLK', 'QQQ', 'NVDA', 'AMD', 'MSFT', 'AAPL'],
    'tech':              ['XLK', 'QQQ', 'NVDA', 'AMD', 'MSFT', 'AAPL'],
    'financials':        ['XLF', 'JPM', 'GS', 'MS', 'BAC'],
    'finance':           ['XLF', 'JPM', 'GS', 'MS', 'BAC'],
    'banking':           ['XLF', 'JPM', 'GS', 'MS', 'BAC'],
    'energy':            ['XLE', 'XOM', 'CVX', 'COP'],
    'oil':               ['XLE', 'XOM', 'CVX', 'COP'],
    'healthcare':        ['XLV', 'UNH', 'JNJ', 'LLY'],
    'health':            ['XLV', 'UNH', 'JNJ', 'LLY'],
    'pharma':            ['XLV', 'PFE', 'MRK', 'ABBV'],
    'consumer':          ['XLY', 'XLP', 'WMT', 'COST', 'MCD'],
    'consumer staples':  ['XLP', 'WMT', 'PG', 'KO', 'COST'],
    'consumer discretionary': ['XLY', 'AMZN', 'TSLA', 'MCD'],
    'industrials':       ['XLI', 'CAT', 'HON', 'RTX'],
    'materials':         ['XLB', 'GLD', 'SLV'],
    'real estate':       ['XLRE', 'AMT', 'PLD', 'EQIX'],
    'reits':             ['XLRE', 'AMT', 'PLD', 'EQIX'],
    'utilities':         ['XLU', 'NEE', 'DUK', 'SO'],
    'communications':    ['XLC', 'DIS', 'NFLX', 'CMCSA'],
    'comms':             ['XLC', 'DIS', 'NFLX', 'CMCSA'],
    'crypto':            ['COIN', 'MSTR'],
    'etf':               ['SPY', 'QQQ', 'IWM', 'VTI'],
}

# Broad fallback universe (same as GET /markets/tickers default list)
_DEFAULT_UNIVERSE: List[str] = [
    'AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','AVGO',
    'JPM','V','MA','BAC','GS','MS','BRK-B','AXP','BLK','SCHW',
    'UNH','JNJ','LLY','ABBV','PFE','CVS','MRK','BMY','GILD',
    'XOM','CVX','COP',
    'WMT','PG','KO','MCD','COST',
    'CAT','HON','RTX',
    'DIS','NFLX','CMCSA',
    'AMD','INTC','QCOM','MU','CRM','ADBE','NOW','SNOW',
    'PYPL','COIN',
    'AMT','PLD','EQIX',
    'NEE','DUK','SO',
    'SPY','QQQ','IWM','DIA','VTI',
    'XLF','XLE','XLK','XLV','XLI','XLC','XLY','XLP',
    'GLD','SLV','TLT','HYG','LQD','UUP',
]


def _scan_candidates(
    candidates: list,
    allowed_tickers: Optional[List[str]],
    allowed_patterns: list,
    allowed_timeframes: list,
    tier: str,
    user_id: str,
    personal_hit_rates: dict,
    db_path: str,
) -> Optional[dict]:
    """
    Inner scan loop: filter candidates by ticker set, tier gates, alerted dedup,
    and calibration filter. Returns first passing row or None.
    """
    from notifications.tip_formatter import pattern_allowed_for_tier, timeframe_allowed_for_tier

    ticker_set = {t.upper() for t in allowed_tickers} if allowed_tickers is not None else None

    for row in candidates:
        if ticker_set is not None and row['ticker'].upper() not in ticker_set:
            continue
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


def _pick_best_pattern(
    db_path:           str,
    user_id:           str,
    tier:              str,
    tip_timeframes:    List[str],
    tip_pattern_types: Optional[List[str]],
    tip_markets:       Optional[List[str]] = None,
) -> Optional[dict]:
    """
    4-level fallback chain:

      1. tip_markets watchlist  → tip_source = 'watchlist'
      2. portfolio holdings     → tip_source = 'portfolio'
      3. connected tickers      → tip_source = 'connected'
         (sector-correlated ETFs / proxies derived from portfolio sectors)
      4. full _DEFAULT_UNIVERSE → tip_source = 'market-wide'

    Each level is only tried when the previous returned nothing.
    The returned dict has an injected key ``tip_source`` used by the
    formatter to label the Telegram message footer.

    When tip_markets is None (All Markets mode), levels 1–3 are skipped
    and the full universe is scanned immediately (no label appended).
    """
    from notifications.tip_formatter import TIER_LIMITS
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
    common = dict(
        allowed_patterns=allowed_patterns,
        allowed_timeframes=allowed_timeframes,
        tier=tier,
        user_id=user_id,
        personal_hit_rates=personal_hit_rates,
        db_path=db_path,
    )

    # ── All Markets mode — no tip_markets set → scan everything, no label ──────
    if not tip_markets:
        row = _scan_candidates(candidates, None, **common)
        if row is not None:
            row = dict(row)
            row['tip_source'] = None  # silent — this is default behaviour
        return row

    # ── Level 1: user's watchlist ─────────────────────────────────────────────
    row = _scan_candidates(candidates, tip_markets, **common)
    if row is not None:
        row = dict(row); row['tip_source'] = 'watchlist'; return row
    _log.info('TipScheduler: no watchlist pattern for user %s — trying portfolio', user_id)

    # ── Level 2: portfolio holdings ───────────────────────────────────────────
    portfolio_tickers: List[str] = []
    portfolio_sectors: List[str] = []
    try:
        from users.user_store import get_portfolio
        holdings = get_portfolio(db_path, user_id)
        for h in holdings:
            t = (h.get('ticker') or '').upper().strip()
            if t:
                portfolio_tickers.append(t)
            s = (h.get('sector') or '').lower().strip()
            if s:
                portfolio_sectors.append(s)
    except Exception:
        pass

    if portfolio_tickers:
        row = _scan_candidates(candidates, portfolio_tickers, **common)
        if row is not None:
            row = dict(row); row['tip_source'] = 'portfolio'; return row
        _log.info('TipScheduler: no portfolio pattern for user %s — trying connected', user_id)

    # ── Level 3: sector-connected tickers ────────────────────────────────────
    connected: List[str] = []
    seen: set = set(portfolio_tickers)
    for sector in portfolio_sectors:
        for t in _SECTOR_CONNECTED.get(sector, []):
            if t not in seen:
                connected.append(t)
                seen.add(t)

    if connected:
        row = _scan_candidates(candidates, connected, **common)
        if row is not None:
            row = dict(row); row['tip_source'] = 'connected'; return row
        _log.info('TipScheduler: no connected pattern for user %s — falling back to universe', user_id)

    # ── Level 4: full default universe ────────────────────────────────────────
    row = _scan_candidates(candidates, _DEFAULT_UNIVERSE, **common)
    if row is not None:
        row = dict(row); row['tip_source'] = 'market-wide'; return row

    return None


def _pick_batch(
    db_path: str,
    user_id: str,
    tier: str,
    tip_timeframes: List[str],
    tip_pattern_types: Optional[List[str]],
    tip_markets: Optional[List[str]],
    batch_size: int,
) -> tuple:
    """
    Pick up to batch_size non-overlapping patterns using the fallback chain.
    Returns (list[pattern_row], tip_source_str).
    """
    from notifications.tip_formatter import TIER_LIMITS
    from users.user_store import get_open_patterns

    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    allowed_patterns   = tip_pattern_types or limits['patterns']
    allowed_timeframes = tip_timeframes or limits['timeframes']

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

    candidates = get_open_patterns(db_path, min_quality=0.0, limit=300)
    common = dict(
        allowed_patterns=allowed_patterns,
        allowed_timeframes=allowed_timeframes,
        tier=tier,
        user_id=user_id,
        personal_hit_rates=personal_hit_rates,
        db_path=db_path,
    )

    batch: List[dict] = []
    used_tickers: set = set()
    final_source = None

    # Run the fallback levels; pick greedily until batch_size reached
    if not tip_markets:
        levels = [(None, None)]
    else:
        portfolio_tickers: List[str] = []
        portfolio_sectors: List[str] = []
        try:
            from users.user_store import get_portfolio
            for h in get_portfolio(db_path, user_id):
                t = (h.get('ticker') or '').upper().strip()
                if t: portfolio_tickers.append(t)
                s = (h.get('sector') or '').lower().strip()
                if s: portfolio_sectors.append(s)
        except Exception:
            pass
        connected: List[str] = []
        seen_c: set = set(portfolio_tickers)
        for sector in portfolio_sectors:
            for t in _SECTOR_CONNECTED.get(sector, []):
                if t not in seen_c:
                    connected.append(t); seen_c.add(t)
        levels = [
            (tip_markets, 'watchlist'),
            (portfolio_tickers or None, 'portfolio'),
            (connected or None, 'connected'),
            (_DEFAULT_UNIVERSE, 'market-wide'),
        ]

    for (ticker_set, source_label) in levels:
        if ticker_set is None and source_label is not None:
            continue  # empty portfolio/connected — skip level
        remaining = [r for r in candidates if r['ticker'].upper() not in used_tickers]
        while len(batch) < batch_size:
            row = _scan_candidates(remaining, ticker_set, **common)
            if row is None:
                break
            batch.append(dict(row))
            used_tickers.add(row['ticker'].upper())
            remaining = [r for r in remaining if r['ticker'].upper() not in used_tickers]
        if batch:
            final_source = source_label
            if len(batch) >= batch_size:
                break
        if not tip_markets:
            break  # All Markets — only one level

    for row in batch:
        row['tip_source'] = final_source
    return batch, final_source


def _check_monday_status(db_path: str, monday_meta: list) -> list:
    """
    For each pattern in monday_meta, check current last_price KB atom vs zone.
    Returns list of status dicts for format_weekly_batch.
    """
    results = []
    for entry in monday_meta:
        ticker    = entry.get('ticker', '')
        zone_low  = entry.get('zone_low', 0.0)
        zone_high = entry.get('zone_high', 0.0)
        stop_loss = entry.get('stop_loss')
        last_price = None
        try:
            import sqlite3 as _sq
            _c = _sq.connect(db_path, timeout=5)
            row = _c.execute(
                """SELECT object FROM facts
                   WHERE LOWER(subject)=? AND predicate='last_price'
                   ORDER BY created_at DESC LIMIT 1""",
                (ticker.lower(),),
            ).fetchone()
            _c.close()
            if row:
                last_price = float(row[0])
        except Exception:
            pass

        if last_price is None:
            status = 'not_triggered'
        elif zone_low <= last_price <= zone_high:
            status = 'in_zone'
        elif (entry.get('direction') == 'bullish' and last_price < zone_low * 0.995) or \
             (entry.get('direction') == 'bearish' and last_price > zone_high * 1.005):
            status = 'zone_broken'
        else:
            status = 'not_triggered'

        results.append({
            'ticker':     ticker,
            'status':     status,
            'last_price': last_price,
            'stop_loss':  stop_loss,
            'zone_low':   zone_low,
            'zone_high':  zone_high,
        })
    return results


def _get_kb_price(db_path: str, ticker: str) -> Optional[float]:
    """Fetch the latest KB last_price atom for a ticker. Returns None if unavailable."""
    try:
        import sqlite3 as _sq
        c = _sq.connect(db_path, timeout=5)
        row = c.execute(
            """SELECT object FROM facts
               WHERE LOWER(subject)=? AND predicate='last_price'
               ORDER BY created_at DESC LIMIT 1""",
            (ticker.lower(),),
        ).fetchone()
        c.close()
        return float(row[0]) if row else None
    except Exception:
        return None


def _deliver_tip_to_user(db_path: str, user_id: str, user_prefs: dict, weekday: str = 'daily') -> None:
    """Run the full tip delivery pipeline for one user."""
    from analytics.pattern_detector import PatternSignal
    from analytics.position_calculator import calculate_position
    from notifications.tip_formatter import (
        TIER_LIMITS, format_tip, format_monday_briefing, format_wednesday_update,
        pattern_allowed_for_tier, timeframe_allowed_for_tier,
    )
    from notifications.telegram_notifier import TelegramNotifier
    from users.user_store import (
        log_tip_delivery, mark_pattern_alerted,
        get_user_open_positions, get_recently_closed_positions,
        get_kb_changes_since, expire_stale_followups, upsert_tip_followup,
    )

    chat_id = user_prefs.get('telegram_chat_id')
    if not chat_id:
        _log.info('TipScheduler: user %s has no telegram_chat_id — skipping', user_id)
        return

    tier              = user_prefs.get('tier', 'basic')
    tip_timeframes    = user_prefs.get('tip_timeframes') or ['1h']
    tip_pattern_types = user_prefs.get('tip_pattern_types')
    tip_markets       = user_prefs.get('tip_markets')

    limits     = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    batch_size = limits.get('batch_size', 1) if weekday != 'daily' else 1
    is_weekly  = weekday in ('monday', 'wednesday')

    local_now  = _get_local_now(user_prefs.get('tip_delivery_timezone', 'UTC'))
    local_date = local_now.strftime('%Y-%m-%d')
    monday_str = _week_monday(local_now.date())

    if is_weekly:
        # ── Expire stale followups first — results included in message ────────
        expired_this_cycle: List[dict] = []
        try:
            expired_this_cycle = expire_stale_followups(db_path)
            # Only include this user's expired positions
            expired_this_cycle = [e for e in expired_this_cycle if e['user_id'] == user_id]
        except Exception as _ee:
            _log.debug('TipScheduler: expire_stale_followups failed: %s', _ee)

        # ── Price lookup helper ───────────────────────────────────────────────
        def _price_fn(ticker: str) -> Optional[float]:
            return _get_kb_price(db_path, ticker)

        # ── MONDAY: living briefing ───────────────────────────────────────────
        if weekday == 'monday':
            open_positions = []
            closed_last_week = []
            try:
                open_positions = get_user_open_positions(db_path, user_id)
            except Exception as _oe:
                _log.debug('TipScheduler: get_user_open_positions failed: %s', _oe)
            try:
                # "Last week" = previous Monday onwards
                from datetime import timedelta
                prev_monday = (local_now.date() - timedelta(days=7)).strftime('%Y-%m-%d')
                closed_last_week = get_recently_closed_positions(db_path, user_id, prev_monday)
                # Don't double-show in expired_this_cycle + closed
                expired_ids = {e['id'] for e in expired_this_cycle}
                closed_last_week = [c for c in closed_last_week if c['id'] not in expired_ids]
            except Exception as _ce:
                _log.debug('TipScheduler: get_recently_closed_positions failed: %s', _ce)

            # Pick new setups to fill the batch
            batch, tip_source = _pick_batch(
                db_path, user_id, tier, tip_timeframes, tip_pattern_types, tip_markets, batch_size,
            )

            pairs = []
            pattern_meta = []
            for row in batch:
                try:
                    sig = PatternSignal(
                        pattern_type  = row['pattern_type'],
                        ticker        = row['ticker'],
                        direction     = row['direction'],
                        zone_high     = row['zone_high'],
                        zone_low      = row['zone_low'],
                        zone_size_pct = row['zone_size_pct'],
                        timeframe     = row['timeframe'],
                        formed_at     = row['formed_at'],
                        quality_score = row['quality_score'] or 0.0,
                        status        = row['status'],
                        kb_conviction = row.get('kb_conviction', ''),
                        kb_regime     = row.get('kb_regime', ''),
                        kb_signal_dir = row.get('kb_signal_dir', ''),
                    )
                    pos = calculate_position(sig, user_prefs)
                    pairs.append((row, pos))
                    pattern_meta.append({
                        'ticker':       row['ticker'],
                        'zone_low':     row['zone_low'],
                        'zone_high':    row['zone_high'],
                        'direction':    row['direction'],
                        'stop_loss':    pos.stop_loss if pos else None,
                        'pattern_type': row['pattern_type'],
                    })
                except Exception as _pe:
                    _log.debug('TipScheduler: error building pair for %s: %s', row.get('ticker'), _pe)

            # Require at least something to send
            if not open_positions and not pairs and not closed_last_week and not expired_this_cycle:
                _log.info('TipScheduler: nothing to brief user %s on Monday', user_id)
                return

            message = format_monday_briefing(
                open_positions   = open_positions,
                new_setups       = pairs,
                closed_last_week = closed_last_week + expired_this_cycle,
                tier             = tier,
                get_price_fn     = _price_fn,
            )

            notifier = TelegramNotifier()
            sent = notifier.send(chat_id, message)

            # Auto-create watching followups for new setups
            for row, pos in pairs:
                try:
                    mark_pattern_alerted(db_path, row['id'], user_id)
                    upsert_tip_followup(
                        db_path,
                        user_id    = user_id,
                        ticker     = row['ticker'],
                        direction  = row['direction'],
                        entry_price   = pos.suggested_entry if pos else None,
                        stop_loss     = pos.stop_loss if pos else None,
                        target_1      = pos.target_1 if pos else None,
                        target_2      = pos.target_2 if pos else None,
                        target_3      = pos.target_3 if pos else None,
                        pattern_type  = row['pattern_type'],
                        timeframe     = row['timeframe'],
                        zone_low      = row['zone_low'],
                        zone_high     = row['zone_high'],
                        regime_at_entry     = row.get('kb_regime'),
                        conviction_at_entry = row.get('kb_conviction'),
                        initial_status = 'watching',
                    )
                except Exception as _fe:
                    _log.debug('TipScheduler: followup create failed for %s: %s', row.get('ticker'), _fe)

            log_tip_delivery(
                db_path, user_id,
                success=sent,
                pattern_signal_id=batch[0]['id'] if batch else None,
                message_length=len(message),
                local_date=local_date,
                pattern_meta=pattern_meta if pattern_meta else None,
            )
            if sent:
                _log.info('TipScheduler: Monday briefing delivered to user %s (%d open, %d new, %d closed)',
                          user_id, len(open_positions), len(pairs), len(closed_last_week))
            else:
                _log.warning('TipScheduler: Telegram send failed for user %s', user_id)
            return

        # ── WEDNESDAY: compound update ────────────────────────────────────────
        if weekday == 'wednesday':
            open_positions = []
            kb_changes = []
            try:
                open_positions = get_user_open_positions(db_path, user_id)
            except Exception as _oe:
                _log.debug('TipScheduler: get_user_open_positions failed: %s', _oe)

            if open_positions:
                try:
                    # KB changes since Monday 00:00 UTC
                    from datetime import timezone as _tz
                    from datetime import datetime as _dt
                    monday_dt = _dt.strptime(monday_str, '%Y-%m-%d').replace(tzinfo=_tz.utc)
                    open_tickers = [p['ticker'] for p in open_positions]
                    kb_changes = get_kb_changes_since(db_path, monday_dt.isoformat(), tickers=open_tickers)
                except Exception as _ke:
                    _log.debug('TipScheduler: get_kb_changes_since failed: %s', _ke)

            if not open_positions and not kb_changes and not expired_this_cycle:
                _log.info('TipScheduler: nothing for Wednesday update for user %s', user_id)
                return

            message = format_wednesday_update(
                open_positions      = open_positions,
                kb_changes          = kb_changes,
                expired_this_cycle  = expired_this_cycle,
                tier                = tier,
                get_price_fn        = _price_fn,
            )

            notifier = TelegramNotifier()
            sent = notifier.send(chat_id, message)

            log_tip_delivery(
                db_path, user_id,
                success=sent,
                pattern_signal_id=None,
                message_length=len(message),
                local_date=local_date,
                pattern_meta=None,
            )
            if sent:
                _log.info('TipScheduler: Wednesday update delivered to user %s (%d open, %d KB changes)',
                          user_id, len(open_positions), len(kb_changes))
            else:
                _log.warning('TipScheduler: Telegram send failed for user %s', user_id)
            return

    # ── Premium / daily path (single tip, existing logic) ────────────────────
    pattern_row = _pick_best_pattern(db_path, user_id, tier, tip_timeframes, tip_pattern_types, tip_markets)
    if pattern_row is None:
        _log.info('TipScheduler: no eligible patterns for user %s', user_id)
        return

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

        tip_source = pattern_row.get('tip_source')
        message  = format_tip(sig, position, tier=tier, calibration=calibration, tip_source=tip_source)

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


def _migrate_pro_to_premium(db_path: str) -> None:
    """
    One-time migration: upgrade existing 'pro' tier users to 'premium'.
    Pro was the old daily tier; premium is the new name for daily delivery.
    Sends each migrated user a Telegram upgrade notification.
    Idempotent — safe to call on every startup.
    """
    import sqlite3 as _sq3
    try:
        conn = _sq3.connect(db_path, timeout=10)
        rows = conn.execute(
            "SELECT user_id, telegram_chat_id FROM user_preferences WHERE tier = 'pro'"
        ).fetchall()
        if not rows:
            conn.close()
            return
        conn.execute("UPDATE user_preferences SET tier = 'premium' WHERE tier = 'pro'")
        conn.commit()
        conn.close()
        _log.info('TipScheduler: migrated %d pro→premium users', len(rows))
        try:
            from notifications.telegram_notifier import TelegramNotifier as _TGN
            _notifier = _TGN()
            _upgrade_msg = (
                "\u2b50 *You've been upgraded to Premium\\!*\n\n"
                "We've restructured our tip tiers\\. Your account has been automatically "
                "upgraded to *Premium* \u2014 daily tips continue exactly as before\\.\n\n"
                "_No action needed\\. Enjoy the signals\\._"
            )
            for (uid, chat_id) in rows:
                if chat_id:
                    try:
                        _notifier.send(chat_id, _upgrade_msg, parse_mode='MarkdownV2')
                    except Exception:
                        pass
        except Exception:
            pass
    except Exception as exc:
        _log.warning('TipScheduler: pro→premium migration failed: %s', exc)


def _run_tip_cycle(db_path: str) -> None:
    """Check all users and dispatch tips where delivery time has arrived."""
    from users.user_store import ensure_user_tables
    import sqlite3

    # Run one-time tier migration on each cycle (idempotent, fast when no rows match)
    _migrate_pro_to_premium(db_path)

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            """SELECT user_id, telegram_chat_id, tier,
                      tip_delivery_time, tip_delivery_timezone,
                      tip_timeframes, tip_pattern_types, tip_markets,
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
            'tip_timeframes', 'tip_pattern_types', 'tip_markets',
            'account_size', 'max_risk_per_trade_pct', 'account_currency']

    for row in rows:
        prefs = dict(zip(cols, row))
        user_id       = prefs['user_id']
        delivery_time = prefs.get('tip_delivery_time') or '08:00'
        timezone_str  = prefs.get('tip_delivery_timezone') or 'UTC'
        tier          = prefs.get('tier') or 'basic'

        for json_col in ('tip_timeframes', 'tip_pattern_types', 'tip_markets'):
            try:
                prefs[json_col] = json.loads(prefs[json_col]) if prefs[json_col] else None
            except (json.JSONDecodeError, TypeError):
                prefs[json_col] = None

        try:
            should_send, weekday = _should_send_batch(db_path, user_id, tier, delivery_time, timezone_str)
            if should_send:
                _deliver_tip_to_user(db_path, user_id, prefs, weekday=weekday)
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
