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


def _get_briefing_mode(day: str) -> str:
    """
    Map a weekday name to a briefing mode string.

    monday    → weekly_setup      (new setups + portfolio open recap)
    tue/wed/thu → position_monitor (KB change check on open positions)
    friday    → week_close        (week summary + closed positions)
    saturday  → weekend_summary   (premium only)
    sunday    → position_monitor  (fallback)
    """
    modes = {
        'monday':    'weekly_setup',
        'tuesday':   'position_monitor',
        'wednesday': 'position_monitor',
        'thursday':  'position_monitor',
        'friday':    'week_close',
        'saturday':  'weekend_summary',
    }
    return modes.get(day, 'position_monitor')


def _should_send_batch(
    db_path: str,
    user_id: str,
    tier: str,
    delivery_time: str,
    timezone_str: str,
) -> tuple:
    """
    Return (should_send: bool, weekday: str).

    Checks both delivery_days (setup batches) and briefing_days (Pro/Premium
    daily monitoring briefings). Monday is always a setup day; other briefing_days
    route to position_monitor / week_close / weekend_summary modes.
    """
    from core.tiers import TIER_CONFIG as TIER_LIMITS, get_tier as _get_tier_cfg

    local_now      = _get_local_now(timezone_str)
    local_time     = local_now.strftime('%H:%M')
    local_date_obj = local_now.date()
    local_date_str = local_date_obj.strftime('%Y-%m-%d')

    # Allow up to 2 hours catch-up after the scheduled time so server restarts
    # don't silently miss the delivery slot.
    try:
        from datetime import datetime as _dt
        _sched_h, _sched_m = map(int, delivery_time.split(':'))
        _now_h, _now_m = map(int, local_time.split(':'))
        _sched_mins = _sched_h * 60 + _sched_m
        _now_mins   = _now_h * 60 + _now_m
        _delta      = _now_mins - _sched_mins
        if not (0 <= _delta <= 120):
            return False, ''
    except Exception:
        if local_time != delivery_time:
            return False, ''

    _WEEKDAY_NAMES = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    today_name = _WEEKDAY_NAMES[local_date_obj.weekday()]

    limits        = _get_tier_cfg(tier)
    delivery_days = limits.get('delivery_days', ['monday'])
    briefing_days = limits.get('briefing_days', delivery_days)

    # Legacy: premium used to have delivery_days='daily' — normalise
    if delivery_days == 'daily':
        delivery_days = briefing_days

    monday_str = _week_monday(local_date_obj)
    from users.user_store import already_tipped_today, already_sent_this_week_slot

    # Is today a setup delivery day?
    if today_name in delivery_days:
        if already_sent_this_week_slot(db_path, user_id, today_name, monday_str):
            return False, ''
        return True, today_name

    # Is today a briefing-only day (Pro Mon–Fri, Premium Mon–Sat)?
    if today_name in briefing_days:
        if already_tipped_today(db_path, user_id, local_date_str):
            return False, ''
        return True, today_name

    return False, ''


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

        # Edge gate: block pattern×timeframe combinations with negative calibration
        # edge gap (stopped_out_rate >= hit_rate on average across the universe).
        # Derived from 5.1M sample calibration dataset — see core/tiers.TIP_EDGE_GATE.
        # fvg, order_block, ifvg 1d, breaker 1h all fail this gate structurally.
        ptype = row['pattern_type']
        ptf   = row['timeframe']
        from core.tiers import tip_pattern_tf_allowed
        if not tip_pattern_tf_allowed(ptype, ptf):
            _log.debug(
                'TipScheduler: blocking %s %s %s — negative calibration edge gap',
                row['ticker'], ptype, ptf,
            )
            continue

        # Secondary: personal hit rate check (kept for per-user personalisation)
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
    from core.tiers import TIER_CONFIG as TIER_LIMITS
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


def _validate_tip(row: dict, tier: str, is_weekly: bool = False,
                   db_path: Optional[str] = None) -> tuple:
    """
    Validate a pattern row against tier quality thresholds.
    Returns (ok: bool, reason: str, warnings: list[str]).

    warnings is a list of non-blocking caution strings to surface in the tip
    (e.g. elevated put/call OI ratio conflicting with a bullish setup).
    """
    from core.tiers import get_tier as _gt
    config = _gt(tier)
    ABSOLUTE_FLOOR = 0.40
    min_asymmetry = config.get('min_asymmetry', 2.0)
    warnings: list = []

    quality = row.get('quality_score') or 0.0
    if quality < ABSOLUTE_FLOOR:
        return False, f"quality {quality:.2f} below floor {ABSOLUTE_FLOOR}", warnings

    zone_high = row.get('zone_high', 0.0)
    zone_low  = row.get('zone_low', 0.0)
    direction = row.get('direction', 'bullish')
    if zone_high > zone_low:
        entry = (zone_high + zone_low) / 2.0
        stop_dist = abs(entry - zone_low) if direction == 'bullish' else abs(zone_high - entry)
        # Use quality_score as a gate — proper asymmetry needs position calc.
        pass

    if is_weekly and row.get('timeframe') == '1h':
        return False, "1h timeframe not valid for weekly delivery", warnings

    # ── Options Greeks conviction checks (non-blocking warnings) ──────────────
    if db_path:
        ticker = row.get('ticker', '')
        try:
            import sqlite3 as _sq
            _gc = _sq.connect(db_path, timeout=5)
            try:
                # put_call_oi_ratio: heavy put buying against a bullish setup
                _pcr_row = _gc.execute(
                    "SELECT object FROM facts WHERE subject=? AND predicate='put_call_oi_ratio'"
                    " ORDER BY timestamp DESC LIMIT 1",
                    (ticker.lower(),)
                ).fetchone()
                if _pcr_row:
                    try:
                        pcr = float(_pcr_row[0])
                        if direction in ('bullish', 'long') and pcr > 1.3:
                            warnings.append(
                                f'put_call_oi_ratio={pcr:.2f} — heavy put positioning '
                                f'conflicts with bullish setup (reduce size or wait for confirmation)'
                            )
                        elif direction in ('bearish', 'short') and pcr < 0.7:
                            warnings.append(
                                f'put_call_oi_ratio={pcr:.2f} — heavy call positioning '
                                f'conflicts with bearish setup'
                            )
                    except (ValueError, TypeError):
                        pass

                # iv_true: elevated IV → widen stops, reduce size
                _iv_row = _gc.execute(
                    "SELECT object FROM facts WHERE subject=? AND predicate='iv_true'"
                    " ORDER BY timestamp DESC LIMIT 1",
                    (ticker.lower(),)
                ).fetchone()
                if _iv_row:
                    try:
                        iv = float(_iv_row[0])
                        if iv > 60:
                            warnings.append(
                                f'iv_true={iv:.1f}% — very high IV: widen stops and reduce position size'
                            )
                        elif iv > 40:
                            warnings.append(
                                f'iv_true={iv:.1f}% — elevated IV: consider wider stops'
                            )
                    except (ValueError, TypeError):
                        pass

                # gamma_exposure: negative GEX = dealers short gamma = amplified moves
                _gex_row = _gc.execute(
                    "SELECT object FROM facts WHERE subject=? AND predicate='gamma_exposure'"
                    " ORDER BY timestamp DESC LIMIT 1",
                    (ticker.lower(),)
                ).fetchone()
                if _gex_row:
                    try:
                        gex = float(_gex_row[0])
                        if gex < 0:
                            warnings.append(
                                f'gamma_exposure={gex:,.0f} — dealers short gamma: '
                                f'expect amplified moves, tighter stop management advised'
                            )
                    except (ValueError, TypeError):
                        pass

                # yield_curve: long_end_stress or bear_steepen regime conflicts with
                # bullish setups — macro headwind for rate-sensitive equities.
                # Atoms are on subject='macro', not the ticker.
                _yc_stress_row = _gc.execute(
                    "SELECT object FROM facts WHERE subject='macro' AND predicate='long_end_stress'"
                    " ORDER BY timestamp DESC LIMIT 1"
                ).fetchone()
                _yc_regime_row = _gc.execute(
                    "SELECT object FROM facts WHERE subject='macro' AND predicate='yield_curve_regime'"
                    " ORDER BY timestamp DESC LIMIT 1"
                ).fetchone()
                if direction in ('bullish', 'long'):
                    _yc_regime = _yc_regime_row[0] if _yc_regime_row else None
                    # Use graded stress level if available, fall back to boolean
                    _yc_stress_level_row = _gc.execute(
                        "SELECT object FROM facts WHERE subject='macro'"
                        " AND predicate='long_end_stress_level'"
                        " ORDER BY timestamp DESC LIMIT 1"
                    ).fetchone()
                    _yc_stress_level = _yc_stress_level_row[0] if _yc_stress_level_row else (
                        'elevated' if (_yc_stress_row and _yc_stress_row[0] == 'true') else 'none'
                    )
                    if _yc_stress_level == 'severe':
                        warnings.append(
                            'long_end_stress=severe — TLT down >1% today: '
                            'significant bond market selloff, yields spiking; '
                            'strong headwind for rate-sensitive setups — reduce size or wait'
                        )
                    elif _yc_stress_level == 'elevated':
                        warnings.append(
                            'long_end_stress=elevated — TLT down >0.5% today: '
                            'rising long-end yields are a headwind for growth/tech setups'
                        )
                    elif _yc_regime in ('bear_steepen', 'bear_flatten'):
                        warnings.append(
                            f'yield_curve_regime={_yc_regime} — rate environment '
                            f'unfavourable for high-multiple growth stocks; '
                            f'consider tighter stops or smaller size'
                        )
            finally:
                _gc.close()
        except Exception:
            pass

    return True, 'ok', warnings


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
    from core.tiers import TIER_CONFIG as TIER_LIMITS
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
            ok, reason, tip_warnings = _validate_tip(
                row, tier,
                is_weekly=is_weekly if 'is_weekly' in dir() else False,
                db_path=db_path,
            )
            used_tickers.add(row['ticker'].upper())
            remaining = [r for r in remaining if r['ticker'].upper() not in used_tickers]
            if not ok:
                _log.debug('TipScheduler: skipping %s — %s', row.get('ticker'), reason)
                continue
            _row = dict(row)
            if tip_warnings:
                _row['options_warnings'] = tip_warnings
            batch.append(_row)
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
    from core.tiers import TIER_CONFIG as TIER_LIMITS
    from notifications.tip_formatter import (
        format_tip, format_monday_briefing, format_position_monitor_briefing,
        pattern_allowed_for_tier, timeframe_allowed_for_tier, fetch_greeks,
    )
    from notifications.telegram_notifier import TelegramNotifier
    from users.user_store import (
        log_tip_delivery, mark_pattern_alerted,
        get_user_open_positions, get_recently_closed_positions,
        get_kb_changes_since, expire_stale_followups, upsert_tip_followup,
    )

    chat_id = user_prefs.get('telegram_chat_id')
    _has_telegram = bool(chat_id)

    tier              = user_prefs.get('tier', 'basic')
    trader_level      = user_prefs.get('trader_level') or 'developing'
    tip_timeframes    = user_prefs.get('tip_timeframes') or ['1h']
    tip_pattern_types = user_prefs.get('tip_pattern_types')
    tip_markets       = user_prefs.get('tip_markets')

    limits     = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    batch_size = limits.get('batch_size', 1) if weekday != 'daily' else 1
    briefing_mode = _get_briefing_mode(weekday) if weekday else 'position_monitor'
    is_weekly  = weekday in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday')

    local_now  = _get_local_now(user_prefs.get('tip_delivery_timezone', 'UTC'))
    local_date = local_now.strftime('%Y-%m-%d')
    monday_str = _week_monday(local_now.date())

    # Always initialise — week_close/weekend_summary paths reference this too
    expired_this_cycle: List[dict] = []

    if is_weekly:
        # ── Expire stale followups first — results included in message ────────
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

            # Fetch highest-priority mid-week alert per open position (since last Monday)
            recent_alerts: dict = {}
            try:
                import sqlite3 as _sq3
                _PRIORITY_ORDER = {'CRITICAL': 4, 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
                _conn = _sq3.connect(db_path, timeout=5)
                _rows = _conn.execute(
                    """SELECT followup_id, alert_type, priority
                       FROM position_alerts
                       WHERE user_id = ?
                         AND created_at >= ?
                         AND priority IN ('CRITICAL','HIGH','MEDIUM')
                       ORDER BY created_at DESC""",
                    (user_id, monday_str),
                ).fetchall()
                _conn.close()
                for _fid, _atype, _pri in _rows:
                    existing = recent_alerts.get(_fid)
                    if existing is None or _PRIORITY_ORDER.get(_pri, 0) > _PRIORITY_ORDER.get(existing[1], 0):
                        recent_alerts[_fid] = (_atype, _pri)
            except Exception as _ae:
                _log.debug('TipScheduler: mid-week alert fetch failed: %s', _ae)

            message = format_monday_briefing(
                open_positions   = open_positions,
                new_setups       = pairs,
                closed_last_week = closed_last_week + expired_this_cycle,
                tier             = tier,
                get_price_fn     = _price_fn,
                recent_alerts    = recent_alerts or None,
            )

            if _has_telegram:
                notifier = TelegramNotifier()
                sent = notifier.send(chat_id, message)
            else:
                sent = True  # in-app only — no Telegram needed

            # Auto-create watching followups for new setups
            for row, pos in pairs:
                try:
                    mark_pattern_alerted(db_path, row['id'], user_id)
                    _fid, _tcands = upsert_tip_followup(
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

        # ── POSITION MONITOR (Wed, Tue, Thu, Fri, Sat) ───────────────────────
        # briefing_mode: position_monitor | week_close | weekend_summary
        if briefing_mode in ('position_monitor', 'week_close', 'weekend_summary'):
            open_positions = []
            kb_changes = []
            closed_positions: list = []
            try:
                open_positions = get_user_open_positions(db_path, user_id)
            except Exception as _oe:
                _log.debug('TipScheduler: get_user_open_positions failed: %s', _oe)

            if open_positions:
                try:
                    from datetime import timezone as _tz
                    from datetime import datetime as _dt
                    monday_dt = _dt.strptime(monday_str, '%Y-%m-%d').replace(tzinfo=_tz.utc)
                    open_tickers = [p['ticker'] for p in open_positions]
                    kb_changes = get_kb_changes_since(db_path, monday_dt.isoformat(), tickers=open_tickers)
                except Exception as _ke:
                    _log.debug('TipScheduler: get_kb_changes_since failed: %s', _ke)

            if briefing_mode in ('week_close', 'weekend_summary'):
                try:
                    from datetime import timedelta
                    prev_monday = (local_now.date() - timedelta(days=7)).strftime('%Y-%m-%d')
                    closed_positions = get_recently_closed_positions(db_path, user_id, prev_monday)
                    expired_ids = {e['id'] for e in expired_this_cycle}
                    closed_positions = [c for c in closed_positions if c['id'] not in expired_ids]
                except Exception as _ce:
                    _log.debug('TipScheduler: get_recently_closed_positions failed: %s', _ce)

            if not open_positions and not kb_changes and not expired_this_cycle and not closed_positions:
                _log.info('TipScheduler: nothing for %s briefing for user %s', briefing_mode, user_id)
                return

            # ── Hybrid premarket narrative (Pro/Premium daily_briefing) ──────
            # Pro/Premium users get a personalised KB-grounded narrative header
            # followed by the structured position list.
            # Fallback: if narrative generation fails, use structured list only.
            from core.tiers import check_feature as _check_feature
            _use_narrative = check_feature(tier, 'daily_briefing')
            _narrative_text = ''
            if _use_narrative:
                try:
                    from notifications.premarket_briefing import generate_premarket_narrative
                    _trader_level = 'developing'
                    try:
                        import sqlite3 as _sq3
                        _pref_conn = _sq3.connect(db_path, timeout=5)
                        _pref_row = _pref_conn.execute(
                            "SELECT trader_level FROM user_preferences WHERE user_id=?",
                            (user_id,),
                        ).fetchone()
                        _pref_conn.close()
                        if _pref_row and _pref_row[0]:
                            _trader_level = _pref_row[0]
                    except Exception:
                        pass
                    _narrative_text = generate_premarket_narrative(
                        user_id        = user_id,
                        db_path        = db_path,
                        open_positions = open_positions,
                        tier           = tier,
                        trader_level   = _trader_level,
                    )
                except Exception as _ne:
                    _log.warning(
                        'TipScheduler: premarket narrative failed for user %s, '
                        'falling back to structured list: %s', user_id, _ne,
                    )

            if _narrative_text:
                # Hybrid: narrative already contains the structured list for positions
                # (premarket_briefing.generate_premarket_narrative appends it).
                # For the structured position monitor (kb_changes, expired), append
                # those below the narrative if they exist.
                _structured = format_position_monitor_briefing(
                    open_positions      = open_positions,
                    kb_changes          = kb_changes,
                    expired_this_cycle  = expired_this_cycle,
                    tier                = tier,
                    get_price_fn        = _price_fn,
                    briefing_mode       = briefing_mode,
                )
                # The narrative already includes the open-position levels panel.
                # Only append the structured section for KB changes / expired
                # positions, which the narrative doesn't cover.
                _kb_section = ''
                if kb_changes or expired_this_cycle:
                    # Extract just the KB changes + expired block from the structured msg.
                    # Heuristic: starts after the second occurrence of the divider line.
                    _div = '─────────────────────'
                    _parts = _structured.split(_div)
                    if len(_parts) >= 3:
                        _kb_section = _div + _div.join(_parts[2:])
                    elif len(_parts) == 2:
                        _kb_section = _parts[1]
                message = _narrative_text + ('\n\n' + _kb_section.strip() if _kb_section.strip() else '')
            else:
                message = format_position_monitor_briefing(
                    open_positions      = open_positions,
                    kb_changes          = kb_changes,
                    expired_this_cycle  = expired_this_cycle,
                    tier                = tier,
                    get_price_fn        = _price_fn,
                    briefing_mode       = briefing_mode,
                )

            if _has_telegram:
                notifier = TelegramNotifier()
                sent = notifier.send(chat_id, message)
            else:
                sent = True

            _pos_meta = [
                {'ticker': p['ticker'], 'direction': p.get('direction', ''),
                 'pattern_type': p.get('pattern_type', ''),
                 'entry_price': p.get('entry_price'), 'stop_loss': p.get('stop_loss'),
                 'zone_low': p.get('zone_low'), 'zone_high': p.get('zone_high')}
                for p in open_positions
            ] if open_positions else None
            log_tip_delivery(
                db_path, user_id,
                success=sent,
                pattern_signal_id=None,
                message_length=len(message),
                local_date=local_date,
                pattern_meta=_pos_meta,
            )
            if sent:
                _log.info('TipScheduler: %s briefing delivered to user %s (%d open, %d KB changes)',
                          briefing_mode, user_id, len(open_positions), len(kb_changes))
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
        tip_greeks = fetch_greeks(db_path, sig.ticker)
        message  = format_tip(
            sig, position, tier=tier, calibration=calibration,
            tip_source=tip_source, trader_level=trader_level,
            greeks=tip_greeks or None,
        )

        if _has_telegram:
            notifier = TelegramNotifier()
            sent = notifier.send(chat_id, message)
        else:
            sent = True

        mark_pattern_alerted(db_path, pattern_row['id'], user_id)
        try:
            upsert_tip_followup(
                db_path,
                user_id             = user_id,
                ticker              = sig.ticker,
                direction           = sig.direction,
                entry_price         = position.suggested_entry if position else None,
                stop_loss           = position.stop_loss if position else None,
                target_1            = position.target_1 if position else None,
                target_2            = position.target_2 if position else None,
                target_3            = position.target_3 if position else None,
                pattern_type        = sig.pattern_type,
                timeframe           = sig.timeframe,
                zone_low            = sig.zone_low,
                zone_high           = sig.zone_high,
                regime_at_entry     = pattern_row.get('kb_regime'),
                conviction_at_entry = pattern_row.get('kb_conviction'),
                initial_status      = 'watching',
            )
        except Exception as _fe:
            _log.debug('TipScheduler: followup create failed for %s: %s', sig.ticker, _fe)

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

        # Skip users with no Telegram linked — can't deliver, don't log failure
        if not prefs.get('telegram_chat_id'):
            _log.debug('TipScheduler: skipping user %s — no telegram_chat_id', user_id)
            continue

        for json_col in ('tip_timeframes', 'tip_pattern_types', 'tip_markets'):
            try:
                prefs[json_col] = json.loads(prefs[json_col]) if prefs[json_col] else None
            except (json.JSONDecodeError, TypeError):
                prefs[json_col] = None

        try:
            from notifications.notify_gate import should_notify
            from users.user_store import already_tipped_today
            should_send, weekday = should_notify(
                db_path             = db_path,
                user_id             = user_id,
                tier                = tier,
                delivery_time       = delivery_time,
                timezone_str        = timezone_str,
                dedup_fn            = already_tipped_today,
                check_briefing_days = True,
            )
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
