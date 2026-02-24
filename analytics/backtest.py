"""
analytics/backtest.py — KB-Native Cross-Sectional Backtest Engine

METHODOLOGY
===========
Two operating modes depending on how many signal snapshots exist:

FORWARD-LOOKING (snapshot_count >= 2):
  Uses the signal_snapshots table. Compares conviction tiers recorded in
  snapshot T-1 (the oldest available) against price returns computed between
  snapshot T-1 and the most recent snapshot T (price change = last_price_T /
  last_price_T1 - 1). This is a genuine forward-looking test: the signal is
  recorded BEFORE the return period.

  To accumulate data: call POST /analytics/snapshot today, then again in
  4 weeks. Each subsequent call adds another observation.

BACKWARD-LOOKING (snapshot_count < 2):
  Falls back to reading current KB atoms and using trailing return_1m/1w/3m
  atoms as proxies. This answers "given today's conviction tier, what did
  the stock return last month?" — which is the wrong causal direction.
  The response includes:
    "backward_looking": true
    "warning": "insufficient_snapshots — result is backward-looking, not predictive"

  Run POST /analytics/snapshot now. Come back in 4 weeks for the first
  forward-looking result.

ALPHA SIGNAL THRESHOLD (pre-committed, not post-hoc)
=====================================================
alpha_signal = True  iff:
    high_cohort.mean_return > low_cohort.mean_return + 1.0 (percentage points)

This threshold is fixed in code before any results were seen. Adjusting it
after seeing results would be p-hacking.

COHORTS
=======
Tickers grouped by conviction_tier × signal_quality from the EARLIER snapshot
(forward-looking) or current KB atoms (backward-looking):
    high_strong, high_all, medium_strong, medium_all, low_all, avoid

SIGNAL SNAPSHOTS TABLE
======================
    signal_snapshots (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker           TEXT NOT NULL,
        snapshot_date    TEXT NOT NULL,        -- ISO-8601 UTC
        conviction_tier  TEXT,
        signal_quality   TEXT,
        position_size_pct REAL,
        upside_pct       REAL,
        last_price       REAL,
        thesis_risk_level TEXT
    )
    UNIQUE(ticker, snapshot_date)

USAGE
=====
    from analytics.backtest import take_snapshot, run_backtest

    # Record today's state (run once, then again in 4 weeks)
    take_snapshot('trading_knowledge.db')

    # Run backtest (forward-looking once 2+ snapshots exist)
    result = run_backtest('trading_knowledge.db', window='1m')
    # or via API:
    #   POST /analytics/snapshot
    #   GET  /analytics/backtest?window=1m
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS signal_snapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT    NOT NULL,
    snapshot_date     TEXT    NOT NULL,
    conviction_tier   TEXT,
    signal_quality    TEXT,
    position_size_pct REAL,
    upside_pct        REAL,
    last_price        REAL,
    thesis_risk_level TEXT,
    UNIQUE(ticker, snapshot_date)
)
"""


def _ensure_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SNAPSHOTS)
    conn.commit()

# ── Snapshot functions ───────────────────────────────────────────────────────


def take_snapshot(db_path: str) -> dict:
    """
    Capture the current KB conviction state into signal_snapshots.

    Reads conviction_tier, signal_quality, position_size_pct, upside_pct,
    last_price, thesis_risk_level from the facts table and writes one row
    per ticker with today's UTC date (YYYY-MM-DD) as snapshot_date.

    The snapshot_date is truncated to the day so repeated calls on the same
    calendar day are idempotent (INSERT OR IGNORE).

    Returns {'inserted': N, 'skipped': M, 'snapshot_date': '...'}
    """
    snapshot_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    predicates = [
        'conviction_tier', 'signal_quality', 'position_size_pct',
        'upside_pct', 'last_price', 'thesis_risk_level',
    ]
    placeholders = ','.join('?' for _ in predicates)

    conn = sqlite3.connect(db_path)
    try:
        _ensure_snapshot_table(conn)
        c = conn.cursor()
        c.execute(f"""
            SELECT subject, predicate, object
            FROM facts
            WHERE predicate IN ({placeholders})
            ORDER BY subject, predicate
        """, predicates)

        ticker_map: Dict[str, dict] = {}
        for subject, predicate, obj in c.fetchall():
            subj = subject.lower()
            if subj not in ticker_map:
                ticker_map[subj] = {'ticker': subj.upper()}
            ticker_map[subj][predicate] = obj

        inserted = 0
        skipped  = 0
        for subj, td in ticker_map.items():
            if not td.get('conviction_tier'):
                skipped += 1
                continue
            try:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO signal_snapshots
                       (ticker, snapshot_date, conviction_tier, signal_quality,
                        position_size_pct, upside_pct, last_price, thesis_risk_level)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        td['ticker'],
                        snapshot_date,
                        td.get('conviction_tier'),
                        td.get('signal_quality'),
                        _safe_float(td.get('position_size_pct')),
                        _safe_float(td.get('upside_pct')),
                        _safe_float(td.get('last_price')),
                        td.get('thesis_risk_level'),
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except sqlite3.Error:
                skipped += 1
        conn.commit()
    finally:
        conn.close()

    return {'inserted': inserted, 'skipped': skipped, 'snapshot_date': snapshot_date}


def list_snapshots(db_path: str) -> List[str]:
    """Return sorted list of distinct snapshot_date values."""
    conn = sqlite3.connect(db_path)
    try:
        _ensure_snapshot_table(conn)
        c = conn.cursor()
        c.execute("SELECT DISTINCT snapshot_date FROM signal_snapshots ORDER BY snapshot_date")
        return [row[0] for row in c.fetchall()]
    finally:
        conn.close()


def _load_snapshot(conn: sqlite3.Connection, snapshot_date: str) -> Dict[str, dict]:
    """Load all rows for a given snapshot_date into a ticker→dict map."""
    c = conn.cursor()
    c.execute("""
        SELECT ticker, conviction_tier, signal_quality,
               position_size_pct, upside_pct, last_price, thesis_risk_level
        FROM signal_snapshots
        WHERE snapshot_date = ?
    """, (snapshot_date,))
    result: Dict[str, dict] = {}
    for row in c.fetchall():
        ticker, ct, sq, pos, up, price, risk = row
        result[ticker.upper()] = {
            'ticker':           ticker.upper(),
            'conviction_tier':  ct,
            'signal_quality':   sq,
            'position_size_pct': pos,
            'upside_pct':       up,
            'last_price':       price,
            'thesis_risk_level': risk,
        }
    return result


def _forward_return(price_start: Optional[float],
                    price_end:   Optional[float]) -> Optional[float]:
    """Percent return between two price snapshots."""
    if price_start is None or price_end is None or price_start <= 0:
        return None
    return round((price_end - price_start) / price_start * 100, 2)


# ── Pre-committed alpha threshold ─────────────────────────────────────────────
# high cohort mean return must exceed low cohort mean return by at least this
# many percentage points for alpha_signal to be True.
ALPHA_THRESHOLD_PP = 1.0

# Valid return windows → KB predicate names
_WINDOW_MAP = {
    '1w': 'return_1w',
    '1m': 'return_1m',
    '3m': 'return_3m',
}

# Conviction tier ordering for display
_TIER_ORDER = {'high': 0, 'medium': 1, 'low': 2, 'avoid': 3}
_QUALITY_ORDER = {'strong': 0, 'confirmed': 1, 'extended': 2, 'conflicted': 3, 'weak': 4}


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _cohort_stats(returns: List[float]) -> dict:
    """Compute summary statistics for a list of return values."""
    n = len(returns)
    if n == 0:
        return {
            'n': 0,
            'mean_return': None,
            'median_return': None,
            'std_return': None,
            'hit_rate': None,
            'min_return': None,
            'max_return': None,
        }
    sorted_r = sorted(returns)
    mean_r = sum(returns) / n
    median_r = sorted_r[n // 2] if n % 2 else (sorted_r[n // 2 - 1] + sorted_r[n // 2]) / 2
    variance = sum((r - mean_r) ** 2 for r in returns) / n if n > 1 else 0.0
    std_r = math.sqrt(variance)
    hit_rate = sum(1 for r in returns if r > 0) / n
    return {
        'n': n,
        'mean_return': round(mean_r, 2),
        'median_return': round(median_r, 2),
        'std_return': round(std_r, 2),
        'hit_rate': round(hit_rate, 3),
        'min_return': round(sorted_r[0], 2),
        'max_return': round(sorted_r[-1], 2),
    }


def _weighted_portfolio_return(
    ticker_data: List[dict],
    return_predicate: str,
) -> Optional[float]:
    """
    Compute position-size-weighted portfolio return for the long book
    (conviction_tier != avoid).

    Uses position_size_pct as weight. Normalises so weights sum to 1.0
    across tickers that have both a return and a position size.
    """
    weighted_sum = 0.0
    weight_total = 0.0
    for td in ticker_data:
        if td.get('conviction_tier') == 'avoid':
            continue
        ret = _safe_float(td.get(return_predicate))
        pos = _safe_float(td.get('position_size_pct'))
        if ret is None or pos is None or pos <= 0:
            continue
        weighted_sum += ret * pos
        weight_total += pos
    if weight_total <= 0:
        return None
    return round(weighted_sum / weight_total, 2)


def _vs_spy(ticker_data: List[dict], window: str) -> Optional[float]:
    """Average return_vs_spy for the long book (conviction_tier != avoid)."""
    vs_spy_pred = f'return_vs_spy_{window}' if window in ('1m', '3m') else None
    if not vs_spy_pred:
        return None
    vals = []
    for td in ticker_data:
        if td.get('conviction_tier') == 'avoid':
            continue
        v = _safe_float(td.get(vs_spy_pred))
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return round(sum(vals) / len(vals), 2)


def _build_cohorts_and_detail(
    ticker_data: List[dict],
    return_key: str,
    window: str,
) -> Tuple[dict, list]:
    """
    Shared cohort grouping and ticker detail builder.
    `return_key` is the dict key holding each ticker's return value.
    Used by both forward-looking and backward-looking paths.
    """
    vs_spy_pred = f'return_vs_spy_{window}' if window in ('1m', '3m') else None

    cohort_returns: Dict[str, List[float]] = {
        'high_strong':   [],
        'high_all':      [],
        'medium_strong': [],
        'medium_all':    [],
        'low_all':       [],
        'avoid':         [],
    }

    for td in ticker_data:
        ct  = td.get('conviction_tier', '')
        sq  = td.get('signal_quality', '')
        ret = _safe_float(td.get(return_key))
        if ret is None:
            continue
        if ct == 'high':
            cohort_returns['high_all'].append(ret)
            if sq in ('strong', 'confirmed'):
                cohort_returns['high_strong'].append(ret)
        elif ct == 'medium':
            cohort_returns['medium_all'].append(ret)
            if sq in ('strong', 'confirmed'):
                cohort_returns['medium_strong'].append(ret)
        elif ct == 'low':
            cohort_returns['low_all'].append(ret)
        elif ct == 'avoid':
            cohort_returns['avoid'].append(ret)

    cohorts = {name: _cohort_stats(rets) for name, rets in cohort_returns.items()}

    def _sort_key(td):
        ct = _TIER_ORDER.get(td.get('conviction_tier', ''), 99)
        sq = _QUALITY_ORDER.get(td.get('signal_quality', ''), 99)
        return (ct, sq)

    ticker_detail = []
    for td in sorted(ticker_data, key=_sort_key):
        entry = {
            'ticker':            td.get('ticker', ''),
            'conviction_tier':   td.get('conviction_tier'),
            'signal_quality':    td.get('signal_quality'),
            'sector':            td.get('sector'),
            'position_size_pct': _safe_float(td.get('position_size_pct')),
            'upside_pct':        _safe_float(td.get('upside_pct')),
            'return':            _safe_float(td.get(return_key)),
        }
        if vs_spy_pred and vs_spy_pred in td:
            entry[vs_spy_pred] = _safe_float(td.get(vs_spy_pred))
        ticker_detail.append(entry)

    return cohorts, ticker_detail


def _alpha_result(cohorts: dict) -> Tuple[bool, str]:
    """Compute alpha_signal and alpha_explanation from cohort stats."""
    high_mean = cohorts['high_all'].get('mean_return')
    low_mean  = cohorts['low_all'].get('mean_return')
    if high_mean is not None and low_mean is not None:
        diff = high_mean - low_mean
        passed = diff >= ALPHA_THRESHOLD_PP
        explanation = (
            f"high_all mean={high_mean:+.2f}%  low_all mean={low_mean:+.2f}%  "
            f"diff={diff:+.2f}pp  threshold={ALPHA_THRESHOLD_PP:+.1f}pp  "
            f"{'PASS' if passed else 'FAIL'}"
        )
        return passed, explanation
    return False, "Insufficient data — high_all or low_all cohort has no returns"


def run_backtest(db_path: str, window: str = '1m') -> dict:
    """
    Run the cross-sectional backtest.

    Parameters
    ----------
    db_path : str
        Path to trading_knowledge.db
    window : str
        One of '1w', '1m', '3m'. Only used in backward-looking mode.
        In forward-looking mode the return window is determined by the
        time elapsed between the two most extreme snapshots.

    Returns
    -------
    dict — always includes:
        window, methodology, backward_looking, snapshot_count,
        alpha_threshold_pp, alpha_signal, alpha_explanation,
        as_of, total_tickers, cohorts, portfolio_return,
        portfolio_vs_spy, ticker_detail

    When backward_looking=True also includes:
        warning: "insufficient_snapshots — result is backward-looking, not predictive"

    When backward_looking=False also includes:
        snapshot_start, snapshot_end, days_between_snapshots
    """
    if window not in _WINDOW_MAP:
        raise ValueError(f"window must be one of {list(_WINDOW_MAP)}, got {window!r}")

    now_iso      = datetime.now(timezone.utc).isoformat()
    snapshots    = list_snapshots(db_path)
    snapshot_count = len(snapshots)

    # ── FORWARD-LOOKING PATH (>= 2 snapshots) ─────────────────────────────────
    if snapshot_count >= 2:
        snap_start = snapshots[0]   # oldest — signals recorded here
        snap_end   = snapshots[-1]  # newest — prices read from here

        conn = sqlite3.connect(db_path)
        try:
            start_map = _load_snapshot(conn, snap_start)
            end_map   = _load_snapshot(conn, snap_end)
        finally:
            conn.close()

        # Compute forward return for each ticker present in both snapshots
        ticker_data: List[dict] = []
        for ticker, start_td in start_map.items():
            end_td = end_map.get(ticker)
            fwd_ret = _forward_return(
                start_td.get('last_price'),
                end_td.get('last_price') if end_td else None,
            )
            row = {
                'ticker':            ticker,
                'conviction_tier':   start_td.get('conviction_tier'),
                'signal_quality':    start_td.get('signal_quality'),
                'position_size_pct': start_td.get('position_size_pct'),
                'upside_pct':        start_td.get('upside_pct'),
                'sector':            None,  # not in snapshots
                'forward_return':    fwd_ret,
            }
            if fwd_ret is not None:
                ticker_data.append(row)

        # Days between snapshots (for context)
        try:
            from datetime import date as _date
            d0 = _date.fromisoformat(snap_start)
            d1 = _date.fromisoformat(snap_end)
            days_between = (d1 - d0).days
        except Exception:
            days_between = None

        cohorts, ticker_detail = _build_cohorts_and_detail(
            ticker_data, 'forward_return', window,
        )
        alpha_signal, alpha_explanation = _alpha_result(cohorts)

        port_return = _weighted_portfolio_return(ticker_data, 'forward_return')

        return {
            'window':                window,
            'methodology':           'forward_looking_snapshot',
            'methodology_note':      (
                'Returns computed as price change between the earliest and most '
                'recent signal_snapshots. Conviction tiers are taken from the '
                'EARLIER snapshot so the signal is recorded BEFORE the return '
                'period — a genuine forward-looking test.'
            ),
            'backward_looking':      False,
            'snapshot_count':        snapshot_count,
            'snapshot_start':        snap_start,
            'snapshot_end':          snap_end,
            'days_between_snapshots': days_between,
            'alpha_threshold_pp':    ALPHA_THRESHOLD_PP,
            'alpha_signal':          alpha_signal,
            'alpha_explanation':     alpha_explanation,
            'as_of':                 now_iso,
            'total_tickers':         len(ticker_data),
            'cohorts':               cohorts,
            'portfolio_return':      port_return,
            'portfolio_vs_spy':      None,  # not available without SPY price in snapshots
            'ticker_detail':         ticker_detail,
        }

    # ── BACKWARD-LOOKING FALLBACK (0 or 1 snapshot) ────────────────────────────
    return_pred = _WINDOW_MAP[window]
    vs_spy_pred = f'return_vs_spy_{window}' if window in ('1m', '3m') else None

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    predicates = ['conviction_tier', 'signal_quality', 'position_size_pct',
                  'sector', 'last_price', 'upside_pct', return_pred]
    if vs_spy_pred:
        predicates.append(vs_spy_pred)

    placeholders = ','.join('?' for _ in predicates)
    c.execute(f"""
        SELECT subject, predicate, object
        FROM facts
        WHERE predicate IN ({placeholders})
        ORDER BY subject, predicate
    """, predicates)

    ticker_map: Dict[str, dict] = {}
    for subject, predicate, obj in c.fetchall():
        subj = subject.lower()
        if subj not in ticker_map:
            ticker_map[subj] = {'ticker': subj.upper()}
        ticker_map[subj][predicate] = obj
    conn.close()

    # Map return_pred → generic 'return' key for shared builder
    ticker_data = []
    for td in ticker_map.values():
        if not td.get('conviction_tier') or td.get(return_pred) is None:
            continue
        row = dict(td)
        row['return'] = _safe_float(td.get(return_pred))
        if vs_spy_pred:
            row[vs_spy_pred] = _safe_float(td.get(vs_spy_pred))
        ticker_data.append(row)

    cohorts, ticker_detail = _build_cohorts_and_detail(ticker_data, 'return', window)

    # Re-attach vs_spy to ticker_detail entries from the row data
    if vs_spy_pred:
        td_map = {td['ticker'].upper(): td for td in ticker_data}
        for entry in ticker_detail:
            td = td_map.get(entry['ticker'].upper(), {})
            if vs_spy_pred in td:
                entry[vs_spy_pred] = td[vs_spy_pred]

    alpha_signal, alpha_explanation = _alpha_result(cohorts)
    port_return = _weighted_portfolio_return(ticker_data, 'return')
    port_vs_spy = _vs_spy(ticker_data, window)

    result = {
        'window':             window,
        'methodology':        'point_in_time_snapshot',
        'methodology_note':   (
            'Current KB atoms with trailing return_'
            f'{window} atoms as proxies. '
            'Answers "given TODAY\'s conviction tier, what did the stock return '
            f'LAST {window}?" — the signal is recorded AFTER the return period. '
            'This is backward-looking and cannot measure predictive alpha. '
            'Run POST /analytics/snapshot now and again in 4 weeks for a '
            'genuine forward-looking test.'
        ),
        'backward_looking':   True,
        'warning':            'insufficient_snapshots — result is backward-looking, not predictive',
        'snapshot_count':     snapshot_count,
        'alpha_threshold_pp': ALPHA_THRESHOLD_PP,
        'alpha_signal':       alpha_signal,
        'alpha_explanation':  alpha_explanation,
        'as_of':              now_iso,
        'total_tickers':      len(ticker_data),
        'cohorts':            cohorts,
        'portfolio_return':   port_return,
        'portfolio_vs_spy':   port_vs_spy,
        'ticker_detail':      ticker_detail,
    }
    return result
