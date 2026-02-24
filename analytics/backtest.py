"""
analytics/backtest.py — KB-Native Cross-Sectional Backtest Engine

METHODOLOGY
===========
This is a POINT-IN-TIME SNAPSHOT test, not a walk-forward backtest.

The engine reads current KB atoms and computes whether tickers with higher
conviction_tier show better trailing returns than lower-conviction tickers.
The historical return windows (return_1w, return_1m, return_3m) are used as
proxies for "what happened to names the signal layer would have been long".

Limitation: this measures coherence between current signal state and recent
price history — it does NOT measure what a portfolio constructed on date X
would have returned by date X+N. A true walk-forward test requires daily
conviction snapshots which are not yet stored. See conviction_snapshot table
in analytics/snapshot.py (future work).

ALPHA SIGNAL THRESHOLD (pre-committed, not post-hoc)
=====================================================
alpha_signal = True  iff:
    high_cohort.mean_return > low_cohort.mean_return + 1.0 (percentage points)

This threshold is fixed in code. It was chosen before running any results.
Adjusting it after seeing results would be p-hacking.

COHORTS
=======
Tickers are grouped by (conviction_tier, signal_quality):
    conviction_tier : high | medium | low | avoid
    signal_quality  : strong | confirmed | weak | (other)

Primary cohorts for alpha measurement:
    high_strong   — best signal: strong quality + high conviction
    high_all      — all high conviction regardless of quality
    medium_strong — strong quality but conviction not yet high
    low_weak      — baseline: low conviction + weak signal
    avoid         — names the KB recommends avoiding

USAGE
=====
    from analytics.backtest import run_backtest
    result = run_backtest('trading_knowledge.db', window='1m')
    # or via API: GET /analytics/backtest?window=1m
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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


def run_backtest(db_path: str, window: str = '1m') -> dict:
    """
    Run the cross-sectional backtest against the current KB state.

    Parameters
    ----------
    db_path : str
        Path to trading_knowledge.db
    window : str
        One of '1w', '1m', '3m'

    Returns
    -------
    dict with keys:
        window, methodology, alpha_threshold_pp, as_of,
        cohorts, portfolio_return, portfolio_vs_spy,
        alpha_signal, alpha_explanation,
        ticker_detail (list, sorted by conviction then quality)
    """
    if window not in _WINDOW_MAP:
        raise ValueError(f"window must be one of {list(_WINDOW_MAP)}, got {window!r}")

    return_pred = _WINDOW_MAP[window]
    vs_spy_pred = f'return_vs_spy_{window}' if window in ('1m', '3m') else None
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # ── Load all relevant atoms in one pass ───────────────────────────────────
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

    # Build per-ticker dict
    ticker_map: Dict[str, dict] = {}
    for subject, predicate, obj in c.fetchall():
        subj = subject.lower()
        if subj not in ticker_map:
            ticker_map[subj] = {'ticker': subj.upper()}
        ticker_map[subj][predicate] = obj

    conn.close()

    # Filter to tickers that have at minimum: conviction_tier + return
    ticker_data = [
        td for td in ticker_map.values()
        if td.get('conviction_tier') and td.get(return_pred) is not None
    ]

    # ── Group into cohorts ────────────────────────────────────────────────────
    cohort_returns: Dict[str, List[float]] = {
        'high_strong':   [],
        'high_all':      [],
        'medium_strong': [],
        'medium_all':    [],
        'low_all':       [],
        'avoid':         [],
    }

    for td in ticker_data:
        ct = td.get('conviction_tier', '')
        sq = td.get('signal_quality', '')
        ret = _safe_float(td.get(return_pred))
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

    # ── Alpha signal (pre-committed threshold) ────────────────────────────────
    high_mean = cohorts['high_all'].get('mean_return')
    low_mean  = cohorts['low_all'].get('mean_return')

    if high_mean is not None and low_mean is not None:
        diff = high_mean - low_mean
        alpha_signal = diff >= ALPHA_THRESHOLD_PP
        alpha_explanation = (
            f"high_all mean={high_mean:+.2f}%  low_all mean={low_mean:+.2f}%  "
            f"diff={diff:+.2f}pp  threshold={ALPHA_THRESHOLD_PP:+.1f}pp  "
            f"{'PASS' if alpha_signal else 'FAIL'}"
        )
    else:
        alpha_signal = False
        alpha_explanation = "Insufficient data — high_all or low_all cohort has no returns"

    # ── Portfolio-weighted return (long book only) ────────────────────────────
    port_return = _weighted_portfolio_return(ticker_data, return_pred)
    port_vs_spy = _vs_spy(ticker_data, window)

    # ── Ticker detail (sorted by conviction tier then signal quality) ─────────
    def _sort_key(td):
        ct = _TIER_ORDER.get(td.get('conviction_tier', ''), 99)
        sq = _QUALITY_ORDER.get(td.get('signal_quality', ''), 99)
        return (ct, sq)

    ticker_detail = []
    for td in sorted(ticker_data, key=_sort_key):
        ret = _safe_float(td.get(return_pred))
        entry = {
            'ticker':          td['ticker'],
            'conviction_tier': td.get('conviction_tier'),
            'signal_quality':  td.get('signal_quality'),
            'sector':          td.get('sector'),
            'position_size_pct': _safe_float(td.get('position_size_pct')),
            'upside_pct':      _safe_float(td.get('upside_pct')),
            f'return_{window}': ret,
        }
        if vs_spy_pred:
            entry[vs_spy_pred] = _safe_float(td.get(vs_spy_pred))
        ticker_detail.append(entry)

    return {
        'window':             window,
        'methodology':        'point_in_time_snapshot',
        'methodology_note':   (
            'Current KB atoms with historical return windows as proxies. '
            'This measures coherence between current signal state and recent '
            'price history — NOT a walk-forward backtest. A walk-forward test '
            'requires daily conviction snapshots (not yet stored).'
        ),
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
