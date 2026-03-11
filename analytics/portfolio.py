"""
analytics/portfolio.py — Portfolio-Level Aggregation

Reads per-ticker signal atoms from the KB and aggregates them into a single
portfolio-level view. No external API calls — reads only from the DB.

NOTE on total_position_pct
==========================
The sum of position_size_pct across all long names will typically exceed 100%.
This is intentional: position_size_pct is a SUGGESTED allocation for each name
independently, not a slice of a fixed portfolio. The output is a ranked menu of
conviction-weighted allocations, not a leveraged portfolio. A portfolio manager
would select a subset of names and scale positions to fit their risk budget.

USAGE
=====
    from analytics.portfolio import build_portfolio_summary
    summary = build_portfolio_summary('trading_knowledge.db')
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

_TIER_ORDER = {'high': 0, 'medium': 1, 'low': 2, 'avoid': 3}


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def build_portfolio_summary(db_path: str) -> dict:
    """
    Aggregate per-ticker KB signal atoms into a portfolio-level view.

    Returns
    -------
    dict with keys:
        as_of, long_book, avoid_book, sector_weights,
        macro_alignment, top_conviction, all_tickers
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # ── Load all relevant atoms in one pass ───────────────────────────────────
    predicates = [
        'conviction_tier', 'signal_quality', 'position_size_pct',
        'sector', 'last_price', 'upside_pct', 'macro_confirmation',
        'thesis_risk_level', 'return_1m', 'return_vs_spy_1m',
    ]
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

    # Only work with tickers that have at minimum conviction_tier
    all_tickers = [td for td in ticker_map.values() if td.get('conviction_tier')]

    # ── Split long book vs avoid ──────────────────────────────────────────────
    long_book  = [td for td in all_tickers if td.get('conviction_tier') != 'avoid']
    avoid_book = [td for td in all_tickers if td.get('conviction_tier') == 'avoid']

    # ── Long book aggregation ─────────────────────────────────────────────────
    sq_counts: Dict[str, int] = {}
    ct_counts: Dict[str, int] = {}
    total_pos_pct = 0.0
    pos_pct_count = 0

    weighted_upside_num = 0.0
    weighted_upside_den = 0.0

    for td in long_book:
        sq = td.get('signal_quality', 'unknown')
        ct = td.get('conviction_tier', 'unknown')
        sq_counts[sq] = sq_counts.get(sq, 0) + 1
        ct_counts[ct] = ct_counts.get(ct, 0) + 1

        pos = _safe_float(td.get('position_size_pct'))
        if pos is not None:
            total_pos_pct += pos
            pos_pct_count += 1

        upside = _safe_float(td.get('upside_pct'))
        if upside is not None and pos is not None and pos > 0:
            weighted_upside_num += upside * pos
            weighted_upside_den += pos

    avg_conviction_weighted_upside = (
        round(weighted_upside_num / weighted_upside_den, 1)
        if weighted_upside_den > 0 else None
    )

    long_book_summary = {
        'tickers':       len(long_book),
        'total_position_pct': round(total_pos_pct, 1),
        'total_position_pct_note': (
            'Sum of suggested allocations across all long names. '
            'Exceeds 100% because not all positions are held simultaneously — '
            'this is a ranked menu of allocations, not a leveraged portfolio.'
        ),
        'avg_conviction_weighted_upside': avg_conviction_weighted_upside,
        'conviction_tier': ct_counts,
        'signal_quality':  sq_counts,
    }

    # ── Avoid book ────────────────────────────────────────────────────────────
    avoid_names = sorted(td['ticker'] for td in avoid_book)
    avoid_book_summary = {
        'tickers': len(avoid_book),
        'names':   avoid_names,
    }

    # ── Sector weights (position-size weighted, long book only) ───────────────
    sector_pos: Dict[str, float] = {}
    sector_n:   Dict[str, int]   = {}
    for td in long_book:
        sector = td.get('sector', 'unknown')
        pos = _safe_float(td.get('position_size_pct')) or 0.0
        sector_pos[sector] = sector_pos.get(sector, 0.0) + pos
        sector_n[sector]   = sector_n.get(sector, 0) + 1

    # Express as % of total_pos_pct (so sector weights sum to ~100%)
    sector_weights = {}
    if total_pos_pct > 0:
        for sector, pos in sorted(sector_pos.items(), key=lambda x: -x[1]):
            sector_weights[sector] = {
                'position_pct_sum': round(pos, 1),
                'weight_pct': round(pos / total_pos_pct * 100, 1),
                'tickers': sector_n[sector],
            }

    # ── Macro alignment ───────────────────────────────────────────────────────
    macro_counts: Dict[str, int] = {}
    for td in long_book:
        mc = td.get('macro_confirmation', 'no_data')
        macro_counts[mc] = macro_counts.get(mc, 0) + 1

    # ── Top conviction: high tier sorted by upside, then medium sorted same ───
    def _sort_key(td):
        ct  = _TIER_ORDER.get(td.get('conviction_tier', ''), 99)
        up  = _safe_float(td.get('upside_pct')) or -999.0
        return (ct, -up)

    top_conviction = []
    for td in sorted(long_book, key=_sort_key)[:20]:
        top_conviction.append({
            'ticker':           td['ticker'],
            'conviction_tier':  td.get('conviction_tier'),
            'signal_quality':   td.get('signal_quality'),
            'sector':           td.get('sector'),
            'position_size_pct': _safe_float(td.get('position_size_pct')),
            'upside_pct':       _safe_float(td.get('upside_pct')),
            'macro_confirmation': td.get('macro_confirmation'),
            'thesis_risk_level':  td.get('thesis_risk_level'),
            'return_1m':        _safe_float(td.get('return_1m')),
            'return_vs_spy_1m': _safe_float(td.get('return_vs_spy_1m')),
        })

    # ── All tickers flat list (for completeness) ──────────────────────────────
    all_tickers_out = []
    for td in sorted(all_tickers, key=_sort_key):
        all_tickers_out.append({
            'ticker':           td['ticker'],
            'conviction_tier':  td.get('conviction_tier'),
            'signal_quality':   td.get('signal_quality'),
            'sector':           td.get('sector'),
            'position_size_pct': _safe_float(td.get('position_size_pct')),
            'upside_pct':       _safe_float(td.get('upside_pct')),
            'macro_confirmation': td.get('macro_confirmation'),
        })

    # Read current market regime atom (written by SignalEnrichmentAdapter)
    macro_regime = None
    try:
        conn2 = sqlite3.connect(db_path, timeout=5)
        row = conn2.execute(
            "SELECT object FROM facts WHERE subject='market' AND predicate='market_regime' "
            "ORDER BY confidence DESC, timestamp DESC LIMIT 1"
        ).fetchone()
        conn2.close()
        macro_regime = row[0] if row else None
    except Exception:
        pass

    return {
        'as_of':          now_iso,
        'long_book':      long_book_summary,
        'avoid_book':     avoid_book_summary,
        'sector_weights': sector_weights,
        'macro_alignment': macro_counts,
        'top_conviction': top_conviction,
        'all_tickers':    all_tickers_out,
        'macro_regime':   macro_regime,
    }
