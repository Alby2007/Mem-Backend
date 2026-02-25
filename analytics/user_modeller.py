"""
analytics/user_modeller.py — Portfolio Analysis → User Model

Derives a user model from portfolio holdings and current KB atoms.
No external API calls — reads from SQLite only.

INFERENCE RULES
===============
risk_tolerance:
  aggressive  — avg portfolio beta > 1.2, OR ≥40% holdings in high-beta sectors
                (technology, biotech, crypto, small_cap)
  conservative — avg portfolio beta < 0.8, OR ≥40% holdings in defensive sectors
                 (utilities, consumer_staples, healthcare)
  moderate     — everything else

holding_style:
  value      — avg upside_pct across holdings < 10% (buying at or near target)
  momentum   — avg conviction_tier score > 0.6 (mostly high/medium tier names)
  mixed      — everything else

sector_affinity:
  sectors with ≥2 holdings (or top-2 by count if all sectors have 1 holding)

concentration_risk:
  diversified  — holdings span ≥5 distinct sectors
  concentrated — fewer than 5 sectors

avg_conviction_threshold:
  mean numeric conviction tier across all holdings:
  {high: 1.0, medium: 0.67, low: 0.33, avoid: 0.0}
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

from users.user_store import upsert_user_model


# ── Constants ──────────────────────────────────────────────────────────────────

_HIGH_BETA_SECTORS = {
    'technology', 'tech', 'biotechnology', 'biotech',
    'semiconductors', 'software', 'small_cap', 'crypto',
    'consumer_discretionary', 'communication_services',
}

_DEFENSIVE_SECTORS = {
    'utilities', 'consumer_staples', 'healthcare', 'real_estate',
    'staples', 'health_care', 'reits',
}

_TIER_SCORE: Dict[str, float] = {
    'high': 1.0, 'medium': 0.67, 'low': 0.33, 'avoid': 0.0,
}


# ── KB atom reader (portfolio-scoped) ─────────────────────────────────────────

def _read_kb_atoms_for_tickers(
    conn: sqlite3.Connection,
    tickers: List[str],
) -> Dict[str, Dict[str, str]]:
    """
    Read relevant KB atoms for the given tickers.
    Returns { ticker_lower: { predicate: object } }
    """
    if not tickers:
        return {}

    tickers_lower = [t.lower() for t in tickers]
    placeholders = ','.join('?' for _ in tickers_lower)
    predicates = [
        'conviction_tier', 'upside_pct', 'sector',
        'volatility_30d', 'signal_quality', 'thesis_risk_level',
    ]
    pred_placeholders = ','.join('?' for _ in predicates)

    c = conn.cursor()
    c.execute(
        f"""SELECT subject, predicate, object
            FROM facts
            WHERE LOWER(subject) IN ({placeholders})
              AND predicate IN ({pred_placeholders})
            ORDER BY subject, predicate, confidence DESC""",
        tickers_lower + predicates,
    )

    result: Dict[str, Dict[str, str]] = {}
    for subj, pred, obj in c.fetchall():
        subj_lower = subj.lower()
        if subj_lower not in result:
            result[subj_lower] = {}
        if pred not in result[subj_lower]:
            result[subj_lower][pred] = obj
    return result


# ── Inference functions ────────────────────────────────────────────────────────

def infer_risk_tolerance(
    holdings: List[dict],
    kb_atoms: Dict[str, Dict[str, str]],
) -> str:
    """
    Infer risk_tolerance from holdings' sector distribution and KB volatility.
    Returns 'aggressive' | 'moderate' | 'conservative'.
    """
    if not holdings:
        return 'moderate'

    sectors = [
        (h.get('sector') or kb_atoms.get(h['ticker'].lower(), {}).get('sector', ''))
        .lower().replace(' ', '_')
        for h in holdings
    ]
    sectors = [s for s in sectors if s]

    n = len(sectors)
    if n == 0:
        # Fall back to volatility
        vols = []
        for h in holdings:
            ticker = h['ticker'].lower()
            v = kb_atoms.get(ticker, {}).get('volatility_30d')
            try:
                vols.append(float(v))
            except (TypeError, ValueError):
                pass
        if vols:
            avg_vol = sum(vols) / len(vols)
            if avg_vol > 35:
                return 'aggressive'
            if avg_vol < 15:
                return 'conservative'
        return 'moderate'

    high_beta_count  = sum(1 for s in sectors if s in _HIGH_BETA_SECTORS)
    defensive_count  = sum(1 for s in sectors if s in _DEFENSIVE_SECTORS)

    if high_beta_count / n >= 0.40:
        return 'aggressive'
    if defensive_count / n >= 0.40:
        return 'conservative'
    return 'moderate'


def infer_sector_affinity(holdings: List[dict]) -> List[str]:
    """
    Return sectors with ≥2 holdings. If no sector has ≥2, return top-2 by count.
    """
    if not holdings:
        return []

    counts: Dict[str, int] = {}
    for h in holdings:
        sector = (h.get('sector') or '').strip().lower()
        if sector:
            counts[sector] = counts.get(sector, 0) + 1

    affinity = [s for s, c in counts.items() if c >= 2]
    if not affinity and counts:
        sorted_sectors = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        affinity = [s for s, _ in sorted_sectors[:2]]
    return affinity


def infer_holding_style(
    holdings: List[dict],
    kb_atoms: Dict[str, Dict[str, str]],
) -> str:
    """
    Infer holding_style from upside_pct and conviction tier distribution.
    Returns 'value' | 'momentum' | 'mixed'.
    """
    if not holdings:
        return 'mixed'

    upsides: List[float] = []
    tier_scores: List[float] = []

    for h in holdings:
        ticker = h['ticker'].lower()
        atoms = kb_atoms.get(ticker, {})
        try:
            upsides.append(float(atoms['upside_pct']))
        except (KeyError, TypeError, ValueError):
            pass
        tier = atoms.get('conviction_tier', '')
        if tier in _TIER_SCORE:
            tier_scores.append(_TIER_SCORE[tier])

    avg_upside = sum(upsides) / len(upsides) if upsides else None
    avg_tier   = sum(tier_scores) / len(tier_scores) if tier_scores else None

    if avg_upside is not None and avg_upside < 10.0:
        return 'value'
    if avg_tier is not None and avg_tier > 0.6:
        return 'momentum'
    return 'mixed'


def infer_concentration_risk(holdings: List[dict]) -> str:
    """
    Return 'diversified' if holdings span ≥5 distinct sectors, else 'concentrated'.
    """
    sectors = {
        (h.get('sector') or '').strip().lower()
        for h in holdings
        if h.get('sector')
    }
    sectors.discard('')
    return 'diversified' if len(sectors) >= 5 else 'concentrated'


def score_portfolio_health(
    holdings: List[dict],
    kb_atoms: Dict[str, Dict[str, str]],
) -> dict:
    """
    Compute avg_conviction_threshold and beta proxy.

    Returns:
        {
          'avg_conviction_threshold': float,
          'portfolio_beta': float | None,
          'holdings_at_risk': [ticker, ...],   -- conviction degraded since avg_cost
          'holdings_performing': [ticker, ...] -- high conviction with strong upside
        }
    """
    tier_scores = []
    vols = []
    at_risk = []
    performing = []

    for h in holdings:
        ticker = h['ticker'].lower()
        atoms = kb_atoms.get(ticker, {})

        tier = atoms.get('conviction_tier', '')
        if tier in _TIER_SCORE:
            tier_scores.append(_TIER_SCORE[tier])
            if _TIER_SCORE[tier] < 0.34:  # low or avoid
                at_risk.append(h['ticker'].upper())
            elif tier == 'high':
                try:
                    upside = float(atoms.get('upside_pct', 0))
                    if upside > 15:
                        performing.append(h['ticker'].upper())
                except (TypeError, ValueError):
                    performing.append(h['ticker'].upper())

        v = atoms.get('volatility_30d')
        try:
            vols.append(float(v))
        except (TypeError, ValueError):
            pass

    avg_ct = round(sum(tier_scores) / len(tier_scores), 3) if tier_scores else None

    # Beta proxy: vol / 20 (SPY reference)
    portfolio_beta = None
    if vols:
        avg_vol = sum(vols) / len(vols)
        portfolio_beta = round(avg_vol / 20.0, 2)

    return {
        'avg_conviction_threshold': avg_ct,
        'portfolio_beta':           portfolio_beta,
        'holdings_at_risk':         at_risk,
        'holdings_performing':      performing,
    }


# ── Main builder ──────────────────────────────────────────────────────────────

def build_user_model(user_id: str, db_path: str) -> dict:
    """
    Derive and persist a user model from the current portfolio + KB state.

    Returns the upserted user_model dict. Works on empty portfolios —
    all inference functions degrade gracefully to safe defaults.
    """
    from users.user_store import get_portfolio  # local import avoids circular
    holdings = get_portfolio(db_path, user_id)

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        tickers = [h['ticker'] for h in holdings]
        kb_atoms = _read_kb_atoms_for_tickers(conn, tickers)
    finally:
        conn.close()

    risk_tolerance    = infer_risk_tolerance(holdings, kb_atoms)
    sector_affinity   = infer_sector_affinity(holdings)
    holding_style     = infer_holding_style(holdings, kb_atoms)
    concentration     = infer_concentration_risk(holdings)
    health            = score_portfolio_health(holdings, kb_atoms)

    return upsert_user_model(
        db_path,
        user_id,
        risk_tolerance=risk_tolerance,
        sector_affinity=sector_affinity,
        avg_conviction_threshold=health['avg_conviction_threshold'],
        holding_style=holding_style,
        portfolio_beta=health['portfolio_beta'],
        concentration_risk=concentration,
    )
