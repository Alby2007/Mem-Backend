"""
analytics/snapshot_curator.py — Personalised Snapshot Assembly

Assembles a CuratedSnapshot from:
  - User model (derived from portfolio or onboarding preferences)
  - Current KB state (conviction tiers, market regime, macro atoms)
  - Opportunity scoring (relevance to user profile)

Two paths produce identical output structure:
  Portfolio path  — uses user_models + user_portfolios
  Fallback path   — uses user_preferences (selected_sectors + selected_risk)

Zero-LLM, pure Python, reads from SQLite only.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class OpportunityCard:
    ticker: str
    thesis: str
    conviction_tier: str
    upside_pct: float
    invalidation_distance: float
    asymmetry_ratio: float
    position_size_pct: float
    relevance_reason: str
    urgency: str           # immediate / this_week / monitoring


@dataclass
class CuratedSnapshot:
    user_id: str
    generated_at: str

    # Section 1 — Portfolio health (empty when fallback path used)
    portfolio_summary: dict
    holdings_at_risk: List[str]
    holdings_performing: List[str]

    # Section 2 — Market context
    market_regime: str
    regime_implication: str
    macro_summary: str

    # Section 3 — Curated opportunities
    top_opportunities: List[OpportunityCard]
    opportunities_to_avoid: List[str]


# ── Constants ──────────────────────────────────────────────────────────────────

_RISK_PROFILE_TIER_FILTER = {
    'aggressive':   {'high', 'medium'},
    'moderate':     {'high', 'medium'},
    'conservative': {'high'},
}

_TIER_SCORE: Dict[str, float] = {
    'high': 1.0, 'medium': 0.5, 'low': 0.0, 'avoid': -1.0,
}

_REGIME_LABELS = {
    'risk_on_expansion':    'Risk-On Expansion',
    'risk_off_contraction': 'Risk-Off Contraction',
    'stagflation':          'Stagflation',
    'recovery':             'Recovery',
    'no_data':              'Unknown',
}

_URGENCY_RULES = {
    # (conviction_tier, options_regime) → urgency
    ('high',   'compressed'): 'immediate',
    ('high',   'normal'):     'immediate',
    ('medium', 'compressed'): 'this_week',
    ('medium', 'normal'):     'this_week',
}


# ── KB atom reader ────────────────────────────────────────────────────────────

def _load_all_signal_atoms(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    """
    Load signal atoms for all tickers from the KB.
    Returns { ticker_lower: { predicate: object } }
    """
    predicates = [
        'conviction_tier', 'upside_pct', 'invalidation_distance',
        'position_size_pct', 'signal_quality', 'thesis_risk_level',
        'macro_confirmation', 'sector', 'options_regime',
        'catalyst', 'last_price', 'price_target',
    ]
    ph = ','.join('?' for _ in predicates)
    rows = conn.execute(
        f"""SELECT subject, predicate, object
            FROM facts WHERE predicate IN ({ph})
            ORDER BY subject, predicate, confidence DESC""",
        predicates,
    ).fetchall()

    result: Dict[str, Dict[str, str]] = {}
    for subj, pred, obj in rows:
        s = subj.lower().strip()
        if s not in result:
            result[s] = {}
        if pred not in result[s]:
            result[s][pred] = obj
    return result


def _load_macro_atoms(conn: sqlite3.Connection) -> Dict[str, str]:
    """
    Load market_regime and key macro atoms.
    Returns { predicate_or_subject: value }
    """
    result: Dict[str, str] = {}

    # Market regime atom (stored as subject=market, predicate=market_regime)
    row = conn.execute(
        """SELECT object FROM facts
           WHERE predicate = 'market_regime'
           ORDER BY confidence DESC LIMIT 1"""
    ).fetchone()
    if row:
        result['market_regime'] = row[0]

    # Fed stance
    row = conn.execute(
        """SELECT object FROM facts
           WHERE predicate = 'central_bank_stance'
           ORDER BY confidence DESC LIMIT 1"""
    ).fetchone()
    if row:
        result['central_bank_stance'] = row[0]

    # Yield curve
    row = conn.execute(
        """SELECT object FROM facts
           WHERE predicate = 'yield_curve_spread'
           ORDER BY confidence DESC LIMIT 1"""
    ).fetchone()
    if row:
        result['yield_curve_spread'] = row[0]

    return result


# ── Opportunity scoring ───────────────────────────────────────────────────────

def _score_opportunity(
    ticker: str,
    atoms: Dict[str, str],
    sector_affinity: List[str],
    risk_tolerance: str,
    existing_tickers: List[str],
) -> float:
    """
    Score an opportunity for relevance to a user profile.
    Returns 0.0–2.0 (not normalised — used for ranking only).
    """
    tier = atoms.get('conviction_tier', '')
    score = _TIER_SCORE.get(tier, 0.0)
    if score < 0:
        return score

    # Sector match
    sector = (atoms.get('sector') or '').lower().replace(' ', '_')
    if sector and sector in sector_affinity:
        score += 0.30

    # Adjacent to existing holding (same sector)
    existing_sectors = set()  # populated by caller when available
    if sector and sector in existing_sectors:
        score += 0.20

    # Options regime
    options_regime = atoms.get('options_regime', '')
    if options_regime == 'compressed':
        score += 0.15
    elif options_regime == 'normal':
        score += 0.05

    # Risk profile match
    thesis_risk = atoms.get('thesis_risk_level', '')
    if risk_tolerance == 'conservative' and thesis_risk == 'wide':
        score += 0.10
    elif risk_tolerance == 'aggressive' and thesis_risk in ('moderate', 'wide'):
        score += 0.05

    # Macro confirmation
    macro_conf = atoms.get('macro_confirmation', '')
    if macro_conf == 'confirmed':
        score += 0.10
    elif macro_conf == 'partial':
        score += 0.05

    return score


def _build_opportunity_card(
    ticker: str,
    atoms: Dict[str, str],
    sector_affinity: List[str],
    existing_tickers: List[str],
) -> OpportunityCard:
    """Build an OpportunityCard from KB atoms."""
    try:
        upside = float(atoms.get('upside_pct', 0))
    except (TypeError, ValueError):
        upside = 0.0

    try:
        inv_dist = float(atoms.get('invalidation_distance', 0))
    except (TypeError, ValueError):
        inv_dist = 0.0

    try:
        pos_size = float(atoms.get('position_size_pct', 0))
    except (TypeError, ValueError):
        pos_size = 0.0

    asymmetry = round(abs(upside / inv_dist), 2) if inv_dist and inv_dist != 0 else 0.0

    conviction_tier = atoms.get('conviction_tier', '')
    options_regime  = atoms.get('options_regime', '')
    urgency = _URGENCY_RULES.get((conviction_tier, options_regime), 'monitoring')

    # Thesis: prefer catalyst atom, fall back to upside summary
    catalyst = atoms.get('catalyst', '')
    if catalyst:
        thesis = catalyst[:120]
    else:
        thesis = f"{upside:+.1f}% upside, {inv_dist:.1f}% invalidation" if upside else 'See KB for details'

    # Relevance reason
    sector = (atoms.get('sector') or '').lower().replace(' ', '_')
    if sector and sector in sector_affinity:
        relevance = f"{sector.replace('_', ' ').title()} affinity"
    elif ticker.lower() in [t.lower() for t in existing_tickers]:
        relevance = 'In your portfolio'
    else:
        relevance = f"{conviction_tier.title()} conviction match"

    return OpportunityCard(
        ticker=ticker.upper(),
        thesis=thesis,
        conviction_tier=conviction_tier,
        upside_pct=round(upside, 2),
        invalidation_distance=round(inv_dist, 2),
        asymmetry_ratio=asymmetry,
        position_size_pct=round(pos_size, 2),
        relevance_reason=relevance,
        urgency=urgency,
    )


# ── Regime implication ────────────────────────────────────────────────────────

def _regime_implication(
    regime: str,
    sector_affinity: List[str],
) -> str:
    """Return a personalised one-liner about what the regime means for the user."""
    label = _REGIME_LABELS.get(regime, regime)
    if not sector_affinity:
        return f"Regime: {label}"

    sector_str = ' and '.join(s.replace('_', ' ').title() for s in sector_affinity[:2])

    if regime == 'risk_on_expansion':
        return f"{label} favours your {sector_str} holdings"
    elif regime == 'risk_off_contraction':
        return f"{label} — defensive positioning advised; review your {sector_str} exposure"
    elif regime == 'stagflation':
        return f"{label} — real assets may outperform; monitor {sector_str} margins"
    elif regime == 'recovery':
        return f"{label} — early-cycle names in {sector_str} may benefit"
    return f"Regime: {label}"


def _macro_summary(macro: Dict[str, str]) -> str:
    """Build a one-line macro summary string."""
    parts = []
    if macro.get('central_bank_stance'):
        parts.append(f"Fed: {macro['central_bank_stance'].replace('_', ' ')}")
    if macro.get('yield_curve_spread'):
        parts.append(f"Yield curve: {macro['yield_curve_spread']}")
    return ' | '.join(parts) if parts else 'No macro data available'


# ── Portfolio health section ──────────────────────────────────────────────────

def _portfolio_health_section(
    holdings: List[dict],
    all_atoms: Dict[str, Dict[str, str]],
) -> dict:
    """
    Build the portfolio_summary dict, holdings_at_risk, and holdings_performing.
    """
    summary = []
    at_risk = []
    performing = []

    for h in holdings:
        ticker = h['ticker'].upper()
        atoms  = all_atoms.get(ticker.lower(), {})
        tier   = atoms.get('conviction_tier', 'no_data')
        try:
            upside = float(atoms.get('upside_pct', 0))
        except (TypeError, ValueError):
            upside = 0.0

        summary.append({
            'ticker':          ticker,
            'conviction_tier': tier,
            'upside_pct':      round(upside, 2),
            'avg_cost':        h.get('avg_cost'),
        })

        tier_score = _TIER_SCORE.get(tier, 0.0)
        if tier_score < 0.34:
            at_risk.append(ticker)
        elif tier == 'high' and upside > 15:
            performing.append(ticker)

    return {
        'summary':            summary,
        'holdings_at_risk':   at_risk,
        'holdings_performing': performing,
    }


# ── Main curator ──────────────────────────────────────────────────────────────

def curate_snapshot(user_id: str, db_path: str) -> CuratedSnapshot:
    """
    Assemble a personalised CuratedSnapshot for a user.

    Uses portfolio-derived user model when available;
    falls back to onboarding preferences otherwise.
    """
    from users.user_store import (
        get_user_model, get_portfolio, get_user, ensure_user_tables
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path, timeout=15)
    try:
        ensure_user_tables(conn)

        # ── Determine user profile ─────────────────────────────────────────────
        user_model   = get_user_model(db_path, user_id)
        user_prefs   = get_user(db_path, user_id)
        holdings     = get_portfolio(db_path, user_id)

        # Resolve profile fields (model takes priority over prefs)
        if user_model:
            risk_tolerance  = user_model.get('risk_tolerance', 'moderate')
            sector_affinity = user_model.get('sector_affinity', [])
        elif user_prefs:
            risk_tolerance  = user_prefs.get('selected_risk', 'moderate')
            sector_affinity = user_prefs.get('selected_sectors', [])
        else:
            risk_tolerance  = 'moderate'
            sector_affinity = []

        # ── Load KB state ──────────────────────────────────────────────────────
        all_atoms = _load_all_signal_atoms(conn)
        macro     = _load_macro_atoms(conn)

        # ── Market context ─────────────────────────────────────────────────────
        regime       = macro.get('market_regime', 'no_data')
        regime_impl  = _regime_implication(regime, sector_affinity)
        macro_str    = _macro_summary(macro)

        # ── Portfolio health (portfolio path only) ─────────────────────────────
        health = _portfolio_health_section(holdings, all_atoms) if holdings else {
            'summary': [], 'holdings_at_risk': [], 'holdings_performing': [],
        }
        existing_tickers = [h['ticker'].upper() for h in holdings]

        # ── Curate opportunities ───────────────────────────────────────────────
        allowed_tiers = _RISK_PROFILE_TIER_FILTER.get(risk_tolerance, {'high', 'medium'})
        avoid_list    = []
        candidates    = []

        for ticker_lower, atoms in all_atoms.items():
            tier = atoms.get('conviction_tier', '')
            if not tier:
                continue

            # Skip macro proxies / ETFs with no upside
            if ticker_lower in ('spy', 'hyg', 'tlt', 'gld', 'uup', 'eem',
                                 'vwo', 'iwm', 'qqq'):
                continue

            if tier == 'avoid':
                # Only surface avoids that are in the user's portfolio
                if ticker_lower.upper() in existing_tickers:
                    avoid_list.append(ticker_lower.upper())
                continue

            if tier not in allowed_tiers:
                continue

            score = _score_opportunity(
                ticker_lower, atoms, sector_affinity, risk_tolerance, existing_tickers
            )
            candidates.append((ticker_lower, atoms, score))

        # Sort by score desc, take top 5
        candidates.sort(key=lambda x: x[2], reverse=True)
        top_candidates = candidates[:5]

        top_opportunities = [
            _build_opportunity_card(t, a, sector_affinity, existing_tickers)
            for t, a, _ in top_candidates
        ]

    finally:
        conn.close()

    return CuratedSnapshot(
        user_id=user_id,
        generated_at=now_iso,
        portfolio_summary=health['summary'],
        holdings_at_risk=health['holdings_at_risk'],
        holdings_performing=health['holdings_performing'],
        market_regime=regime,
        regime_implication=regime_impl,
        macro_summary=macro_str,
        top_opportunities=top_opportunities,
        opportunities_to_avoid=avoid_list,
    )
