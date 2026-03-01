"""
analytics/opportunity_engine.py — On-Demand Investment Opportunity Generation

Answers open-ended generation queries like:
  "make me a daytime trading strategy"
  "where are gaps in the market"
  "find momentum plays"
  "what sectors are rotating"
  "show me squeeze setups"
  "what's a good mean-reversion trade"
  "what macro event risk setups exist"

DESIGN
======
1. Intent classifier maps free-text query → one or more OpportunityMode values.
2. Each mode runs a pure-KB scanner (no external calls) that reads from SQLite.
3. Results are serialised as a structured context block injected into the LLM prompt
   under a new === OPPORTUNITY SCAN === header.
4. The LLM is given a generation-specific system rule (_SYSTEM_GENERATION_RULE in
   prompt_builder) that tells it how to format the output as a concrete strategy.

MODES
=====
  intraday        — high vol, tight spread, active options — names ready to move today
  momentum        — strong trend + sector tailwind + insider buy
  gap_fill        — price near 52w low, bullish signal, compressed IV → mean-reversion
  sector_rotation — sector leader/laggard from SectorRotationAdapter atoms
  squeeze         — high short_squeeze_potential + bullish signal_direction
  macro_gap       — macro_event_risk low → safe window for new positions
  mean_reversion  — overextended price (near 52w high OR low) + conflicting signal
  broad_screen    — top 10 by conviction_tier + signal_quality composite (default)

ZERO EXTERNAL CALLS — reads only from the shared KB SQLite DB.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)

# Single sentinel string written into scan context when no results are found.
# prompt_builder imports this constant to check for empty scans — never match
# on natural-language substrings which may differ across code paths.
EMPTY_SCAN_SENTINEL = 'SCAN_EMPTY: No setups found with current KB coverage.'

# ── Intent keyword map ────────────────────────────────────────────────────────
# Maps user query keywords → mode names (first match wins; order matters)

_INTENT_MAP: List[Tuple[List[str], str]] = [
    # Intraday / day trading
    (['intraday', 'day trade', 'daytime', 'day trading', 'scalp', 'scalping',
      'short term', 'short-term', 'today', 'today\'s trade', 'quick trade',
      'hourly', 'fast move', 'active trade'], 'intraday'),
    # Squeeze / short covering
    (['squeeze', 'short squeeze', 'short covering', 'short interest',
      'heavily shorted', 'high short', 'gamma squeeze'], 'squeeze'),
    # Sector rotation
    (['sector rotation', 'sector leader', 'sector laggard', 'rotate',
      'rotation', 'sector momentum', 'leading sector', 'sector trend',
      'best sector', 'hot sector', 'sector play',
      'what sector', 'which sector', 'sector play', 'sectors are',
      'sector perform', 'top sector', 'strongest sector'], 'sector_rotation'),
    # Momentum
    (['momentum', 'trending', 'breakout', 'trend following', 'strong trend',
      'bull run', 'uptrend', 'moving higher', 'strength', 'trend play',
      'follow the trend', 'trending higher', 'trending up'], 'momentum'),
    # Gap fill / mean reversion
    (['gap', 'gap fill', 'fill the gap', 'gaps in the market', 'market gap',
      'price gap', 'unfilled gap', 'gap down', 'gap up', 'gap trade'], 'gap_fill'),
    # Mean reversion
    (['mean reversion', 'oversold', 'overbought', 'bounce', 'reversal',
      'contrarian', 'overextended', 'stretched', 'revert', 'pullback trade',
      'dip buy', 'buy the dip', 'counter trend'], 'mean_reversion'),
    # Macro / event risk
    (['macro', 'fomc', 'fed', 'cpi', 'nfp', 'event risk', 'macro event',
      'before the fed', 'before cpi', 'macro window', 'safe window',
      'macro setup', 'pre-event', 'event driven'], 'macro_gap'),
    # Broad screen (catch-all)
    (['opportunity', 'opportunities', 'best trade', 'best trades', 'top setup',
      'top setups', 'where to invest', 'what to trade', 'what should i trade',
      'investment idea', 'investment ideas', 'new position', 'open position',
      'trade idea', 'best setup', 'strong setup', 'high conviction',
      'where is value', 'market opportunity'], 'broad_screen'),
]


def classify_intent(message: str) -> List[str]:
    """
    Map a free-text message to a list of opportunity modes.
    Returns ['broad_screen'] as the default if nothing matches.
    Suppresses broad_screen if any specific mode matched.
    """
    msg = message.lower()
    found = []
    for keywords, mode in _INTENT_MAP:
        if any(kw in msg for kw in keywords):
            if mode not in found:
                found.append(mode)
    if not found:
        return ['broad_screen']
    # If a specific mode was detected, drop broad_screen unless it's the only one
    _SPECIFIC_MODES = {'intraday', 'momentum', 'squeeze', 'sector_rotation',
                       'gap_fill', 'mean_reversion', 'macro_gap'}
    has_specific = any(m in _SPECIFIC_MODES for m in found)
    if has_specific and 'broad_screen' in found and len(found) > 1:
        found = [m for m in found if m != 'broad_screen']
    return found


# ── Atom loading helpers ──────────────────────────────────────────────────────

_CORE_PREDICATES = [
    'last_price', 'price_target', 'signal_direction', 'signal_quality',
    'conviction_tier', 'macro_confirmation', 'upside_pct', 'position_size_pct',
    'invalidation_distance', 'thesis_risk_level', 'volatility_regime',
    'volatility_30d', 'iv_rank', 'options_regime', 'return_1m', 'return_3m',
    'price_regime', 'low_52w', 'high_52w', 'sector',
    # New adapter atoms
    'short_squeeze_potential', 'insider_conviction', 'sector_tailwind',
    'short_interest_pct', 'days_to_cover', 'institutional_flow',
    'earnings_proximity', 'news_sentiment', 'skew_regime',
]

_MARKET_PREDICATES = [
    'market_regime', 'sector_rotation_leader', 'sector_rotation_laggard',
    'risk_appetite', 'macro_event_risk', 'next_macro_event', 'days_to_macro_event',
    'macro_event_type',
]

_ETF_SECTOR_MAP = {
    'xlk': 'technology', 'xlf': 'financials', 'xle': 'energy',
    'xlv': 'healthcare', 'xli': 'industrials', 'xlc': 'communication',
    'xly': 'consumer_discretionary', 'xlp': 'consumer_staples',
    'xlu': 'utilities', 'xlre': 'real_estate', 'xlb': 'materials',
}

_SKIP_SUBJECTS = frozenset({
    'spy', 'hyg', 'tlt', 'gld', 'uup', 'eem', 'vwo', 'iwm', 'qqq',
    'vti', 'dia', 'lqd', 'slv', 'bnd', 'uup', 'xlk', 'xlf', 'xle',
    'xlv', 'xli', 'xlc', 'xly', 'xlp', 'xlu', 'xlre', 'xlb',
    'market',
})


def _load_all_atoms(db_path: str) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Load per-ticker atoms and market-level atoms from KB.
    Returns (ticker_atoms, market_atoms).
    """
    ticker_atoms: Dict[str, Dict[str, str]] = {}
    market_atoms: Dict[str, str] = {}

    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            ph = ','.join('?' for _ in _CORE_PREDICATES)
            rows = conn.execute(
                f"""SELECT subject, predicate, object, confidence
                    FROM facts WHERE predicate IN ({ph})
                    ORDER BY subject, predicate, confidence DESC""",
                _CORE_PREDICATES,
            ).fetchall()
            for row in rows:
                subj = row['subject'].lower().strip()
                pred = row['predicate'].strip()
                if subj not in ticker_atoms:
                    ticker_atoms[subj] = {}
                if pred not in ticker_atoms[subj]:
                    ticker_atoms[subj][pred] = row['object'].strip()

            ph2 = ','.join('?' for _ in _MARKET_PREDICATES)
            mrows = conn.execute(
                f"""SELECT predicate, object FROM facts
                    WHERE subject='market' AND predicate IN ({ph2})
                    ORDER BY predicate, confidence DESC""",
                _MARKET_PREDICATES,
            ).fetchall()
            seen_m: set = set()
            for row in mrows:
                pred = row['predicate'].strip()
                if pred not in seen_m:
                    market_atoms[pred] = row['object'].strip()
                    seen_m.add(pred)

            # Also load per-ETF sector_momentum atoms
            etf_rows = conn.execute(
                """SELECT subject, predicate, object FROM facts
                   WHERE predicate='sector_momentum'
                   ORDER BY subject, confidence DESC"""
            ).fetchall()
            for row in etf_rows:
                subj = row['subject'].lower().strip()
                if subj not in ticker_atoms:
                    ticker_atoms[subj] = {}
                if 'sector_momentum' not in ticker_atoms[subj]:
                    ticker_atoms[subj]['sector_momentum'] = row['object'].strip()

        finally:
            conn.close()
    except Exception as exc:
        _logger.warning('opportunity_engine: atom load failed: %s', exc)

    return ticker_atoms, market_atoms


def _load_pattern_signals(db_path: str, tickers: Optional[List[str]] = None) -> Dict[str, dict]:
    """Load most recent open pattern signal per ticker."""
    result: Dict[str, dict] = {}
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        if tickers:
            ph = ','.join('?' * len(tickers))
            rows = conn.execute(
                f"""SELECT ticker, pattern_type, direction, zone_high, zone_low,
                           quality_score, timeframe, formed_at
                    FROM pattern_signals
                    WHERE status='open' AND ticker IN ({ph})
                    ORDER BY ticker, quality_score DESC""",
                [t.upper() for t in tickers],
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT ticker, pattern_type, direction, zone_high, zone_low,
                          quality_score, timeframe, formed_at
                   FROM pattern_signals
                   WHERE status='open'
                   ORDER BY ticker, quality_score DESC"""
            ).fetchall()
        seen: set = set()
        for row in rows:
            t = row[0].upper()
            if t not in seen:
                seen.add(t)
                result[t] = {
                    'pattern_type':  row[1],
                    'direction':     row[2],
                    'zone_high':     row[3],
                    'zone_low':      row[4],
                    'quality_score': row[5],
                    'timeframe':     row[6],
                    'formed_at':     row[7],
                }
        conn.close()
    except Exception as exc:
        _logger.debug('opportunity_engine: pattern load failed: %s', exc)
    return result


# ── Scoring helpers ───────────────────────────────────────────────────────────

_TIER_RANK = {'high': 3, 'medium': 2, 'low': 1, 'avoid': -1}
_QUALITY_RANK = {'strong': 3, 'confirmed': 2, 'extended': 1, 'conflicted': -1, 'weak': 0}
_DIRECTION_RANK = {'long': 2, 'bullish': 2, 'neutral': 0, 'bearish': -1, 'short': -1}
_SQUEEZE_RANK = {'high': 3, 'moderate': 2, 'low': 1, 'minimal': 0}
_INSIDER_RANK = {'high': 3, 'moderate': 2, 'low': 1, 'none': 0}
_TAILWIND_RANK = {'positive': 1, 'neutral': 0, 'negative': -1}


def _base_score(atoms: Dict[str, str]) -> float:
    tier    = _TIER_RANK.get(atoms.get('conviction_tier', ''), 0)
    quality = _QUALITY_RANK.get(atoms.get('signal_quality', ''), 0)
    dirn    = _DIRECTION_RANK.get(atoms.get('signal_direction', ''), 0)
    macro   = 1 if atoms.get('macro_confirmation') in ('confirmed', 'partial') else 0
    return tier * 1.5 + quality * 1.0 + dirn * 0.5 + macro * 0.5


# ── Opportunity result type ───────────────────────────────────────────────────

@dataclass
class OpportunityResult:
    ticker: str
    mode: str
    score: float
    conviction_tier: str
    signal_direction: str
    signal_quality: str
    upside_pct: str
    position_size_pct: str
    thesis: str
    rationale: str                       # why this mode triggered
    pattern: Optional[str] = None        # e.g. "FVG bullish 1d"
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass
class OpportunityScan:
    mode: str
    generated_at: str
    market_regime: str
    market_context: str                  # one-liner about macro state
    results: List[OpportunityResult]
    scan_notes: List[str]                # warnings / empty-scan reasons


# ── Mode scanner functions ────────────────────────────────────────────────────

def _scan_broad(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 10,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Top opportunities by composite conviction+quality score."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        tier = atoms.get('conviction_tier', '')
        if tier in ('', 'avoid'):
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn in ('bearish', 'short'):
            continue
        score = _base_score(atoms)
        insider = _INSIDER_RANK.get(atoms.get('insider_conviction', ''), 0)
        tailwind = _TAILWIND_RANK.get(atoms.get('sector_tailwind', ''), 0)
        score += insider * 0.3 + tailwind * 0.2
        upside = atoms.get('upside_pct', '')
        try:
            if float(upside) > 20:
                score += 0.3
        except (ValueError, TypeError):
            pass
        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        rationale_parts = []
        if tier == 'high':
            rationale_parts.append('high conviction')
        if atoms.get('signal_quality') in ('strong', 'confirmed'):
            rationale_parts.append('confirmed signal')
        if atoms.get('macro_confirmation') == 'confirmed':
            rationale_parts.append('macro confirmed')
        if atoms.get('insider_conviction') in ('high', 'moderate'):
            rationale_parts.append('insider buy')
        if atoms.get('sector_tailwind') == 'positive':
            rationale_parts.append('sector tailwind')
        if pat:
            rationale_parts.append(f'{pat["pattern_type"]} pattern detected')

        thesis = _build_thesis(ticker, atoms, upside)
        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='broad_screen', score=score,
            conviction_tier=tier,
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=upside,
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=thesis,
            rationale=' | '.join(rationale_parts) if rationale_parts else 'conviction match',
            pattern=pat_str,
            extra={k: atoms[k] for k in ('sector', 'volatility_regime', 'news_sentiment',
                                          'earnings_proximity') if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No tickers with conviction_tier > avoid found in KB. Run ingest cycle first.')
    return results[:limit], notes


def _scan_momentum(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 8,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Strong trend: bullish signal + sector tailwind + insider buy + low volatility regime."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn not in ('long', 'bullish'):
            continue
        quality = atoms.get('signal_quality', '')
        if quality not in ('strong', 'confirmed'):
            continue

        score = _base_score(atoms)
        # Momentum bonus: tailwind + insider
        score += _TAILWIND_RANK.get(atoms.get('sector_tailwind', ''), 0) * 0.5
        score += _INSIDER_RANK.get(atoms.get('insider_conviction', ''), 0) * 0.4
        # Penalise high-vol (momentum often better on moderate vol)
        if atoms.get('volatility_regime') == 'high':
            score -= 0.2

        # Require positive return_1m for true momentum
        ret1m = None
        try:
            ret1m = float(atoms.get('return_1m', 0) or 0)
        except (ValueError, TypeError):
            pass
        if ret1m is not None and ret1m < 0:
            score -= 0.5

        if score <= 0:
            continue

        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        rationale_parts = ['bullish signal', 'confirmed quality']
        if atoms.get('sector_tailwind') == 'positive':
            rationale_parts.append('sector tailwind')
        if atoms.get('insider_conviction') in ('high', 'moderate'):
            rationale_parts.append('insider conviction')
        if ret1m and ret1m > 0:
            rationale_parts.append(f'+{ret1m:.1f}% last month')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='momentum', score=score,
            conviction_tier=atoms.get('conviction_tier', ''),
            signal_direction=dirn,
            signal_quality=quality,
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=_build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
            rationale=' | '.join(rationale_parts),
            pattern=pat_str,
            extra={k: atoms[k] for k in ('return_1m', 'return_3m', 'sector_tailwind',
                                          'insider_conviction') if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No momentum setups found — need bullish signal_direction + confirmed quality.')
    return results[:limit], notes


def _scan_intraday(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 6,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Intraday: high iv_rank or active options + tight thesis_risk + pattern detected."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn in ('bearish', 'short', ''):
            continue
        tier = atoms.get('conviction_tier', '')
        if tier in ('', 'avoid', 'low'):
            continue

        iv_rank = None
        try:
            iv_rank = float(atoms.get('iv_rank', 0) or 0)
        except (ValueError, TypeError):
            pass

        vol_regime = atoms.get('volatility_regime', '')
        options_regime = atoms.get('options_regime', '')
        pat = patterns.get(ticker.upper())

        # Intraday favours: active options + compressed IV + pattern
        score = _base_score(atoms)
        if iv_rank is not None:
            if iv_rank > 50:
                score += 0.4      # active options chain
            elif iv_rank > 30:
                score += 0.2
        if options_regime == 'compressed':
            score += 0.3          # low IV = cheap options, good for intraday
        if pat:
            score += 0.5          # pattern detected = structural entry point
            if pat.get('quality_score', 0) > 0.7:
                score += 0.2
        if vol_regime in ('moderate', 'high'):
            score += 0.2          # need movement for intraday

        if score <= 1.0:
            continue

        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None
        rationale_parts = [f'conviction={tier}']
        if pat:
            rationale_parts.append(f'{pat["pattern_type"]} pattern')
        if iv_rank is not None and iv_rank > 30:
            rationale_parts.append(f'IV rank {iv_rank:.0f}%')
        if options_regime == 'compressed':
            rationale_parts.append('compressed IV = cheap options')
        if vol_regime == 'high':
            rationale_parts.append('high volatility regime')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='intraday', score=score,
            conviction_tier=tier,
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=_build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
            rationale=' | '.join(rationale_parts),
            pattern=pat_str,
            extra={k: atoms[k] for k in ('iv_rank', 'options_regime', 'volatility_regime',
                                          'skew_regime') if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No intraday setups found — need pattern + active options + conviction tier.')
    return results[:limit], notes


def _scan_squeeze(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 6,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Short squeeze: high short_squeeze_potential + bullish signal_direction."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        squeeze = atoms.get('short_squeeze_potential', '')
        if squeeze not in ('high', 'moderate'):
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn not in ('long', 'bullish', 'neutral'):
            continue

        score = _SQUEEZE_RANK.get(squeeze, 0) * 1.5
        score += _base_score(atoms)
        score += _INSIDER_RANK.get(atoms.get('insider_conviction', ''), 0) * 0.5

        dtc = atoms.get('days_to_cover', '')
        short_pct = atoms.get('short_interest_pct', '')

        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        rationale_parts = [f'squeeze={squeeze}']
        if short_pct:
            rationale_parts.append(f'short%={short_pct}')
        if dtc:
            rationale_parts.append(f'DTC={dtc}d')
        if atoms.get('insider_conviction') in ('high', 'moderate'):
            rationale_parts.append('insider buying')
        if dirn in ('long', 'bullish'):
            rationale_parts.append('bullish signal')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='squeeze', score=score,
            conviction_tier=atoms.get('conviction_tier', ''),
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=f"Short squeeze candidate: {squeeze} squeeze potential. "
                   + (f"Short interest {short_pct}%, {dtc} days to cover. " if short_pct else '')
                   + _build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
            rationale=' | '.join(rationale_parts),
            pattern=pat_str,
            extra={k: atoms[k] for k in ('short_interest_pct', 'days_to_cover',
                                          'short_squeeze_potential', 'insider_conviction')
                   if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No squeeze setups found — ShortInterestAdapter needs a successful FINRA fetch.')
    return results[:limit], notes


def _scan_gap_fill(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 6,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Gap fill: price near 52w low + bullish signal + compressed IV → bounce trade."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        regime = atoms.get('price_regime', '')
        dirn = atoms.get('signal_direction', '')
        if regime not in ('near_52w_low', 'oversold'):
            continue
        if dirn in ('bearish', 'short'):
            continue

        score = _base_score(atoms)
        # Gap fill bonus: near lows + options compressed = high asymmetry
        if atoms.get('options_regime') == 'compressed':
            score += 0.5
        # Upside matters a lot for gap fills
        try:
            upside = float(atoms.get('upside_pct', 0) or 0)
            if upside > 30:
                score += 0.4
            elif upside > 15:
                score += 0.2
        except (ValueError, TypeError):
            pass

        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        rationale_parts = ['near 52w low', f'signal={dirn}']
        if atoms.get('options_regime') == 'compressed':
            rationale_parts.append('IV compressed → cheap calls')
        upside_str = atoms.get('upside_pct', '')
        if upside_str:
            rationale_parts.append(f'{upside_str}% upside to target')
        if pat:
            rationale_parts.append(f'{pat["pattern_type"]} pattern detected')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='gap_fill', score=score,
            conviction_tier=atoms.get('conviction_tier', ''),
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=upside_str,
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=f"Price near 52-week low ({regime}). "
                   + _build_thesis(ticker, atoms, upside_str),
            rationale=' | '.join(rationale_parts),
            pattern=pat_str,
            extra={k: atoms[k] for k in ('price_regime', 'options_regime', 'iv_rank',
                                          'low_52w', 'last_price') if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No gap fill setups — need price_regime=near_52w_low + bullish signal.')
    return results[:limit], notes


def _scan_mean_reversion(
    ticker_atoms: Dict[str, Dict[str, str]],
    patterns: Dict[str, dict],
    limit: int = 6,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Mean reversion: overextended (near 52w high OR low) + conflicting/weak signal → fading."""
    results = []
    notes = []
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        regime = atoms.get('price_regime', '')
        quality = atoms.get('signal_quality', '')
        if regime not in ('near_52w_high', 'near_52w_low', 'extended'):
            continue
        if quality not in ('extended', 'conflicted', 'weak'):
            continue

        score = 1.0
        if quality == 'conflicted':
            score += 0.5
        if regime == 'near_52w_high':
            score += 0.3      # near top = higher fade probability

        try:
            upside = float(atoms.get('upside_pct', 0) or 0)
            if upside < -10:  # price above target = overextended
                score += 0.3
        except (ValueError, TypeError):
            pass

        dirn = atoms.get('signal_direction', '')
        fade_dir = 'short' if regime == 'near_52w_high' else 'long'

        rationale_parts = [f'regime={regime}', f'quality={quality}']
        if atoms.get('upside_pct'):
            rationale_parts.append(f'upside={atoms.get("upside_pct")}%')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='mean_reversion', score=score,
            conviction_tier=atoms.get('conviction_tier', ''),
            signal_direction=dirn,
            signal_quality=quality,
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=f"Price regime {regime} with {quality} signal — mean reversion setup. "
                   + f"Fade direction: {fade_dir}.",
            rationale=' | '.join(rationale_parts),
            extra={k: atoms[k] for k in ('price_regime', 'signal_quality', 'upside_pct',
                                          'high_52w', 'low_52w') if k in atoms},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No mean reversion setups — need extended/conflicted signal quality at price extremes.')
    return results[:limit], notes


def _scan_sector_rotation(
    ticker_atoms: Dict[str, Dict[str, str]],
    market_atoms: Dict[str, str],
    patterns: Dict[str, dict],
    limit: int = 8,
) -> Tuple[List[OpportunityResult], List[str]]:
    """Sector rotation: play leading sector ETF + per-ticker tailwind atoms."""
    results = []
    notes = []

    leader   = market_atoms.get('sector_rotation_leader', '')
    laggard  = market_atoms.get('sector_rotation_laggard', '')
    appetite = market_atoms.get('risk_appetite', '')

    if not leader and not laggard:
        notes.append('No sector rotation data yet — SectorRotationAdapter needs sector ETF atoms.')
        return results, notes

    # Collect tickers with positive sector_tailwind in the leading sector
    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        tailwind = atoms.get('sector_tailwind', '')
        if tailwind != 'positive':
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn in ('bearish', 'short'):
            continue
        tier = atoms.get('conviction_tier', '')
        if tier in ('', 'avoid'):
            continue

        score = _base_score(atoms)
        score += 0.5  # tailwind bonus
        if atoms.get('insider_conviction') in ('high', 'moderate'):
            score += 0.4

        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        rationale_parts = ['sector tailwind positive']
        if leader:
            rationale_parts.append(f'leading sector: {leader}')
        if appetite:
            rationale_parts.append(f'market risk appetite: {appetite}')
        if atoms.get('insider_conviction') in ('high', 'moderate'):
            rationale_parts.append('insider conviction')

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='sector_rotation', score=score,
            conviction_tier=tier,
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=_build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
            rationale=' | '.join(rationale_parts),
            pattern=pat_str,
            extra={k: atoms[k] for k in ('sector_tailwind', 'sector', 'insider_conviction')
                   if k in atoms},
        ))

    # Also include the leading sector ETF itself
    if leader:
        leader_lower = leader.replace('_', '').replace(' ', '')[:5].lower()
        etf_map = {v: k for k, v in _ETF_SECTOR_MAP.items()}
        etf_ticker = etf_map.get(leader_lower, '')
        if not etf_ticker:
            for etf, sec in _ETF_SECTOR_MAP.items():
                if sec.startswith(leader_lower[:4]):
                    etf_ticker = etf
                    break
        if etf_ticker and etf_ticker in ticker_atoms:
            etf_atoms = ticker_atoms[etf_ticker]
            score = 2.0  # ETF itself in leading sector = strong setup
            results.insert(0, OpportunityResult(
                ticker=etf_ticker.upper(), mode='sector_rotation', score=score,
                conviction_tier=etf_atoms.get('conviction_tier', 'medium'),
                signal_direction=etf_atoms.get('signal_direction', 'long'),
                signal_quality=etf_atoms.get('signal_quality', ''),
                upside_pct=etf_atoms.get('upside_pct', ''),
                position_size_pct=etf_atoms.get('position_size_pct', ''),
                thesis=f"Leading sector ETF: {leader.replace('_', ' ').title()}. "
                       + f"Sector momentum: {etf_atoms.get('sector_momentum', 'outperforming')}.",
                rationale=f'Sector rotation leader | risk_appetite={appetite}',
                extra={'sector_momentum': etf_atoms.get('sector_momentum', ''),
                       'risk_appetite': appetite},
            ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No sector rotation setups with positive tailwind found in KB.')
    return results[:limit], notes


def _scan_macro_gap(
    ticker_atoms: Dict[str, Dict[str, str]],
    market_atoms: Dict[str, str],
    patterns: Dict[str, dict],
    limit: int = 8,
) -> Tuple[List[OpportunityResult], List[str]]:
    """
    Macro event risk window: surface the best setups when macro_event_risk is
    low or medium (safe window), or name the risk if high (pre-event caution).
    """
    results = []
    notes = []

    macro_risk = market_atoms.get('macro_event_risk', 'low')
    next_event = market_atoms.get('next_macro_event', '')
    days_to    = market_atoms.get('days_to_macro_event', '')
    event_type = market_atoms.get('macro_event_type', '')

    if macro_risk == 'high':
        notes.append(
            f'Macro event risk is HIGH — {event_type.upper()} {next_event} in {days_to} days. '
            'Position sizing automatically reduced 30%. Avoid opening large new positions. '
            'Consider event-driven setups only.'
        )
        # Still surface the top 3 highest-conviction ideas as post-event plays
        for ticker, atoms in ticker_atoms.items():
            if ticker in _SKIP_SUBJECTS:
                continue
            if atoms.get('conviction_tier') != 'high':
                continue
            if atoms.get('signal_direction') not in ('long', 'bullish'):
                continue
            results.append(OpportunityResult(
                ticker=ticker.upper(), mode='macro_gap', score=_base_score(atoms) - 1.0,
                conviction_tier='high',
                signal_direction=atoms.get('signal_direction', ''),
                signal_quality=atoms.get('signal_quality', ''),
                upside_pct=atoms.get('upside_pct', ''),
                position_size_pct=atoms.get('position_size_pct', ''),
                thesis=f'Post-event setup — wait for {event_type.upper()} clarity before entry. '
                       + _build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
                rationale=f'High conviction but macro_event_risk=high — size reduced',
                extra={'macro_event_risk': macro_risk, 'next_macro_event': next_event,
                       'days_to_macro_event': days_to},
            ))
        results.sort(key=lambda r: r.score, reverse=True)
        return results[:3], notes

    # Low/medium risk — surface best setups with macro as tailwind
    notes.append(
        f'Macro event risk: {macro_risk}. '
        + (f'Next event: {event_type.upper()} on {next_event} ({days_to} days away). ' if next_event else '')
        + 'Safe window for new positions.'
    )

    for ticker, atoms in ticker_atoms.items():
        if ticker in _SKIP_SUBJECTS:
            continue
        tier = atoms.get('conviction_tier', '')
        if tier in ('', 'avoid', 'low'):
            continue
        dirn = atoms.get('signal_direction', '')
        if dirn in ('bearish', 'short'):
            continue

        score = _base_score(atoms)
        if macro_risk == 'low':
            score += 0.3       # clean window bonus
        if atoms.get('macro_confirmation') == 'confirmed':
            score += 0.4

        pat = patterns.get(ticker.upper())
        pat_str = f"{pat['pattern_type']} {pat['direction']} {pat['timeframe']}" if pat else None

        results.append(OpportunityResult(
            ticker=ticker.upper(), mode='macro_gap', score=score,
            conviction_tier=tier,
            signal_direction=dirn,
            signal_quality=atoms.get('signal_quality', ''),
            upside_pct=atoms.get('upside_pct', ''),
            position_size_pct=atoms.get('position_size_pct', ''),
            thesis=_build_thesis(ticker, atoms, atoms.get('upside_pct', '')),
            rationale=f'macro_event_risk={macro_risk} — safe entry window | '
                      + ('macro confirmed' if atoms.get('macro_confirmation') == 'confirmed' else 'position sized'),
            pattern=pat_str,
            extra={'macro_event_risk': macro_risk, 'next_macro_event': next_event},
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    if not results:
        notes.append('No setups found for macro gap window scan.')
    return results[:limit], notes


def _build_thesis(ticker: str, atoms: Dict[str, str], upside_pct: str) -> str:
    """Build a one-sentence thesis from KB atoms."""
    parts = []
    price = atoms.get('last_price', '')
    regime = atoms.get('price_regime', '').replace('_', ' ')
    dirn = atoms.get('signal_direction', '')
    tier = atoms.get('conviction_tier', '')
    quality = atoms.get('signal_quality', '')

    if price:
        parts.append(f'Price {price}')
    if regime:
        parts.append(f'({regime})')
    if dirn:
        parts.append(f'signal {dirn}')
    if tier:
        parts.append(f'{tier} conviction')
    if quality:
        parts.append(f'{quality} quality')
    if upside_pct:
        try:
            parts.append(f'{float(upside_pct):+.1f}% upside')
        except (ValueError, TypeError):
            pass
    return ' | '.join(parts) if parts else f'{ticker} — see KB for details'


# ── Market context builder ────────────────────────────────────────────────────

def _build_market_context(market_atoms: Dict[str, str]) -> str:
    """One-liner summarising current market state for the LLM."""
    parts = []
    regime = market_atoms.get('market_regime', '')
    if regime:
        parts.append(f'Market regime: {regime.replace("_", " ")}')
    leader = market_atoms.get('sector_rotation_leader', '')
    if leader:
        parts.append(f'Sector leader: {leader.replace("_", " ").title()}')
    laggard = market_atoms.get('sector_rotation_laggard', '')
    if laggard:
        parts.append(f'Laggard: {laggard.replace("_", " ").title()}')
    appetite = market_atoms.get('risk_appetite', '')
    if appetite:
        parts.append(f'Risk appetite: {appetite.replace("_", " ")}')
    macro_risk = market_atoms.get('macro_event_risk', '')
    if macro_risk:
        parts.append(f'Macro event risk: {macro_risk}')
    next_ev = market_atoms.get('next_macro_event', '')
    if next_ev:
        days = market_atoms.get('days_to_macro_event', '')
        parts.append(f'Next event: {next_ev}' + (f' ({days}d)' if days else ''))
    return ' | '.join(parts) if parts else 'Market context not yet available'


# ── Serialiser — produces the context block for the LLM prompt ───────────────

def _format_result(r: OpportunityResult, rank: int) -> str:
    lines = [f'  {rank}. {r.ticker} [{r.mode.upper()}] score={r.score:.2f}']
    lines.append(f'     Conviction: {r.conviction_tier} | Signal: {r.signal_direction} | Quality: {r.signal_quality}')
    if r.upside_pct:
        lines.append(f'     Upside: {r.upside_pct}% | Position size: {r.position_size_pct}%')
    if r.pattern:
        lines.append(f'     Pattern: {r.pattern}')
    lines.append(f'     Thesis: {r.thesis}')
    lines.append(f'     Why: {r.rationale}')
    for k, v in r.extra.items():
        if v:
            lines.append(f'     {k}: {v}')
    return '\n'.join(lines)


def format_scan_as_context(scan: OpportunityScan) -> str:
    """
    Serialise an OpportunityScan as a structured context block
    to inject into the LLM prompt under === OPPORTUNITY SCAN ===.
    """
    lines = [
        '=== OPPORTUNITY SCAN ===',
        f'Mode: {scan.mode.upper().replace("_", " ")} | Generated: {scan.generated_at[:19]}',
        f'Market: {scan.market_context}',
        '',
    ]
    if scan.scan_notes:
        for note in scan.scan_notes:
            lines.append(f'[NOTE] {note}')
        lines.append('')

    if not scan.results:
        lines.append(EMPTY_SCAN_SENTINEL)
    else:
        lines.append(f'Found {len(scan.results)} setup(s):')
        lines.append('')
        for i, r in enumerate(scan.results, 1):
            lines.append(_format_result(r, i))
            lines.append('')

    lines.append('=== END OPPORTUNITY SCAN ===')
    return '\n'.join(lines)


# ── Main entry point ──────────────────────────────────────────────────────────

def run_opportunity_scan(
    query: str,
    db_path: str = 'trading_knowledge.db',
    modes: Optional[List[str]] = None,
    limit_per_mode: int = 6,
) -> OpportunityScan:
    """
    Run an opportunity scan for a free-text query or explicit mode list.

    Args:
        query:          Free-text query string (used for intent classification).
        db_path:        Path to the SQLite KB.
        modes:          Override modes instead of classifying from query.
        limit_per_mode: Max results per mode.

    Returns:
        OpportunityScan with results and a formatted context block.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    resolved_modes = modes or classify_intent(query)
    primary_mode = resolved_modes[0]

    ticker_atoms, market_atoms = _load_all_atoms(db_path)
    patterns = _load_pattern_signals(db_path)

    market_context = _build_market_context(market_atoms)
    market_regime  = market_atoms.get('market_regime', 'no_data')

    all_results: List[OpportunityResult] = []
    all_notes: List[str] = []

    for mode in resolved_modes:
        if mode == 'broad_screen':
            r, n = _scan_broad(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'momentum':
            r, n = _scan_momentum(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'intraday':
            r, n = _scan_intraday(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'squeeze':
            r, n = _scan_squeeze(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'gap_fill':
            r, n = _scan_gap_fill(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'mean_reversion':
            r, n = _scan_mean_reversion(ticker_atoms, patterns, limit=limit_per_mode)
        elif mode == 'sector_rotation':
            r, n = _scan_sector_rotation(ticker_atoms, market_atoms, patterns, limit=limit_per_mode)
        elif mode == 'macro_gap':
            r, n = _scan_macro_gap(ticker_atoms, market_atoms, patterns, limit=limit_per_mode)
        else:
            r, n = _scan_broad(ticker_atoms, patterns, limit=limit_per_mode)

        # Deduplicate tickers across modes (keep higher-scored entry)
        existing_tickers = {res.ticker for res in all_results}
        for res in r:
            if res.ticker not in existing_tickers:
                all_results.append(res)
                existing_tickers.add(res.ticker)
        all_notes.extend(n)

    all_results.sort(key=lambda r: r.score, reverse=True)

    mode_label = '+'.join(resolved_modes) if len(resolved_modes) > 1 else primary_mode

    _logger.info(
        'opportunity_engine: mode=%s results=%d market=%s',
        mode_label, len(all_results), market_regime,
    )

    return OpportunityScan(
        mode=mode_label,
        generated_at=now_iso,
        market_regime=market_regime,
        market_context=market_context,
        results=all_results,
        scan_notes=all_notes,
    )
