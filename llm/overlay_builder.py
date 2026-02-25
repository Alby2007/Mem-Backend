"""
llm/overlay_builder.py — Overlay Card Assembly for Active Copilot Mode

Builds structured overlay_cards from KB atoms for the POST /chat endpoint
when overlay_mode=True. No LLM involved — pure KB lookup.

OVERLAY CARDS
=============
  signal_summary  — per-ticker conviction/signal/size/upside data
  causal_context  — macro event → affected tickers via causal graph
  stress_flag     — composite_stress summary with flag threshold

ENTITY EXTRACTION
=================
extract_tickers() uses:
  1. Regex \b[A-Z]{2,5}\b on screen_context
  2. Filter via _UPPERCASE_STOPWORDS from retrieval.py (covers RSI, GMT, etc.)
  3. Validate against known KB subjects (only real tickers pass through)
  4. Merge with explicitly provided screen_entities list
"""

from __future__ import annotations

import re
import sqlite3
from typing import Dict, List, Optional

# Import stopword list from retrieval and extend with screen-context false positives
_SCREEN_CONTEXT_STOPWORDS = {
    # Technical analysis terms
    'RSI', 'EMA', 'SMA', 'ATR', 'ADX', 'OBV', 'MACD', 'VWAP', 'BBANDS',
    # Role/event acronyms
    'ETF', 'CEO', 'CFO', 'COO', 'IPO', 'SPO', 'API',
    # Currency codes
    'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'BTC',
    # Timezones
    'GMT', 'UTC', 'EST', 'PST', 'CST', 'MST', 'BST',
    # Financial abbreviations
    'IV', 'OI', 'PE', 'PB', 'EPS', 'YOY', 'QOQ', 'TTM',
    'CAGR', 'ROE', 'ROA', 'FCF', 'DCF', 'YTD', 'MTD', 'WTD',
    # Time words
    'DAY', 'WEEK', 'DAILY', 'CHART',
}

try:
    from retrieval import _UPPERCASE_STOPWORDS as _BASE_STOPWORDS
    _UPPERCASE_STOPWORDS = _BASE_STOPWORDS | _SCREEN_CONTEXT_STOPWORDS
except ImportError:
    _UPPERCASE_STOPWORDS = _SCREEN_CONTEXT_STOPWORDS | {
        'THE', 'IS', 'AT', 'ON', 'AN', 'AND', 'OR', 'FOR', 'IN', 'OF',
        'TO', 'THAT', 'THIS', 'WITH', 'FROM', 'BY', 'ARE', 'WAS', 'BE',
        'HAS', 'HAVE', 'HAD', 'ITS', 'DO', 'DID', 'WHAT', 'HOW', 'WHY',
        'WHEN', 'WHERE', 'WHO', 'CAN', 'WILL', 'THERE', 'THEIR', 'THEY',
        'YOU', 'NOT', 'BUT', 'ALL', 'GET', 'GOT', 'NEW', 'NOW', 'OUT',
        'USE', 'WAY', 'USED', 'ALSO', 'JUST', 'INTO', 'OVER', 'COULD',
        'WOULD', 'SHOULD', 'THAN', 'THEN', 'WHICH', 'SOME', 'MORE',
    }

_TICKER_RE = re.compile(r'\b[A-Z]{2,5}\b')

_SIGNAL_PREDICATES = [
    'conviction_tier', 'signal_quality', 'position_size_pct',
    'upside_pct', 'invalidation_distance', 'invalidation_price',
    'thesis_risk_level', 'macro_confirmation', 'options_regime',
    'last_price', 'price_target', 'sector',
]

_STRESS_FLAG_THRESHOLD = 0.6


# ── Entity extraction ─────────────────────────────────────────────────────────

def _load_kb_subjects(conn: sqlite3.Connection) -> frozenset:
    """Return all known KB subjects as a frozenset of uppercase strings."""
    rows = conn.execute(
        "SELECT DISTINCT UPPER(subject) FROM facts"
    ).fetchall()
    return frozenset(r[0] for r in rows)


def extract_tickers(
    screen_context: str,
    conn: sqlite3.Connection,
    screen_entities: Optional[List[str]] = None,
) -> List[str]:
    """
    Extract ticker symbols from screen_context text.

    1. Regex \b[A-Z]{2,5}\b
    2. Filter _UPPERCASE_STOPWORDS
    3. Validate against known KB subjects
    4. Merge with explicitly provided screen_entities (bypasses stopword filter)

    Returns a deduplicated list of uppercase ticker strings.
    """
    candidates = _TICKER_RE.findall(screen_context or '')
    kb_subjects = _load_kb_subjects(conn)

    extracted = [
        t for t in candidates
        if t not in _UPPERCASE_STOPWORDS and t in kb_subjects
    ]

    # Merge explicit entities — these bypass validation since caller already knows them
    explicit = [str(e).upper().strip() for e in (screen_entities or []) if e]
    merged = list(dict.fromkeys(extracted + explicit))  # dedup, preserve order
    return merged


# ── KB atom reader ────────────────────────────────────────────────────────────

def _load_atoms_for_tickers(
    conn: sqlite3.Connection,
    tickers: List[str],
) -> Dict[str, Dict[str, str]]:
    """Load signal atoms for the given tickers. Returns { ticker_lower: {...} }."""
    if not tickers:
        return {}

    tickers_lower = [t.lower() for t in tickers]
    ph_t = ','.join('?' for _ in tickers_lower)
    ph_p = ','.join('?' for _ in _SIGNAL_PREDICATES)

    rows = conn.execute(
        f"""SELECT subject, predicate, object
            FROM facts
            WHERE LOWER(subject) IN ({ph_t})
              AND predicate IN ({ph_p})
            ORDER BY subject, predicate, confidence DESC""",
        tickers_lower + _SIGNAL_PREDICATES,
    ).fetchall()

    result: Dict[str, Dict[str, str]] = {}
    for subj, pred, obj in rows:
        s = subj.lower()
        if s not in result:
            result[s] = {}
        if pred not in result[s]:
            result[s][pred] = obj
    return result


# ── Overlay card builders ─────────────────────────────────────────────────────

def _build_signal_summary_card(
    ticker: str,
    atoms: Dict[str, str],
) -> dict:
    """Build a signal_summary card for one ticker."""

    def _f(key: str) -> Optional[float]:
        try:
            return float(atoms[key])
        except (KeyError, TypeError, ValueError):
            return None

    upside = _f('upside_pct')
    inv    = _f('invalidation_distance')
    pos    = _f('position_size_pct')

    asymmetry = None
    if upside is not None and inv is not None and inv != 0:
        asymmetry = round(abs(upside / inv), 2)

    return {
        'type':                  'signal_summary',
        'ticker':                ticker.upper(),
        'conviction_tier':       atoms.get('conviction_tier'),
        'signal_quality':        atoms.get('signal_quality'),
        'position_size_pct':     pos,
        'upside_pct':            upside,
        'invalidation_distance': inv,
        'asymmetry_ratio':       asymmetry,
        'options_regime':        atoms.get('options_regime'),
        'thesis_risk_level':     atoms.get('thesis_risk_level'),
        'macro_confirmation':    atoms.get('macro_confirmation'),
    }


def _build_causal_context_card(conn: sqlite3.Connection) -> Optional[dict]:
    """
    Build a causal_context card using the current market_regime atom.
    Returns None if causal graph module is unavailable or no regime atom exists.
    """
    try:
        from knowledge.causal_graph import traverse_causal
    except ImportError:
        return None

    row = conn.execute(
        """SELECT object FROM facts
           WHERE predicate = 'market_regime'
           ORDER BY confidence DESC LIMIT 1"""
    ).fetchone()
    if not row:
        return None

    regime = row[0]

    try:
        chain = traverse_causal(conn, regime, max_depth=2)
        affected = list({
            t.upper()
            for node in chain.get('chain', [])
            for t in node.get('affected_tickers', [])
        })[:10]
    except Exception:
        affected = []

    return {
        'type':              'causal_context',
        'event':             regime,
        'affected_tickers':  affected,
        'regime':            regime,
    }


def _build_stress_flag_card(stress_dict: Optional[dict]) -> dict:
    """Build a stress_flag card from the epistemic stress dict."""
    composite = 0.0
    if stress_dict:
        try:
            composite = float(stress_dict.get('composite_stress', 0.0))
        except (TypeError, ValueError):
            composite = 0.0

    flag = 'high_stress' if composite > _STRESS_FLAG_THRESHOLD else None
    return {
        'type':             'stress_flag',
        'composite_stress': round(composite, 3),
        'flag':             flag,
    }


# ── Main builder ──────────────────────────────────────────────────────────────

def build_overlay_cards(
    tickers: List[str],
    conn: sqlite3.Connection,
    stress_dict: Optional[dict] = None,
) -> List[dict]:
    """
    Assemble overlay_cards for the given ticker entities.

    Returns a list of typed card dicts:
      - One signal_summary per ticker (if KB atoms exist)
      - One causal_context (if causal graph is available + regime atom exists)
      - One stress_flag (always included)

    Parameters
    ----------
    tickers     Uppercase ticker symbols to build cards for
    conn        Active SQLite connection to the KB
    stress_dict Epistemic stress dict from compute_stress() — may be None
    """
    cards: List[dict] = []

    # signal_summary cards
    atoms_map = _load_atoms_for_tickers(conn, tickers)
    for ticker in tickers:
        atoms = atoms_map.get(ticker.lower(), {})
        if atoms.get('conviction_tier'):
            cards.append(_build_signal_summary_card(ticker, atoms))

    # causal_context card
    causal_card = _build_causal_context_card(conn)
    if causal_card:
        cards.append(causal_card)

    # stress_flag card (always)
    cards.append(_build_stress_flag_card(stress_dict))

    return cards
