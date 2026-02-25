"""
knowledge/causal_graph.py — Causal Graph Layer

Stores and traverses directed causal relationships between market concepts.
Unlike the undirected correlation edges in graph_retrieval.py, these edges
represent cause → effect relationships with a mechanism label.

ARCHITECTURE
============
    causal_edges table  — persistent storage for all causal edges
    SEED_EDGES          — ~30 hardcoded macro causal chains, version-controlled
    traverse_causal()   — BFS from seed event, depth-limited, returns chain
    add_causal_edge()   — runtime extension via POST /kb/causal-edge
    get_affected_tickers() — terminal-node → live KB ticker resolution

SEED EDGES (hardcoded, not migration)
======================================
Seeded at DB init via ensure_causal_edges_table(). Each call is idempotent
(INSERT OR IGNORE on cause+effect+mechanism). Extend at runtime via
POST /kb/causal-edge.

TRAVERSAL
=========
BFS from a seed concept up to max_depth hops. Each hop follows directed
cause→effect edges. The returned chain is a list of hops:
    [
        { "step": 1, "cause": "fed_rate_hike", "effect": "credit_cost_rises",
          "mechanism": "debt_service_transmission", "confidence": 0.85 },
        ...
    ]
The chain terminates at depth or when no outbound edges exist.

AFFECTED TICKERS
================
Terminal nodes are matched against live KB subjects — tickers whose
current atoms mention the terminal concept. This connects abstract macro
causal chains to specific portfolio names.

Zero-LLM, pure Python, <5ms per call on typical graphs.
"""

from __future__ import annotations

import sqlite3
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_CAUSAL_EDGES = """
CREATE TABLE IF NOT EXISTS causal_edges (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cause        TEXT    NOT NULL,
    effect       TEXT    NOT NULL,
    mechanism    TEXT,
    confidence   REAL    DEFAULT 0.7,
    source       TEXT    DEFAULT 'hardcoded_macro',
    created_at   TEXT,
    UNIQUE(cause, effect, mechanism)
)
"""


# ── Seed macro causal edges (hardcoded, auditable in git) ─────────────────────
# Format: (cause, effect, mechanism, confidence)
# These represent well-established macro transmission channels.

SEED_EDGES: List[Tuple[str, str, str, float]] = [
    # ── Monetary policy transmission ─────────────────────────────────────────
    ("fed_rate_hike",        "credit_cost_rises",         "debt_service_transmission",   0.90),
    ("fed_rate_hike",        "risk_free_rate_rises",      "treasury_yield_transmission",  0.95),
    ("fed_rate_hike",        "dollar_strengthens",        "carry_trade_reversal",         0.80),
    ("fed_rate_hike",        "hyg_spreads_widen",         "credit_risk_repricing",        0.85),
    ("fed_rate_cut",         "credit_cost_falls",         "debt_service_transmission",    0.90),
    ("fed_rate_cut",         "risk_free_rate_falls",      "treasury_yield_transmission",  0.95),
    ("fed_rate_cut",         "dollar_weakens",            "carry_trade_reversal",         0.80),
    ("fed_rate_cut",         "hyg_spreads_tighten",       "credit_risk_repricing",        0.85),

    # ── Credit channel ────────────────────────────────────────────────────────
    ("credit_cost_rises",    "equity_multiples_compress", "dcf_discount_rate_expansion",  0.85),
    ("credit_cost_falls",    "equity_multiples_expand",   "dcf_discount_rate_compression",0.85),
    ("hyg_spreads_widen",    "risk_off_rotation",         "credit_stress_contagion",      0.80),
    ("hyg_spreads_tighten",  "risk_on_rotation",          "credit_confidence_signal",     0.80),

    # ── Yield curve dynamics ──────────────────────────────────────────────────
    ("risk_free_rate_rises", "yield_curve_steepens",      "term_premium_expansion",       0.75),
    ("risk_free_rate_falls", "yield_curve_flattens",      "term_premium_compression",     0.75),
    ("yield_curve_inverts",  "recession_probability_rises","historical_leading_indicator", 0.70),
    ("yield_curve_steepens", "financials_net_interest_margin_expands", "banking_spread_transmission", 0.80),

    # ── Dollar / FX channel ───────────────────────────────────────────────────
    ("dollar_strengthens",   "em_equity_outflows",        "fx_carry_unwind",              0.80),
    ("dollar_strengthens",   "commodities_decline",       "dollar_denominated_repricing", 0.75),
    ("dollar_weakens",       "commodities_rise",          "dollar_denominated_repricing", 0.75),
    ("dollar_weakens",       "em_equity_inflows",         "fx_carry_build",               0.75),

    # ── Inflation channel ─────────────────────────────────────────────────────
    ("commodities_rise",     "inflation_rises",           "cost_push_transmission",       0.80),
    ("inflation_rises",      "fed_rate_hike",             "central_bank_reaction_function",0.85),
    ("inflation_rises",      "real_wage_compresses",      "consumer_purchasing_power",    0.75),
    ("real_wage_compresses", "consumer_discretionary_demand_falls", "household_income_effect", 0.70),

    # ── Sector rotation channel ───────────────────────────────────────────────
    ("risk_off_rotation",    "defensives_outperform",     "flight_to_quality",            0.80),
    ("risk_on_rotation",     "cyclicals_outperform",      "growth_risk_appetite",         0.80),
    ("equity_multiples_compress","growth_stocks_underperform","duration_sensitivity",     0.85),
    ("equity_multiples_expand",  "growth_stocks_outperform", "duration_sensitivity",     0.85),

    # ── Banking sector specific ───────────────────────────────────────────────
    ("yield_curve_inverts",  "bank_nii_compresses",       "liability_repricing_faster",   0.75),
    ("hyg_spreads_widen",    "bank_loan_loss_reserves_rise","credit_cycle_transmission",  0.70),

    # ── Geopolitical / energy channel ─────────────────────────────────────────
    ("energy_prices_rise",   "inflation_rises",           "cost_push_energy",             0.80),
    ("energy_prices_rise",   "energy_sector_outperforms", "direct_revenue_transmission",  0.90),
    ("energy_prices_fall",   "consumer_discretionary_demand_rises", "disposable_income_effect", 0.70),
]

# Mapping from terminal causal nodes to KB subjects/tickers that are affected.
# When a causal chain terminates at one of these nodes, the listed tickers
# are flagged as potentially affected in get_affected_tickers().
_NODE_TO_TICKERS: Dict[str, List[str]] = {
    "equity_multiples_compress":    ["QQQ", "MSFT", "GOOGL", "AMZN", "NVDA", "META"],
    "equity_multiples_expand":      ["QQQ", "MSFT", "GOOGL", "AMZN", "NVDA", "META"],
    "growth_stocks_underperform":   ["QQQ", "MSFT", "GOOGL", "AMZN", "NVDA", "META"],
    "growth_stocks_outperform":     ["QQQ", "MSFT", "GOOGL", "AMZN", "NVDA", "META"],
    "financials_net_interest_margin_expands": ["JPM", "BAC", "GS", "MS", "WFC", "C"],
    "bank_nii_compresses":          ["JPM", "BAC", "GS", "MS", "WFC", "C"],
    "bank_loan_loss_reserves_rise": ["JPM", "BAC", "WFC", "C"],
    "defensives_outperform":        ["JNJ", "PG", "KO", "WMT", "PFE"],
    "cyclicals_outperform":         ["CAT", "HON", "DE", "F", "GM"],
    "energy_sector_outperforms":    ["XOM", "CVX", "SLB", "OXY"],
    "consumer_discretionary_demand_falls": ["AMZN", "HD", "NKE", "SBUX", "MCD"],
    "consumer_discretionary_demand_rises": ["AMZN", "HD", "NKE", "SBUX", "MCD"],
    "em_equity_outflows":           ["EEM", "VWO"],
    "em_equity_inflows":            ["EEM", "VWO"],
    "commodities_rise":             ["XOM", "CVX", "GLD", "SLB"],
    "commodities_decline":          ["XOM", "CVX", "GLD", "SLB"],
    "risk_off_rotation":            ["TLT", "GLD", "JNJ", "PG"],
    "risk_on_rotation":             ["SPY", "QQQ", "HYG", "IWM"],
}


# ── Schema management ─────────────────────────────────────────────────────────

def ensure_causal_edges_table(conn: sqlite3.Connection) -> None:
    """
    Create the causal_edges table if absent and seed it with SEED_EDGES.
    Safe to call on every startup — seeds use INSERT OR IGNORE.
    """
    conn.execute(_CREATE_CAUSAL_EDGES)
    conn.commit()

    now_iso = datetime.now(timezone.utc).isoformat()
    for cause, effect, mechanism, confidence in SEED_EDGES:
        conn.execute(
            """INSERT OR IGNORE INTO causal_edges
               (cause, effect, mechanism, confidence, source, created_at)
               VALUES (?, ?, ?, ?, 'hardcoded_macro', ?)""",
            (cause, effect, mechanism, confidence, now_iso),
        )
    conn.commit()


# ── Graph loading ─────────────────────────────────────────────────────────────

def _load_adjacency(
    conn: sqlite3.Connection,
    min_confidence: float = 0.0,
) -> Dict[str, List[dict]]:
    """
    Load all causal edges into a directed adjacency dict:
        { cause_concept: [ { effect, mechanism, confidence, source } ] }
    """
    c = conn.cursor()
    c.execute("""
        SELECT cause, effect, mechanism, confidence, source
        FROM causal_edges
        WHERE confidence >= ?
        ORDER BY confidence DESC
    """, (min_confidence,))

    adj: Dict[str, List[dict]] = {}
    for cause, effect, mechanism, confidence, source in c.fetchall():
        if cause not in adj:
            adj[cause] = []
        adj[cause].append({
            'effect':     effect,
            'mechanism':  mechanism,
            'confidence': confidence,
            'source':     source,
        })
    return adj


# ── BFS causal chain traversal ────────────────────────────────────────────────

def traverse_causal(
    conn: sqlite3.Connection,
    seed: str,
    max_depth: int = 4,
    min_confidence: float = 0.5,
) -> dict:
    """
    BFS traversal from a seed concept through the causal graph.

    Parameters
    ----------
    conn            open sqlite3 connection
    seed            starting concept (e.g. 'fed_rate_hike')
    max_depth       maximum hop depth (default 4)
    min_confidence  minimum edge confidence to follow (default 0.5)

    Returns
    -------
    {
        "seed":            str,
        "max_depth":       int,
        "min_confidence":  float,
        "chain":           [ { step, cause, effect, mechanism, confidence, depth } ],
        "concepts_reached":[ str ],            -- all unique concepts reached
        "affected_tickers":{ concept: [tickers] },
        "chain_confidence":float,              -- product of confidences along longest path
        "paths":           int,                -- number of distinct terminal nodes reached
    }
    """
    adj = _load_adjacency(conn, min_confidence=min_confidence)

    seed_norm = seed.lower().replace(' ', '_')

    # BFS: queue items = (concept, depth)
    visited_edges: set = set()      # (cause, effect, mechanism) — prevents duplicate hops
    queued_concepts: set = set()    # concepts already placed on queue — prevents re-queuing
    chain: List[dict] = []
    concepts_reached: set = set()
    concepts_reached.add(seed_norm)
    queued_concepts.add(seed_norm)

    queue: deque = deque()
    queue.append((seed_norm, 0))

    while queue:
        concept, depth = queue.popleft()
        if depth >= max_depth:
            continue

        for edge in adj.get(concept, []):
            effect    = edge['effect']
            mechanism = edge['mechanism']
            edge_key  = (concept, effect, mechanism)

            if edge_key in visited_edges:
                continue
            visited_edges.add(edge_key)

            chain.append({
                'step':       len(chain) + 1,
                'depth':      depth + 1,
                'cause':      concept,
                'effect':     effect,
                'mechanism':  mechanism,
                'confidence': edge['confidence'],
                'source':     edge['source'],
            })
            concepts_reached.add(effect)

            if effect not in queued_concepts:
                queued_concepts.add(effect)
                queue.append((effect, depth + 1))

    # Affected tickers — map terminal concepts to known tickers
    affected: Dict[str, List[str]] = {}
    terminal_concepts = {hop['effect'] for hop in chain}
    for concept in terminal_concepts:
        tickers = _NODE_TO_TICKERS.get(concept)
        if tickers:
            affected[concept] = tickers

    # Also check live KB for tickers that mention any reached concept
    live_tickers = _find_live_kb_tickers(conn, list(terminal_concepts))
    for concept, tickers in live_tickers.items():
        if concept in affected:
            existing = set(affected[concept])
            affected[concept] = list(existing | set(tickers))
        else:
            affected[concept] = tickers

    # Chain confidence = product of max-confidence path (greedy)
    chain_confidence = _compute_chain_confidence(chain, seed_norm)

    return {
        'seed':             seed_norm,
        'max_depth':        max_depth,
        'min_confidence':   min_confidence,
        'chain':            chain,
        'concepts_reached': sorted(concepts_reached - {seed_norm}),
        'affected_tickers': affected,
        'chain_confidence': round(chain_confidence, 4),
        'paths':            len({hop['effect'] for hop in chain if
                                 hop['effect'] not in {h['cause'] for h in chain}}),
    }


def _compute_chain_confidence(chain: List[dict], seed: str) -> float:
    """
    Compute the product of confidences along the highest-confidence path
    from the seed. Uses greedy selection of highest-confidence outbound edge
    at each step.
    """
    if not chain:
        return 1.0

    # Build adjacency from chain hops only
    adj: Dict[str, List[dict]] = {}
    for hop in chain:
        cause = hop['cause']
        if cause not in adj:
            adj[cause] = []
        adj[cause].append(hop)

    # Greedy path from seed
    current = seed
    product = 1.0
    visited = {current}
    max_hops = len(chain) + 1

    for _ in range(max_hops):
        edges = adj.get(current, [])
        if not edges:
            break
        best = max(edges, key=lambda e: e['confidence'])
        if best['effect'] in visited:
            break
        product *= best['confidence']
        visited.add(best['effect'])
        current = best['effect']

    return product


def _find_live_kb_tickers(
    conn: sqlite3.Connection,
    concepts: List[str],
) -> Dict[str, List[str]]:
    """
    Search the facts table for tickers that have atoms mentioning any of
    the terminal concepts. Returns { concept: [ticker, ...] }.
    """
    if not concepts:
        return {}

    try:
        c = conn.cursor()
        result: Dict[str, List[str]] = {}
        for concept in concepts[:20]:  # cap to avoid N+1 explosion
            c.execute("""
                SELECT DISTINCT subject FROM facts
                WHERE (LOWER(object) LIKE ? OR LOWER(predicate) LIKE ?)
                  AND subject != 'market'
                  AND LENGTH(subject) <= 6
                LIMIT 10
            """, (f'%{concept.replace("_", " ")}%', f'%{concept}%'))
            tickers = [r[0].upper() for r in c.fetchall()]
            if tickers:
                result[concept] = tickers
        return result
    except Exception:
        return {}


# ── Edge management ───────────────────────────────────────────────────────────

def add_causal_edge(
    conn: sqlite3.Connection,
    cause: str,
    effect: str,
    mechanism: str,
    confidence: float = 0.7,
    source: str = 'user_defined',
) -> dict:
    """
    Add a new causal edge. Returns { inserted: bool, id: int, message: str }.
    Duplicate (cause, effect, mechanism) is rejected with inserted=False.
    """
    cause     = cause.lower().replace(' ', '_')
    effect    = effect.lower().replace(' ', '_')
    mechanism = mechanism.lower().replace(' ', '_')
    confidence = max(0.0, min(1.0, confidence))

    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO causal_edges
               (cause, effect, mechanism, confidence, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cause, effect, mechanism, confidence, source, now_iso),
        )
        conn.commit()
        inserted = cur.rowcount > 0
        row_id = cur.lastrowid if inserted else None
        if not inserted:
            c = conn.cursor()
            c.execute(
                "SELECT id FROM causal_edges WHERE cause=? AND effect=? AND mechanism=?",
                (cause, effect, mechanism),
            )
            row = c.fetchone()
            row_id = row[0] if row else None
        return {
            'inserted': inserted,
            'id':       row_id,
            'cause':    cause,
            'effect':   effect,
            'mechanism': mechanism,
            'confidence': confidence,
            'message':  'created' if inserted else 'duplicate — edge already exists',
        }
    except sqlite3.Error as e:
        return {'inserted': False, 'id': None, 'message': str(e)}


def list_causal_edges(
    conn: sqlite3.Connection,
    cause_filter: Optional[str] = None,
    limit: int = 200,
) -> List[dict]:
    """Return all causal edges, optionally filtered by cause concept."""
    c = conn.cursor()
    if cause_filter:
        c.execute("""
            SELECT id, cause, effect, mechanism, confidence, source, created_at
            FROM causal_edges
            WHERE cause LIKE ?
            ORDER BY confidence DESC LIMIT ?
        """, (f'%{cause_filter.lower()}%', limit))
    else:
        c.execute("""
            SELECT id, cause, effect, mechanism, confidence, source, created_at
            FROM causal_edges
            ORDER BY confidence DESC LIMIT ?
        """, (limit,))
    return [
        {
            'id':         r[0],
            'cause':      r[1],
            'effect':     r[2],
            'mechanism':  r[3],
            'confidence': r[4],
            'source':     r[5],
            'created_at': r[6],
        }
        for r in c.fetchall()
    ]
