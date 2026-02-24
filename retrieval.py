"""
retrieval.py — Trading KB Smart Retrieval Engine

Multi-strategy retrieval over the trading knowledge graph.
Applies authority re-ranking and epistemic stress signals.

Strategies (in order):
  1. Cross-instrument / cross-asset relationship queries  → GNN atoms
  2. FTS on extracted key terms                          → full-text search
  3. Direct ticker/subject match                         → precise lookup
  4. High-value signal atoms for matched terms           → signal-type predicates
  5. Fallback: top-confidence non-noise atoms

Zero-LLM, pure Python.
"""

from __future__ import annotations

import re
import sqlite3
from typing import List, Tuple, Dict

try:
    from knowledge.authority import effective_score as _effective_score
    HAS_AUTHORITY = True
except ImportError:
    HAS_AUTHORITY = False

try:
    from knowledge.graph_retrieval import build_graph_context, what_do_i_know_about
    HAS_GRAPH_RETRIEVAL = True
except ImportError:
    HAS_GRAPH_RETRIEVAL = False


# ── Constants ──────────────────────────────────────────────────────────────────

_CROSS_ASSET_KW = (
    'compare', 'versus', 'vs', 'correlat', 'relation', 'between',
    'and ', 'all ', 'portfolio', 'cross', 'relative',
)

# Queries that benefit from relational graph traversal rather than keyword lookup
_GRAPH_TRAVERSAL_KW = (
    'why', 'how', 'explain', 'driven', 'affect', 'impact', 'related',
    'connected', 'exposure', 'sensitive', 'through', 'chain', 'path',
    'what do you know', 'tell me about', 'overview', 'summary',
)

_NOISE_PREDICATES = {
    'source_code', 'has_title', 'has_section', 'has_content',
}

_HIGH_VALUE_PREDICATES = (
    'signal_direction', 'signal_confidence', 'price_target',
    'catalyst', 'invalidation_condition', 'supporting_evidence',
    'contradicting_evidence', 'regime_label', 'risk_factor',
    'entry_condition', 'exit_condition', 'rating', 'key_finding',
    'signal_quality', 'macro_confirmation', 'price_regime', 'upside_pct',
    'return_1m', 'return_3m', 'return_6m', 'return_1y',
    'volatility_30d', 'volatility_90d', 'drawdown_from_52w_high',
    'return_vs_spy_1m', 'return_vs_spy_3m',
)

# Query keyword → predicate boost mapping
# When a query contains these words, we directly fetch atoms with the mapped predicates
_KEYWORD_PREDICATE_BOOST: dict = {
    'target':      ('price_target', 'signal_direction', 'upside_pct'),
    'upside':      ('price_target', 'signal_direction', 'upside_pct'),
    'analyst':     ('price_target', 'signal_direction', 'rating', 'upside_pct'),
    'consensus':   ('price_target', 'signal_direction', 'upside_pct'),
    'signal':      ('signal_direction', 'signal_confidence', 'signal_quality'),
    'quality':     ('signal_quality', 'signal_direction', 'macro_confirmation'),
    'direction':   ('signal_direction', 'signal_quality'),
    'long':        ('signal_direction', 'signal_quality'),
    'short':       ('signal_direction', 'signal_quality'),
    'catalyst':    ('catalyst',),
    'risk':        ('risk_factor', 'signal_quality', 'macro_confirmation'),
    'earnings':    ('earnings_quality',),
    'confirm':     ('macro_confirmation', 'signal_quality'),
    'confirmed':   ('macro_confirmation', 'signal_quality'),
    'macro':       ('regime_label', 'central_bank_stance', 'dominant_driver', 'growth_environment', 'inflation_environment', 'macro_confirmation'),
    'regime':      ('regime_label', 'central_bank_stance', 'dominant_driver', 'growth_environment', 'inflation_environment', 'price_regime'),
    'inflation':   ('inflation_environment', 'dominant_driver', 'regime_label'),
    'rate':        ('central_bank_stance', 'dominant_driver'),
    'yield':       ('risk_factor', 'dominant_driver'),
    'sector':      ('sector',),
    'volatility':  ('volatility_regime', 'signal_quality'),
    'beta':        ('volatility_regime',),
    'momentum':    ('signal_direction', 'price_regime', 'signal_quality', 'return_1m', 'return_3m'),
    'extended':    ('signal_quality', 'price_regime'),
    'conflict':    ('signal_quality', 'macro_confirmation'),
    'conviction':  ('signal_quality', 'upside_pct', 'macro_confirmation'),
    'return':      ('return_1m', 'return_3m', 'return_6m', 'return_1y', 'return_vs_spy_1m'),
    'performance': ('return_1m', 'return_3m', 'return_6m', 'return_1y', 'return_vs_spy_3m'),
    'drawdown':    ('drawdown_from_52w_high', 'price_regime', 'volatility_90d'),
    'relative':    ('return_vs_spy_1m', 'return_vs_spy_3m', 'signal_quality'),
    'outperform':  ('return_vs_spy_1m', 'return_vs_spy_3m'),
    'vol':         ('volatility_30d', 'volatility_90d', 'volatility_regime'),
    'trend':       ('return_1m', 'return_3m', 'price_regime', 'signal_direction'),
}

_STOPWORDS = {
    'the', 'is', 'at', 'which', 'on', 'a', 'an', 'and', 'or', 'for',
    'in', 'of', 'to', 'that', 'this', 'with', 'from', 'by', 'are',
    'was', 'be', 'has', 'have', 'had', 'its', 'it', 'do', 'did',
    'what', 'how', 'why', 'when', 'where', 'who', 'can', 'will',
    'there', 'their', 'they', 'we', 'you', 'me', 'my', 'our',
}

_UPPERCASE_STOPWORDS = {
    'THE', 'IS', 'AT', 'ON', 'AN', 'AND', 'OR', 'FOR', 'IN', 'OF',
    'TO', 'THAT', 'THIS', 'WITH', 'FROM', 'BY', 'ARE', 'WAS', 'BE',
    'HAS', 'HAVE', 'HAD', 'ITS', 'DO', 'DID', 'WHAT', 'HOW', 'WHY',
    'WHEN', 'WHERE', 'WHO', 'CAN', 'WILL', 'THERE', 'THEIR', 'THEY',
    'YOU', 'NOT', 'BUT', 'ALL', 'GET', 'GOT', 'NEW', 'NOW', 'OUT',
    'USE', 'WAY', 'USED', 'ALSO', 'JUST', 'INTO', 'OVER', 'COULD',
    'WOULD', 'SHOULD', 'THAN', 'THEN', 'WHICH', 'SOME', 'MORE',
}


def _extract_key_terms(message: str) -> List[str]:
    """Extract meaningful search terms from a query, filtering stopwords."""
    words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
    terms = [w.lower() for w in words if w.lower() not in _STOPWORDS and len(w) > 2]
    seen: set = set()
    return [t for t in terms if not (t in seen or seen.add(t))]


def _extract_tickers(message: str) -> List[str]:
    """Extract uppercase ticker symbols from a message."""
    candidates = re.findall(r'\b[A-Z]{2,5}\b', message)
    return [t for t in candidates if t not in _UPPERCASE_STOPWORDS]


def retrieve(
    message: str,
    conn: sqlite3.Connection,
    limit: int = 30,
    nudges=None,   # AdaptationNudges | None — from EpistemicAdaptationEngine
) -> Tuple[str, List[dict]]:
    """
    Smart multi-strategy retrieval for the Trading KB.

    nudges: optional AdaptationNudges from EpistemicAdaptationEngine.
      - prefer_recent=True  → ORDER BY timestamp DESC in direct ticker match
      - prefer_high_authority=True → post-filter atoms below authority cutoff
      - retrieval_scope_broadened=True → also fetch atoms from all sources (no source filter)

    Returns:
        (formatted_snippet, raw_atom_list)
        raw_atom_list is passed to epistemic stress computation.
    """
    seen: set = set()
    results: List[dict] = []

    def _normalise(r) -> dict | None:
        try:
            if hasattr(r, 'keys'):
                return {
                    'subject':    str(r['subject'] or '').strip(),
                    'predicate':  str(r['predicate'] or '').strip(),
                    'object':     str(r['object'] or '')[:300].strip(),
                    'source':     str(r['source'] if 'source' in r.keys() else '').strip(),
                    'confidence': float(r['confidence']) if 'confidence' in r.keys() else 0.5,
                }
            return {
                'subject': str(r[0]).strip(), 'predicate': str(r[1]).strip(),
                'object': str(r[2])[:300].strip(),
                'source': str(r[3]).strip() if len(r) > 3 else '',
                'confidence': float(r[4]) if len(r) > 4 else 0.5,
            }
        except Exception:
            return None

    def _add(rows):
        for r in rows:
            atom = _normalise(r)
            if not atom:
                continue
            key = (atom['subject'][:60], atom['predicate'], atom['object'][:60])
            if key not in seen and atom['predicate'] not in _NOISE_PREDICATES \
                    and atom['subject'] and atom['object']:
                seen.add(key)
                results.append(atom)

    c = conn.cursor()
    msg_lower = message.lower()
    terms = _extract_key_terms(message)
    tickers = _extract_tickers(message)

    # Expand limit dynamically for multi-ticker queries so pinned atoms
    # (4 per ticker) don't crowd out the context atoms.
    if len(tickers) >= 2:
        limit = max(limit, len(tickers) * 6 + 12)

    # Pre-compute boost predicates so FTS can be skipped when not needed
    boosted_predicates: set = set()
    for term in terms:
        for kw, preds in _KEYWORD_PREDICATE_BOOST.items():
            if kw in term or term in kw:
                boosted_predicates.update(preds)

    # ── 0. Graph-relational context (PageRank + clustering + BFS paths) ───────
    # Fires on relational/explanatory queries and when no explicit tickers present.
    # Fetches a broad atom set for the topic then runs graph analysis over it.
    graph_snippet: str = ''
    is_graph_query = any(kw in msg_lower for kw in _GRAPH_TRAVERSAL_KW)
    if HAS_GRAPH_RETRIEVAL and (is_graph_query or (not tickers and terms)):
        try:
            # Broad fetch: all atoms for the first two key terms, up to 200
            graph_atoms: list = []
            for term in terms[:2]:
                c.execute("""
                    SELECT subject, predicate, object, source, confidence
                    FROM facts
                    WHERE (LOWER(subject) LIKE ? OR LOWER(object) LIKE ?)
                    AND predicate NOT IN ('source_code','has_title','has_section','has_content')
                    ORDER BY confidence DESC LIMIT 100
                """, (f'%{term}%', f'%{term}%'))
                for r in c.fetchall():
                    graph_atoms.append({
                        'subject':    str(r[0]).strip(),
                        'predicate':  str(r[1]).strip(),
                        'object':     str(r[2])[:200].strip(),
                        'source':     str(r[3]).strip() if r[3] else '',
                        'confidence': float(r[4]) if r[4] else 0.5,
                    })
            # Deduplicate
            seen_ga: set = set()
            unique_graph_atoms = []
            for a in graph_atoms:
                k = (a['subject'], a['predicate'], a['object'][:60])
                if k not in seen_ga:
                    seen_ga.add(k)
                    unique_graph_atoms.append(a)
            if len(unique_graph_atoms) >= 5:
                graph_snippet = build_graph_context(unique_graph_atoms, message, max_nodes_in_context=80)
        except Exception:
            pass

    # ── 0.5. Ticker-pinned key-atom pre-fetch ─────────────────────────────────
    # For every ticker explicitly named in the query, guarantee that
    # last_price, price_target, signal_direction and earnings_quality are
    # retrieved FIRST — before any limit-based competition with other atoms.
    # Without this, multi-ticker queries (e.g. "rank AAPL MSFT GOOGL AMZN
    # NVDA META by upside") exhaust the 30-atom limit before all tickers
    # get their price/target atoms, leaving the LLM with gaps.
    _PINNED_PREDICATES = (
        'last_price', 'price_target', 'signal_direction', 'earnings_quality',
        'signal_quality', 'macro_confirmation', 'price_regime', 'upside_pct',
        'return_1m', 'return_3m', 'return_1y',
        'volatility_30d', 'drawdown_from_52w_high',
        'return_vs_spy_1m', 'return_vs_spy_3m',
    )
    _pin_ph = ','.join('?' * len(_PINNED_PREDICATES))
    for ticker in tickers:
        try:
            c.execute(f"""
                SELECT subject, predicate, object, source, confidence
                FROM facts
                WHERE LOWER(subject) = ?
                AND predicate IN ({_pin_ph})
                ORDER BY confidence DESC
            """, (ticker.lower(), *_PINNED_PREDICATES))
            _add(c.fetchall())
        except Exception:
            pass

    # ── 1. Cross-asset / portfolio queries → GNN atoms ────────────────────────
    is_cross_asset = any(kw in msg_lower for kw in _CROSS_ASSET_KW)
    if is_cross_asset:
        try:
            c.execute("""
                SELECT subject, predicate, object, source, confidence
                FROM facts WHERE source = 'cross_asset_gnn'
                ORDER BY confidence DESC
            """)
            _add(c.fetchall())
        except Exception:
            pass

    # ── 2. FTS on key terms ────────────────────────────────────────────────────
    # Skip FTS when explicit tickers + intent keywords are both present —
    # the boost (step 3) gives more precise results and FTS would flood seen-set
    # with low-value sector/price atoms before price_target atoms get added.
    use_fts = not (tickers and boosted_predicates)
    if use_fts and terms:
        fts_query = ' OR '.join(terms[:6])
        try:
            c.execute("""
                SELECT f.subject, f.predicate, f.object, f.source, f.confidence
                FROM facts_fts fts
                JOIN facts f ON fts.rowid = f.id
                WHERE facts_fts MATCH ?
                ORDER BY rank, f.confidence DESC
                LIMIT ?
            """, (fts_query, limit))
            _add(c.fetchall())
        except Exception:
            pass

    # ── 3. Predicate keyword boost (intent-aware) ───────────────────────────
    # Runs BEFORE bulk ticker match. Fetches exact predicate atoms for tickers.

    if boosted_predicates:
        pred_ph = ','.join('?' * len(boosted_predicates))
        if tickers:
            for ticker in tickers:
                try:
                    c.execute(f"""
                        SELECT subject, predicate, object, source, confidence
                        FROM facts
                        WHERE predicate IN ({pred_ph})
                        AND LOWER(subject) = ?
                        ORDER BY confidence DESC LIMIT 10
                    """, (*boosted_predicates, ticker.lower()))
                    _add(c.fetchall())
                except Exception:
                    pass
        else:
            try:
                c.execute(f"""
                    SELECT subject, predicate, object, source, confidence
                    FROM facts
                    WHERE predicate IN ({pred_ph})
                    ORDER BY confidence DESC LIMIT 20
                """, (*boosted_predicates,))
                _add(c.fetchall())
            except Exception:
                pass

    # ── 4. Direct ticker / subject match ──────────────────────────────────────
    for ticker in tickers:
        try:
            c.execute("""
                SELECT subject, predicate, object, source, confidence
                FROM facts
                WHERE LOWER(subject) LIKE ?
                AND predicate NOT IN ('source_code','has_title','has_section','has_content')
                ORDER BY confidence DESC LIMIT 6
            """, (f'%{ticker.lower()}%',))
            _add(c.fetchall())
        except Exception:
            pass

    # ── 5a. High-value signal predicates for matched terms ────────────────────
    if terms:
        term = terms[0]
        try:
            pred_placeholders = ','.join('?' * len(_HIGH_VALUE_PREDICATES))
            c.execute(f"""
                SELECT subject, predicate, object, source, confidence
                FROM facts
                WHERE predicate IN ({pred_placeholders})
                AND (LOWER(subject) LIKE ? OR LOWER(object) LIKE ?)
                ORDER BY confidence DESC LIMIT 12
            """, (*_HIGH_VALUE_PREDICATES, f'%{term}%', f'%{term}%'))
            _add(c.fetchall())
        except Exception:
            pass

    # ── 5. Fallback: top-confidence atoms ─────────────────────────────────────
    if len(results) < 8:
        try:
            c.execute("""
                SELECT subject, predicate, object, source, confidence
                FROM facts
                WHERE predicate NOT IN ('source_code','has_title','has_section','has_content')
                ORDER BY confidence DESC LIMIT 20
            """)
            _add(c.fetchall())
        except Exception:
            pass

    if not results:
        return '', []

    # ── Re-rank by epistemic strength ─────────────────────────────────────────
    if HAS_AUTHORITY:
        try:
            results.sort(key=_effective_score, reverse=True)
        except Exception:
            pass

    results = results[:limit]

    # ── Apply adaptation nudges ────────────────────────────────────────────────
    if nudges is not None:
        # Recency bias: sort by timestamp DESC (prefer freshest atoms)
        if getattr(nudges, 'prefer_recent', False) and results:
            try:
                from datetime import datetime as _dt
                def _ts(a):
                    m = a.get('metadata') or {}
                    t = (m.get('as_of') or m.get('timestamp') or '') if isinstance(m, dict) else ''
                    return t or '0'
                results.sort(key=_ts, reverse=True)
            except Exception:
                pass

        # Authority filter: drop atoms below cutoff when in high-conflict mode
        if getattr(nudges, 'prefer_high_authority', False):
            try:
                if HAS_AUTHORITY:
                    from knowledge.epistemic_adaptation import AUTHORITY_FILTER_CUTOFF
                    from knowledge.authority import get_authority
                    filtered = [a for a in results
                                if _effective_score(a['confidence'], a['source'])
                                >= AUTHORITY_FILTER_CUTOFF * 0.5
                                or get_authority(a['source']) >= AUTHORITY_FILTER_CUTOFF]
                    if len(filtered) >= 5:
                        results = filtered
            except Exception:
                pass

        # Scope broadening: if fewer than 8 results, broaden via all-source fallback
        if getattr(nudges, 'retrieval_scope_broadened', False) and len(results) < 8:
            try:
                for term in terms[:2]:
                    c.execute("""
                        SELECT subject, predicate, object, source, confidence
                        FROM facts
                        WHERE (LOWER(subject) LIKE ? OR LOWER(object) LIKE ?)
                        AND predicate NOT IN ('source_code','has_title','has_section','has_content')
                        ORDER BY confidence DESC LIMIT 20
                    """, (f'%{term}%', f'%{term}%'))
                    _add(c.fetchall())
                results = results[:limit]
            except Exception:
                pass

    # ── Increment hit_count for returned atoms ─────────────────────────────────
    # Feeds the frequency term (δ) in the PageRank importance formula.
    # Best-effort: failure must not affect the response.
    if results:
        try:
            subj_pred_pairs = list({
                (a['subject'], a['predicate']) for a in results
            })
            for subj, pred in subj_pred_pairs:
                c.execute("""
                    UPDATE facts SET hit_count = COALESCE(hit_count, 0) + 1
                    WHERE subject = ? AND predicate = ?
                """, (subj, pred))
            conn.commit()
        except Exception:
            pass

    # ── Format output ──────────────────────────────────────────────────────────
    lines = ['=== TRADING KNOWLEDGE CONTEXT ===']

    signals, theses, macro, research, other = [], [], [], [], []
    for r in results:
        pred = r['predicate']
        src = r['source']
        if pred in ('signal_direction', 'signal_confidence', 'price_target',
                    'entry_condition', 'exit_condition', 'invalidation_condition'):
            signals.append(r)
        elif pred in ('premise', 'supporting_evidence', 'contradicting_evidence',
                      'risk_reward_ratio', 'position_sizing_note'):
            theses.append(r)
        elif src.startswith('macro_data') or pred in ('regime_label', 'dominant_driver',
                                                       'central_bank_stance', 'risk_on_off'):
            macro.append(r)
        elif src.startswith('broker_research') or pred in ('rating', 'key_finding',
                                                            'compared_to_consensus'):
            research.append(r)
        else:
            other.append(r)

    def _fmt(r):
        return f"  {r['subject']} | {r['predicate']} | {r['object']}"

    if signals:
        lines.append('[Signals & Positioning]')
        lines.extend(_fmt(r) for r in signals[:10])
    if theses:
        lines.append('[Theses & Evidence]')
        lines.extend(_fmt(r) for r in theses[:8])
    if macro:
        lines.append('[Macro / Regime]')
        lines.extend(_fmt(r) for r in macro[:6])
    if research:
        lines.append('[Research]')
        lines.extend(_fmt(r) for r in research[:6])
    if other:
        lines.append('[Other]')
        lines.extend(_fmt(r) for r in other[:6])

    flat_snippet = '\n'.join(lines)

    # Prepend graph-structured context when it was produced (relational queries)
    if graph_snippet:
        full_snippet = graph_snippet + '\n\n' + flat_snippet
    else:
        full_snippet = flat_snippet

    return full_snippet, results
