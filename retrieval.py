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


# ── Constants ──────────────────────────────────────────────────────────────────

_CROSS_ASSET_KW = (
    'compare', 'versus', 'vs', 'correlat', 'relation', 'between',
    'and ', 'all ', 'portfolio', 'cross', 'relative',
)

_NOISE_PREDICATES = {
    'source_code', 'has_title', 'has_section', 'has_content',
}

_HIGH_VALUE_PREDICATES = (
    'signal_direction', 'signal_confidence', 'price_target',
    'catalyst', 'invalidation_condition', 'supporting_evidence',
    'contradicting_evidence', 'regime_label', 'risk_factor',
    'entry_condition', 'exit_condition', 'rating', 'key_finding',
)

# Query keyword → predicate boost mapping
# When a query contains these words, we directly fetch atoms with the mapped predicates
_KEYWORD_PREDICATE_BOOST: dict = {
    'target':      ('price_target', 'signal_direction'),
    'upside':      ('price_target', 'signal_direction'),
    'analyst':     ('price_target', 'signal_direction', 'rating'),
    'consensus':   ('price_target', 'signal_direction'),
    'signal':      ('signal_direction', 'signal_confidence'),
    'direction':   ('signal_direction',),
    'long':        ('signal_direction',),
    'short':       ('signal_direction',),
    'catalyst':    ('catalyst',),
    'risk':        ('risk_factor',),
    'earnings':    ('earnings_quality',),
    'regime':      ('regime_label', 'central_bank_stance', 'dominant_driver', 'growth_environment', 'inflation_environment'),
    'macro':       ('regime_label', 'central_bank_stance', 'dominant_driver', 'growth_environment', 'inflation_environment'),
    'inflation':   ('inflation_environment', 'dominant_driver', 'regime_label'),
    'rate':        ('central_bank_stance', 'dominant_driver'),
    'yield':       ('risk_factor', 'dominant_driver'),
    'sector':      ('sector',),
    'volatility':  ('volatility_regime',),
    'beta':        ('volatility_regime',),
    'momentum':    ('signal_direction',),
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
) -> Tuple[str, List[dict]]:
    """
    Smart multi-strategy retrieval for the Trading KB.

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
    if terms:
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

    # ── 3. Direct ticker / subject match ──────────────────────────────────────
    for ticker in tickers:
        try:
            c.execute("""
                SELECT subject, predicate, object, source, confidence
                FROM facts
                WHERE LOWER(subject) LIKE ?
                AND predicate NOT IN ('source_code','has_title','has_section','has_content')
                ORDER BY confidence DESC LIMIT 12
            """, (f'%{ticker.lower()}%',))
            _add(c.fetchall())
        except Exception:
            pass

    # ── 4. High-value signal predicates for matched terms ────────────────────
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

    # ── 4b. Predicate keyword boost ───────────────────────────────────────────
    # When query contains intent words (upside, target, sector, regime...) fetch
    # atoms with those exact predicates for all matched tickers/terms.
    boosted_predicates: set = set()
    for term in terms:
        for kw, preds in _KEYWORD_PREDICATE_BOOST.items():
            if kw in term or term in kw:
                boosted_predicates.update(preds)

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
            # No explicit tickers — fetch top boosted-predicate atoms globally
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

    return '\n'.join(lines), results
