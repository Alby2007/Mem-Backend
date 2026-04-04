"""
analytics/thesis_generator.py — Automatic Thesis Generation from KB Convergence

Scans the KB for tickers where signals from 4+ *independent source categories*
all align in the same direction, then auto-generates a thesis via ThesisBuilder.

SOURCE CATEGORIES (independence rule — prevents false convergence from same-chain atoms)
================
  price_technical  — signal_direction, conviction_tier, pattern_type
  news_sentiment   — news_sentiment, llm_sentiment, news_bias
  causal           — causal_signal (shock engine)
  sector           — sector_tailwind, sector_rotation_signal
  insider          — insider_conviction, insider_flow
  macro            — macro_confirmation, central_bank_stance, regime_label

≥4 distinct categories all aligned in the same direction → emit auto-thesis.

ATOMS WRITTEN
=============
  {ticker} | auto_thesis      | bullish  (or bearish)
  {ticker} | auto_thesis_score| 0.83     (fraction of categories aligned)
  {ticker} | auto_thesis_at   | 2026-03-10T09:00:00Z

Thesis is also written via ThesisBuilder.build() under user_id='system',
so ThesisMonitor immediately starts watching for invalidation.

USAGE
=====
  from analytics.thesis_generator import ThesisGenerator
  gen = ThesisGenerator(db_path)
  results = gen.run()
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

# ── Source category mapping ────────────────────────────────────────────────────

_CATEGORY_MAP: Dict[str, str] = {
    'signal_direction':        'price_technical',
    'conviction_tier':         'price_technical',
    'pattern_type':            'price_technical',
    'news_sentiment':          'news_sentiment',
    'llm_sentiment':           'news_sentiment',
    'news_bias':               'news_sentiment',
    'causal_signal':           'causal',
    'sector_tailwind':         'sector',
    'sector_rotation_signal':  'sector',
    'macro_confirmation':      'macro',
    'central_bank_stance':     'macro',
    'regime_label':            'macro',
    'market_regime':           'macro',
    'growth_environment':      'macro',
    'dominant_driver':         'macro',
    'inflation_environment':   'macro',
    'insider_conviction':      'insider',
    'insider_flow':            'insider',
    'institutional_flow':      'insider',
}

# Shared macro subjects written by FRED/macro adapters (not ticker-specific)
_MACRO_SUBJECTS = ('us_macro', 'us_yields', 'us_labor')

# Values considered bullish / bearish per predicate.
# Fix B: expanded to match actual adapter output values (FRED, market-state snapshots).
_BULLISH_VALUES = {
    'signal_direction':       {'bullish', 'long', 'buy', 'near_high'},
    'conviction_tier':        {'high', 'confirmed', 'strong', 'medium', 'moderate'},  # medium = positive confirmation
    'news_sentiment':         {'positive', 'bullish', 'optimistic'},
    'llm_sentiment':          {'positive', 'bullish'},
    'news_bias':              {'bullish', 'positive'},
    'causal_signal':          {'bullish', 'positive_shock', 'positive'},
    'sector_tailwind':        {'strong', 'tailwind', 'bullish', 'positive'},
    'sector_rotation_signal': {'bullish', 'inflow', 'positive'},
    # FRED emits: 'confirmed', 'supportive', 'positive_growth'
    'macro_confirmation':     {'confirmed', 'positive', 'supportive', 'positive_growth', 'improving'},
    'inflation_environment':  {'low_inflation', 'target_inflation', 'disinflation'},  # low inflation = equity bullish
    # FRED central_bank_stance: 'restrictive'/'neutral_to_restrictive' = equity headwind (bearish),
    # 'accommodative'/'easy policy' = equity tailwind (bullish)
    'central_bank_stance':    {'accommodative', 'dovish', 'easy policy'},
    # FRED regime_label: 'tight policy, expansion', 'moderate_growth', 'gaining_momentum', etc.
    'regime_label':           {
        'risk_on_expansion', 'recovery',
        'gaining_momentum', 'risk_on', 'expansion', 'moderate_growth',
        'strong_growth',
    },
    'market_regime':          {
        'risk_on_expansion', 'recovery',
        'gaining_momentum', 'risk_on', 'expansion', 'moderate_growth',
    },
    'insider_conviction':     {'accumulating', 'bullish', 'buying'},
    'insider_flow':           {'net_buy', 'accumulating', 'bullish'},
    'institutional_flow':     {'accumulating', 'bullish', 'net_buy'},
    # FRED growth_environment predicate
    'growth_environment':     {'strong_growth', 'moderate_growth'},
    # FRED dominant_driver is free-text; scored via keyword matching in _score_convergence
    'dominant_driver':        set(),
}

_BEARISH_VALUES = {
    'signal_direction':       {'bearish', 'short', 'sell', 'near_low'},
    'conviction_tier':        {'avoid'},
    'news_sentiment':         {'negative', 'bearish', 'pessimistic'},
    'llm_sentiment':          {'negative', 'bearish'},
    'news_bias':              {'bearish', 'negative'},
    'causal_signal':          {'bearish', 'negative_shock', 'negative'},
    'sector_tailwind':        {'headwind', 'bearish', 'negative', 'weak'},
    'sector_rotation_signal': {'bearish', 'outflow', 'negative'},
    'macro_confirmation':     {'unconfirmed', 'negative', 'adverse', 'no_data'},  # no_data = treat as unconfirmed
    'inflation_environment':  {'high_inflation', 'above_target_inflation'},  # inflation headwind = equity bearish
    # FRED: restrictive/neutral_to_restrictive = rate headwind for equities
    'central_bank_stance':    {
        'restrictive', 'hawkish',
        'neutral_to_restrictive', 'tight policy',
    },
    'regime_label':           {
        'risk_off_contraction', 'stagflation',
        'contraction', 'recession risk', 'slowing growth',
        'tight policy',  # FRED composite regime string
    },
    'market_regime':          {
        'risk_off_contraction', 'stagflation', 'contraction',
    },
    'insider_conviction':     {'distributing', 'bearish', 'selling'},
    'insider_flow':           {'net_sell', 'distributing', 'bearish'},
    'institutional_flow':     {'distributing', 'bearish', 'net_sell'},
    'growth_environment':     {'stagnation', 'contraction'},
    'dominant_driver':        set(),
}

_MIN_CATEGORIES = 2  # was 4→3; broker_research + FRED only live adapters; raise to 3 when news/causal/sector active


# ── Invalidation helpers ───────────────────────────────────────────────────────

# Maps (predicate, direction) → plain-English invalidation condition string
_INV_TEMPLATES: Dict[Tuple[str, str], str] = {
    ('signal_direction',    'bullish'): 'signal_direction turns bearish',
    ('signal_direction',    'bearish'): 'signal_direction turns bullish',
    ('conviction_tier',     'bullish'): 'conviction_tier drops to avoid',
    ('conviction_tier',     'bearish'): 'conviction_tier rises to high',
    ('macro_confirmation',  'bullish'): 'macro_confirmation becomes unconfirmed',
    ('macro_confirmation',  'bearish'): 'macro_confirmation becomes confirmed',
    ('causal_signal',       'bullish'): 'causal_signal turns bearish or negative',
    ('causal_signal',       'bearish'): 'causal_signal turns bullish or positive',
    ('sector_tailwind',     'bullish'): 'sector_tailwind weakens to headwind',
    ('sector_tailwind',     'bearish'): 'sector_tailwind strengthens to tailwind',
    ('news_sentiment',      'bullish'): 'news_sentiment turns negative',
    ('news_sentiment',      'bearish'): 'news_sentiment turns positive',
    ('insider_conviction',  'bullish'): 'insider activity turns to distributing',
    ('insider_conviction',  'bearish'): 'insider activity turns to accumulating',
    ('regime_label',        'bullish'): 'market regime transitions to risk_off_contraction',
    ('regime_label',        'bearish'): 'market regime transitions to risk_on_expansion',
}


def _derive_invalidation(
    signals: Dict[str, Tuple[str, float]],  # predicate → (value, confidence)
    direction: str,
) -> str:
    """
    Find the supporting signal with the lowest confidence and return its
    reversal as the invalidation condition.
    """
    # Sort by confidence ascending — weakest first
    sorted_sigs = sorted(signals.items(), key=lambda kv: kv[1][1])
    for pred, (val, conf) in sorted_sigs:
        key = (pred, direction)
        if key in _INV_TEMPLATES:
            return _INV_TEMPLATES[key]
    # Fallback
    opp = 'bearish' if direction == 'bullish' else 'bullish'
    return f'signal_direction turns {opp}'


# ── ThesisGenerator ────────────────────────────────────────────────────────────

class ThesisGenerator:
    """
    Scans the KB for multi-source signal convergence and auto-generates theses.

    run() returns a list of dicts: {ticker, direction, categories, thesis_id}
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _write_atom(
        self,
        conn: sqlite3.Connection,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float = 0.85,
        source: str = 'thesis_generator',
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        subj = subject.lower()
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute("DELETE FROM facts WHERE subject=%s AND predicate=%s AND source=%s",
                                (subj, predicate, source))
                    cur.execute("INSERT INTO facts (subject, predicate, object, confidence, source, timestamp) VALUES (%s,%s,%s,%s,%s,%s)",
                                (subj, predicate, obj, confidence, source, now))
                return
            except Exception:
                pass
        conn.execute(
            "DELETE FROM facts WHERE subject=? AND predicate=? AND source=?",
            (subj, predicate, source),
        )
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, confidence, source, timestamp) VALUES (?,?,?,?,?,?)",
            (subj, predicate, obj, confidence, source, now),
        )

    def _get_all_tickers(self, conn: sqlite3.Connection) -> List[str]:
        rows = conn.execute(
            """SELECT DISTINCT subject FROM facts
               WHERE predicate IN ('signal_direction','conviction_tier','macro_confirmation')
                 AND confidence > 0.3"""
        ).fetchall()
        return [r['subject'] for r in rows]

    def _get_ticker_signals(
        self,
        conn: sqlite3.Connection,
        ticker: str,
    ) -> Dict[str, Tuple[str, float]]:
        """Returns {predicate: (value, confidence)} for all relevant predicates."""
        preds = list(_CATEGORY_MAP.keys())
        placeholders = ','.join('?' for _ in preds)
        rows = conn.execute(
            f"""SELECT predicate, object, confidence
                FROM facts
                WHERE subject=? AND predicate IN ({placeholders})
                  AND confidence > 0.3
                ORDER BY confidence DESC""",
            [ticker.lower()] + preds,
        ).fetchall()
        result: Dict[str, Tuple[str, float]] = {}
        for r in rows:
            pred = r['predicate']
            if pred not in result:  # keep highest confidence per predicate
                result[pred] = (r['object'].lower().strip(), float(r['confidence']))
        return result

    def _score_convergence(
        self,
        signals: Dict[str, Tuple[str, float]],
    ) -> Tuple[str, int, Dict[str, Tuple[str, float]], float]:
        """
        Returns (direction, n_categories_aligned, aligned_signals, score).
        direction is 'bullish' or 'bearish'.
        """
        bull_cats: Dict[str, Tuple[str, float]] = {}
        bear_cats: Dict[str, Tuple[str, float]] = {}

        # Predicates that may emit composite/multi-word strings (e.g. FRED regime_label
        # emits "tight policy, elevated inflation") — use substring keyword matching.
        _substr_preds = {
            'regime_label', 'central_bank_stance',
            'growth_environment', 'market_regime', 'dominant_driver',
        }
        for pred, (val, conf) in signals.items():
            cat = _CATEGORY_MAP.get(pred)
            if not cat:
                continue
            bull_vals = _BULLISH_VALUES.get(pred, set())
            bear_vals = _BEARISH_VALUES.get(pred, set())
            if pred in _substr_preds:
                is_bull = any(kw in val for kw in bull_vals)
                is_bear = any(kw in val for kw in bear_vals)
            else:
                is_bull = val in bull_vals
                is_bear = val in bear_vals
            if is_bull:
                if cat not in bull_cats or conf > bull_cats[cat][1]:
                    bull_cats[cat] = (val, conf)
            elif is_bear:
                if cat not in bear_cats or conf > bear_cats[cat][1]:
                    bear_cats[cat] = (val, conf)

        n_bull = len(bull_cats)
        n_bear = len(bear_cats)
        total = max(n_bull + n_bear, 1)

        if n_bull >= n_bear:
            return 'bullish', n_bull, bull_cats, round(n_bull / total, 3)
        else:
            return 'bearish', n_bear, bear_cats, round(n_bear / total, 3)

    def _already_has_fresh_thesis(self, conn: sqlite3.Connection, ticker: str) -> bool:
        """Skip if an auto_thesis atom was written in the last 24h."""
        row = conn.execute(
            """SELECT timestamp FROM facts
               WHERE subject=? AND predicate='auto_thesis' AND source='thesis_generator'
               ORDER BY timestamp DESC LIMIT 1""",
            (ticker.lower(),),
        ).fetchone()
        if not row:
            return False
        try:
            from datetime import timedelta
            ts = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
            age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            return age_h < 24
        except Exception:
            return False

    def _get_macro_overlay(self, conn: sqlite3.Connection) -> Dict[str, Tuple[str, float]]:
        """Fix A: pull shared macro atoms from us_macro/us_yields/us_labor subjects.

        Returns merged {predicate: (value, confidence)} — lowest confidence wins
        so per-ticker signals always dominate when merged (ticker signals are
        applied after the overlay via dict update).
        """
        overlay: Dict[str, Tuple[str, float]] = {}
        for subj in _MACRO_SUBJECTS:
            subj_signals = self._get_ticker_signals(conn, subj)
            for pred, (val, conf) in subj_signals.items():
                if pred not in overlay or conf > overlay[pred][1]:
                    overlay[pred] = (val, conf)
        return overlay

    def run(self) -> List[dict]:
        """
        Scan all tickers, emit auto-theses where ≥3 source categories converge.
        Returns list of {ticker, direction, n_categories, thesis_id}.
        """
        conn = self._conn()
        results = []
        try:
            tickers = self._get_all_tickers(conn)
            _log.info('ThesisGenerator: scanning %d tickers', len(tickers))

            # Fix A: load shared macro overlay once, reuse for every ticker
            macro_overlay = self._get_macro_overlay(conn)
            _log.debug(
                'ThesisGenerator: macro overlay has %d signals: %s',
                len(macro_overlay),
                {p: v for p, (v, _) in macro_overlay.items()},
            )

            for ticker in tickers:
                try:
                    if self._already_has_fresh_thesis(conn, ticker):
                        continue

                    signals = self._get_ticker_signals(conn, ticker)
                    # Merge macro overlay under ticker signals (ticker takes precedence)
                    merged = {**macro_overlay, **signals}
                    if len(merged) < 3:
                        continue

                    direction, n_cats, aligned, score = self._score_convergence(merged)
                    if n_cats < _MIN_CATEGORIES:
                        continue

                    # Build premise string
                    cat_descs = []
                    for cat, (val, conf) in sorted(aligned.items()):
                        cat_descs.append(f'{cat}:{val}')
                    premise = (
                        f'Auto-generated: {ticker.upper()} {direction} — '
                        f'{n_cats} independent signal categories converge: '
                        + ', '.join(cat_descs)
                    )

                    # Derive invalidation from weakest aligned signal
                    inv_condition = _derive_invalidation(aligned, direction)

                    # Write auto_thesis atoms
                    now = datetime.now(timezone.utc).isoformat()
                    self._write_atom(conn, ticker, 'auto_thesis', direction, score)
                    self._write_atom(conn, ticker, 'auto_thesis_score', str(round(score, 3)), score)
                    self._write_atom(conn, ticker, 'auto_thesis_at', now, 1.0)
                    self._write_atom(conn, ticker, 'auto_thesis_invalidation', inv_condition, score)
                    conn.commit()

                    # Also call ThesisBuilder so ThesisMonitor watches it
                    try:
                        from knowledge.thesis_builder import ThesisBuilder
                        builder = ThesisBuilder(self._db)
                        result = builder.build(
                            ticker=ticker,
                            premise=premise,
                            direction=direction,
                            user_id='system',
                        )
                        thesis_id = result.thesis_id
                    except Exception as _tb_e:
                        _log.debug('ThesisBuilder call failed for %s: %s', ticker, _tb_e)
                        thesis_id = f'auto_{ticker}_{now[:10]}'

                    results.append({
                        'ticker':       ticker.upper(),
                        'direction':    direction,
                        'premise':      premise,
                        'n_categories': n_cats,
                        'score':        score,
                        'thesis_id':    thesis_id,
                        'invalidation': inv_condition,
                    })
                    _log.info(
                        'ThesisGenerator: %s → %s (cats=%d score=%.2f)',
                        ticker.upper(), direction, n_cats, score,
                    )

                except Exception as _e:
                    _log.debug('ThesisGenerator: error on %s: %s', ticker, _e)

        finally:
            conn.close()

        _log.info('ThesisGenerator: generated %d theses from %d tickers', len(results), len(tickers) if 'tickers' in dir() else 0)
        return results
