"""
ingest/convergence_adapter.py — Strategy Convergence Engine

Reads independent signal families from the facts table, detects when multiple
families align on the same ticker+direction within a time window, computes a
convergence score, and writes results to the PG strategy_convergence table.

Runs every 600s (registered in api_v2.py).

Signal families checked (all stored as predicates in facts):
  Structure  : pattern_signals (open patterns from pattern_signals table)
  Energy     : bb_squeeze, atr_regime
  Momentum   : rsi_regime, macd_signal, sma_alignment
  Volume     : relative_volume, volume_poc_zone
  Breadth    : breadth_regime, breadth_thrust

Convergence score formula:
  base = family_count / total_families  (0.0–1.0)
  timing_bonus = +0.1 if lead and follow within 4h
  direction_bonus = +0.1 if all families agree on direction
  score = min(base + bonuses, 1.0)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

try:
    from db import HAS_POSTGRES, get_pg
except ImportError:
    HAS_POSTGRES = False
    get_pg = None  # type: ignore

# Signal family definitions: family_name → list of (predicate, bullish_values, bearish_values)
_FAMILIES = {
    'structure': {
        'predicates': [],  # checked via pattern_signals table separately
    },
    'energy': {
        'predicates': [
            ('bb_squeeze', {'firing', 'building'}, set()),
            ('atr_regime', {'expanding'}, {'contracting'}),
        ],
    },
    'momentum': {
        'predicates': [
            ('rsi_regime', {'oversold', 'neutral_low'}, {'overbought', 'neutral_high'}),
            ('macd_signal', {'bullish_cross'}, {'bearish_cross'}),
            ('sma_alignment', {'bullish_stack'}, {'bearish_stack'}),
        ],
    },
    'volume': {
        'predicates': [
            ('relative_volume', set(), set()),  # >1.5 = confirming (direction from context)
            ('volume_poc_zone', {'below'}, {'above'}),  # below POC = bullish demand
        ],
    },
    'breadth': {
        'predicates': [
            ('breadth_regime', {'strong'}, {'weak'}),
            ('breadth_thrust', {'bullish'}, {'bearish'}),
        ],
    },
}

_TOTAL_FAMILIES = len(_FAMILIES)
_EXPIRY_HOURS = 24  # convergence records expire after 24h


class ConvergenceAdapter(BaseIngestAdapter):
    """Detect multi-family signal convergence per ticker."""

    name = "strategy_convergence"

    def __init__(self, db_path: str):
        super().__init__(self.name)
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        if not HAS_POSTGRES or not get_pg:
            _logger.debug("convergence_adapter: PG not available, skipping")
            return []

        try:
            convergences = self._compute_convergences()
            self._write_convergences(convergences)
        except Exception as e:
            _logger.error("convergence_adapter: failed: %s", e)
        return []  # no KB atoms — writes directly to strategy_convergence table

    def transform(self, raw):
        return raw if isinstance(raw, list) else []

    # ── Core computation ──────────────────────────────────────────────────────

    def _compute_convergences(self) -> List[dict]:
        """Scan all tickers for multi-family signal overlap."""
        results = []
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()

        with get_pg() as pgconn:
            cur = pgconn.cursor()

            # Get all tickers with recent facts
            cur.execute(
                "SELECT DISTINCT UPPER(subject) AS ticker FROM facts "
                "WHERE timestamp >= %s AND subject != 'market_breadth_global' "
                "AND subject != 'system'",
                (cutoff,))
            tickers = [r['ticker'] for r in cur.fetchall()]

            # Get breadth signals (global, shared across tickers)
            breadth_signals = self._get_breadth_signals(cur, cutoff)

            for ticker in tickers:
                for direction in ('bullish', 'bearish'):
                    families_active = set()
                    lead_family = None
                    follow_family = None
                    earliest_ts = None
                    latest_ts = None

                    # Check structure family (open patterns)
                    if self._has_structure_signal(cur, ticker, direction):
                        families_active.add('structure')

                    # Check other families from facts
                    cur.execute(
                        "SELECT predicate, object, timestamp FROM facts "
                        "WHERE UPPER(subject) = %s AND timestamp >= %s "
                        "ORDER BY timestamp DESC",
                        (ticker, cutoff))
                    facts = cur.fetchall()

                    fact_map: Dict[str, Tuple[str, str]] = {}
                    for f in facts:
                        pred = f['predicate']
                        if pred not in fact_map:
                            fact_map[pred] = (f['object'], f['timestamp'] or '')

                    for family_name, family_def in _FAMILIES.items():
                        if family_name == 'structure':
                            continue
                        if family_name == 'breadth':
                            if direction in breadth_signals:
                                families_active.add('breadth')
                            continue

                        for pred, bull_vals, bear_vals in family_def['predicates']:
                            if pred not in fact_map:
                                continue
                            val, ts = fact_map[pred]
                            if direction == 'bullish' and bull_vals and val in bull_vals:
                                families_active.add(family_name)
                                self._update_timing(ts, families_active, family_name,
                                                    earliest_ts, latest_ts)
                                if earliest_ts is None or ts < earliest_ts:
                                    earliest_ts = ts
                                    lead_family = family_name
                                if latest_ts is None or ts > latest_ts:
                                    latest_ts = ts
                                    follow_family = family_name
                                break
                            elif direction == 'bearish' and bear_vals and val in bear_vals:
                                families_active.add(family_name)
                                if earliest_ts is None or ts < earliest_ts:
                                    earliest_ts = ts
                                    lead_family = family_name
                                if latest_ts is None or ts > latest_ts:
                                    latest_ts = ts
                                    follow_family = family_name
                                break

                    # Only record if >= 2 families converge
                    if len(families_active) < 2:
                        continue

                    # Compute timing span
                    hours_span = 0.0
                    if earliest_ts and latest_ts and earliest_ts != latest_ts:
                        try:
                            t0 = datetime.fromisoformat(earliest_ts.replace('Z', '+00:00'))
                            t1 = datetime.fromisoformat(latest_ts.replace('Z', '+00:00'))
                            hours_span = abs((t1 - t0).total_seconds()) / 3600
                        except Exception:
                            pass

                    # Score
                    base = len(families_active) / _TOTAL_FAMILIES
                    timing_bonus = 0.1 if 0 < hours_span <= 4 else 0.0
                    score = min(round(base + timing_bonus, 4), 1.0)

                    results.append({
                        'ticker': ticker,
                        'direction': direction,
                        'families_active': json.dumps(sorted(families_active)),
                        'family_count': len(families_active),
                        'lead_family': lead_family,
                        'follow_family': follow_family if follow_family != lead_family else None,
                        'hours_span': round(hours_span, 2),
                        'convergence_score': score,
                        'detected_at': datetime.now(timezone.utc).isoformat(),
                        'expires_at': (datetime.now(timezone.utc) + timedelta(hours=_EXPIRY_HOURS)).isoformat(),
                    })

        return results

    def _has_structure_signal(self, cur, ticker: str, direction: str) -> bool:
        """Check for open pattern signals matching ticker+direction."""
        cur.execute(
            "SELECT 1 FROM pattern_signals "
            "WHERE UPPER(ticker) = %s AND direction = %s AND status = 'open' "
            "LIMIT 1",
            (ticker, direction))
        return cur.fetchone() is not None

    def _get_breadth_signals(self, cur, cutoff: str) -> Set[str]:
        """Return set of directions supported by breadth signals."""
        directions: Set[str] = set()
        cur.execute(
            "SELECT predicate, object FROM facts "
            "WHERE UPPER(subject) = 'MARKET_BREADTH_GLOBAL' AND timestamp >= %s",
            (cutoff,))
        for row in cur.fetchall():
            pred, val = row['predicate'], row['object']
            if pred == 'breadth_regime' and val == 'strong':
                directions.add('bullish')
            elif pred == 'breadth_regime' and val == 'weak':
                directions.add('bearish')
            elif pred == 'breadth_thrust' and val == 'bullish':
                directions.add('bullish')
            elif pred == 'breadth_thrust' and val == 'bearish':
                directions.add('bearish')
        return directions

    @staticmethod
    def _update_timing(ts, families_active, family_name, earliest_ts, latest_ts):
        """Helper stub — timing tracked in main loop."""
        pass

    # ── Write to PG ───────────────────────────────────────────────────────────

    def _write_convergences(self, convergences: List[dict]) -> None:
        """Upsert convergence records into strategy_convergence table."""
        if not convergences:
            return

        with get_pg() as pgconn:
            cur = pgconn.cursor()
            # Expire old records
            cur.execute(
                "DELETE FROM strategy_convergence WHERE expires_at < %s",
                (datetime.now(timezone.utc).isoformat(),))

            for c in convergences:
                cur.execute(
                    """INSERT INTO strategy_convergence
                       (ticker, direction, families_active, family_count,
                        lead_family, follow_family, hours_span,
                        convergence_score, detected_at, expires_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (ticker, direction)
                       DO UPDATE SET
                         families_active = EXCLUDED.families_active,
                         family_count = EXCLUDED.family_count,
                         lead_family = EXCLUDED.lead_family,
                         follow_family = EXCLUDED.follow_family,
                         hours_span = EXCLUDED.hours_span,
                         convergence_score = EXCLUDED.convergence_score,
                         detected_at = EXCLUDED.detected_at,
                         expires_at = EXCLUDED.expires_at""",
                    (c['ticker'], c['direction'], c['families_active'],
                     c['family_count'], c['lead_family'], c['follow_family'],
                     c['hours_span'], c['convergence_score'],
                     c['detected_at'], c['expires_at']),
                )

        _logger.info("convergence_adapter: wrote %d convergence records", len(convergences))
