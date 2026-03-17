"""
analytics/causal_shock_engine.py — Reactive Causal Shock Propagation

Activates the causal graph as a live reactive system. When a macro atom is
written to the KB (BoE rate decision, Fed decision, CPI print, stance change),
this engine:
  1. Detects whether the atom constitutes a meaningful shock (delta > threshold)
  2. Traverses the causal graph from the shock source
  3. Writes `causal_signal` atoms to all affected tickers with confidence
     proportional to edge weight × shock magnitude

INTEGRATION
===========
Called from KnowledgeGraph.add_fact() via a post-write hook:
    if self._shock_engine:
        self._shock_engine.on_atom_written(atom)

The engine is initialised in api.py at startup and injected into _kg via:
    _kg.set_shock_engine(CausalShockEngine(_DB_PATH))

SHOCK TRIGGERS
==============
Predicates that can trigger propagation when their value changes materially:
  - boe_base_rate      (subject: uk_macro)   threshold: 0.05 percentage points
  - central_bank_stance (subject: uk_macro)  threshold: stance string change
  - fed_funds_rate     (subject: us_macro)   threshold: 0.05 percentage points
  - uk_cpi_yoy         (subject: uk_macro)   threshold: 0.20 percentage points
  - inflation_rate     (subject: us_macro)   threshold: 0.20 percentage points

CONFIDENCE MODEL
================
  confidence = |magnitude| × edge_weight × _PROP_SCALAR
  Capped at 0.90, floor 0.10.
  Half-life: 48 hours (set via source prefix decay rules).

SHOCK LOG
=========
_shock_log is an in-memory deque(maxlen=200). Used by GET /causal/shocks
to return recent propagation events without a DB query.

PERFORMANCE
===========
on_atom_written() does an O(1) predicate check. Propagation only fires for
~5 predicates out of thousands written. Causal traversal is BFS, typically
<5ms. Total overhead per triggering atom: ~20ms.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

# Predicates that trigger shock propagation and the subjects they live on.
# Format: predicate → expected subject (None = any subject)
_SHOCK_TRIGGERS: Dict[str, Optional[str]] = {
    'boe_base_rate':       'uk_macro',    # BoEAdapter writes on uk_macro
    'central_bank_stance': None,           # LLM writes on fed/us_macro/uk_macro — accept any
    'fed_funds_rate':      None,           # may be written on us_macro or fed
    'uk_cpi_yoy':          'uk_macro',
    'inflation_rate':      None,           # written on us_macro or uk_macro
}

# Minimum delta (numeric) or True (string change) to fire propagation
_THRESHOLDS: Dict[str, float] = {
    'boe_base_rate':       0.05,
    'fed_funds_rate':      0.05,
    'uk_cpi_yoy':          0.20,
    'inflation_rate':      0.20,
    'central_bank_stance': 0.0,   # any string change triggers
}

# Causal graph traversal depth
_TRAVERSAL_DEPTH = 3

# Confidence scalar: confidence = |magnitude| × edge_weight × scalar
_PROP_SCALAR = 0.80
_CONF_MIN    = 0.10
_CONF_MAX    = 0.90

# Half-life label written into metadata so the decay engine picks it up
_HALF_LIFE_HOURS = 48

# Source prefix for causal signal atoms
_CAUSAL_SOURCE = 'causal_shock_engine'


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class Shock:
    predicate:  str
    subject:    str
    old_value:  Optional[str]
    new_value:  str
    magnitude:  float          # numeric delta, or 1.0 for string changes
    direction:  str            # 'increase' | 'decrease' | 'change'
    detected_at: str


@dataclass
class ShockPropagationEvent:
    shock:           Shock
    affected_tickers: List[str]
    atoms_written:   int
    traversal_depth: int
    propagated_at:   str
    summary:         str


# ── CausalShockEngine ──────────────────────────────────────────────────────────

class CausalShockEngine:
    """
    Detects macro shocks and propagates their effects through the causal graph.

    Usage (in api.py)
    -----------------
    from analytics.causal_shock_engine import CausalShockEngine
    _shock_engine = CausalShockEngine(_DB_PATH)
    _kg.set_shock_engine(_shock_engine)

    Introspection
    -------------
    engine.get_recent_shocks(n=50)  →  List[dict]  (for GET /causal/shocks)
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path
        self._shock_log: Deque[ShockPropagationEvent] = deque(maxlen=200)

    # ── Public hook ───────────────────────────────────────────────────────────

    def on_atom_written(self, atom_subject: str, atom_predicate: str, atom_object: str) -> None:
        """
        Called by KnowledgeGraph.add_fact() for every new atom written.
        O(1) check against _SHOCK_TRIGGERS — no-op for non-trigger predicates.
        """
        if atom_predicate not in _SHOCK_TRIGGERS:
            return

        expected_subject = _SHOCK_TRIGGERS[atom_predicate]
        if expected_subject and atom_subject.lower() != expected_subject.lower():
            return

        shock = self._detect_shock(atom_predicate, atom_subject, atom_object)
        if shock is None:
            return

        _log.info(
            'CausalShockEngine: shock detected — %s on %s (magnitude=%.3f %s)',
            atom_predicate, atom_subject, shock.magnitude, shock.direction,
        )

        event = self._propagate(shock)
        if event:
            self._shock_log.appendleft(event)

    # ── Shock detection ───────────────────────────────────────────────────────

    def _detect_shock(
        self,
        predicate: str,
        subject:   str,
        new_value: str,
    ) -> Optional[Shock]:
        """
        Reads the prior value from KB and computes delta.
        Returns None if delta is below threshold.
        """
        old_value = self._read_prior(subject, predicate)
        now_iso   = datetime.now(timezone.utc).isoformat()

        threshold = _THRESHOLDS.get(predicate, 0.05)

        # Numeric predicates
        if predicate not in ('central_bank_stance',):
            old_num = self._parse_numeric(old_value)
            new_num = self._parse_numeric(new_value)

            if old_num is None or new_num is None:
                # Can't compute delta — treat any new value as a shock signal
                if old_value != new_value:
                    return Shock(
                        predicate=predicate, subject=subject,
                        old_value=old_value, new_value=new_value,
                        magnitude=1.0, direction='change',
                        detected_at=now_iso,
                    )
                return None

            delta = new_num - old_num
            if abs(delta) < threshold:
                return None

            return Shock(
                predicate=predicate, subject=subject,
                old_value=old_value, new_value=new_value,
                magnitude=abs(delta),
                direction='increase' if delta > 0 else 'decrease',
                detected_at=now_iso,
            )

        # String predicate (central_bank_stance)
        if old_value == new_value or old_value is None:
            return None

        return Shock(
            predicate=predicate, subject=subject,
            old_value=old_value, new_value=new_value,
            magnitude=1.0, direction='change',
            detected_at=now_iso,
        )

    # ── Propagation ───────────────────────────────────────────────────────────

    def _propagate(self, shock: Shock) -> Optional[ShockPropagationEvent]:
        """
        Traverses causal graph from the shock predicate and writes
        causal_signal atoms to all terminal-node tickers.
        """
        from knowledge.causal_graph import traverse_causal

        # Map predicate + direction to a causal graph seed concept
        seed_concept = self._seed_concept(shock)
        if not seed_concept:
            return None

        conn = sqlite3.connect(self._db, timeout=10)
        try:
            result = traverse_causal(conn, seed_concept, max_depth=_TRAVERSAL_DEPTH)
        finally:
            conn.close()

        if not result or not result.get('chain'):
            _log.debug('CausalShockEngine: no causal chain from %s', seed_concept)
            return None

        # Build flat list of {ticker, node, confidence} from affected_tickers dict
        # affected_tickers = { terminal_node: [ticker, ...] }
        affected_map = result.get('affected_tickers', {})
        if not affected_map:
            return None

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms_written = 0
        all_tickers: list = []

        for node, tickers in affected_map.items():
            # Get the confidence of the edge leading to this terminal node
            edge_weight = next(
                (h['confidence'] for h in result['chain'] if h['effect'] == node),
                0.7,
            )
            for ticker in tickers:
                if not ticker:
                    continue
                confidence = min(
                    _CONF_MAX,
                    max(_CONF_MIN, shock.magnitude * edge_weight * _PROP_SCALAR),
                )
                signal_value = (
                    f'{seed_concept} '
                    f'(magnitude={shock.magnitude:.2f}, '
                    f'via={node})'
                )
                self._write_causal_atom(
                    subject    = ticker.lower(),
                    predicate  = 'causal_signal',
                    object_val = signal_value,
                    confidence = round(confidence, 3),
                    metadata   = {
                        'shock_source':    shock.predicate,
                        'shock_subject':   shock.subject,
                        'seed_concept':    seed_concept,
                        'terminal_node':   node,
                        'magnitude':       shock.magnitude,
                        'direction':       shock.direction,
                        'edge_weight':     edge_weight,
                        'half_life_hours': _HALF_LIFE_HOURS,
                        'old_value':       shock.old_value,
                        'new_value':       shock.new_value,
                        'propagated_at':   now_iso,
                    },
                )
                atoms_written += 1
                all_tickers.append(ticker)

        summary = (
            f'{shock.predicate} {shock.direction} '
            f'(magnitude={shock.magnitude:.2f}) → '
            f'{atoms_written} tickers affected via {seed_concept}'
        )
        _log.info('CausalShockEngine: %s', summary)

        return ShockPropagationEvent(
            shock             = shock,
            affected_tickers  = all_tickers,
            atoms_written     = atoms_written,
            traversal_depth   = len(result['chain']),
            propagated_at     = now_iso,
            summary           = summary,
        )

    def _seed_concept(self, shock: Shock) -> Optional[str]:
        """Map a shock to its causal graph entry concept."""
        p = shock.predicate
        d = shock.direction

        if p == 'boe_base_rate':
            # Map to fed_rate causal chain — same transmission mechanism
            return 'fed_rate_cut' if d == 'decrease' else 'fed_rate_hike'

        if p == 'fed_funds_rate':
            return 'fed_rate_cut' if d == 'decrease' else 'fed_rate_hike'

        if p in ('uk_cpi_yoy', 'inflation_rate'):
            return 'inflation_rises' if d == 'increase' else 'commodities_decline'

        if p == 'central_bank_stance':
            new = shock.new_value.lower()
            # Dovish / easing signals
            if any(w in new for w in ('accommodative', 'dovish', 'easing', 'cuts',
                                       'cut', 'potential_rate_cuts', 'lower_rates',
                                       'pause', 'hold_with_cuts_expected')):
                return 'fed_rate_cut'
            # Hawkish / tightening signals
            if any(w in new for w in ('restrictive', 'hawkish', 'tightening',
                                       'higher_for_longer', 'hike', 'rates_on_hold_hawkish')):
                return 'fed_rate_hike'
            return None

        return None

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _read_prior(self, subject: str, predicate: str) -> Optional[str]:
        """Read the current KB value for (subject, predicate) before the new write."""
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                row = conn.execute(
                    """SELECT object FROM facts
                       WHERE subject=? AND predicate=?
                       ORDER BY confidence DESC, id DESC LIMIT 1""",
                    (subject.lower(), predicate),
                ).fetchone()
                return row[0] if row else None
            finally:
                conn.close()
        except Exception:
            return None

    def _write_causal_atom(
        self,
        subject:    str,
        predicate:  str,
        object_val: str,
        confidence: float,
        metadata:   dict,
    ) -> None:
        """Write a causal_signal atom directly to the facts table."""
        import json
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            conn = sqlite3.connect(self._db, timeout=10)
            try:
                # Delete any existing causal_signal atom for this ticker
                # (latest shock always wins — decay handled by kb_cleanup)
                conn.execute(
                    "DELETE FROM facts WHERE subject=? AND predicate=? AND source=?",
                    (subject.lower(), predicate, _CAUSAL_SOURCE),
                )
                conn.execute(
                    """INSERT INTO facts
                       (subject, predicate, object, confidence, source, timestamp, metadata)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        subject.lower(), predicate, object_val,
                        confidence, _CAUSAL_SOURCE,
                        now_iso,
                        json.dumps(metadata),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            _log.warning('CausalShockEngine: failed to write atom for %s: %s', subject, exc)

    @staticmethod
    def _parse_numeric(value: Optional[str]) -> Optional[float]:
        """Extract the leading numeric value from an atom string."""
        if value is None:
            return None
        try:
            # Handle '5.25%', 'CPI YoY: 4.0%', '4.35', etc.
            cleaned = value.replace('%', '').split()
            for token in cleaned:
                try:
                    return float(token)
                except ValueError:
                    continue
        except Exception:
            pass
        return None

    # ── Introspection ─────────────────────────────────────────────────────────

    def get_recent_shocks(self, n: int = 50) -> list:
        """
        Return the n most recent shock propagation events as plain dicts.
        Used by GET /causal/shocks.
        """
        events = list(self._shock_log)[:n]
        result = []
        for ev in events:
            result.append({
                'summary':          ev.summary,
                'propagated_at':    ev.propagated_at,
                'shock_predicate':  ev.shock.predicate,
                'shock_subject':    ev.shock.subject,
                'shock_direction':  ev.shock.direction,
                'shock_magnitude':  ev.shock.magnitude,
                'old_value':        ev.shock.old_value,
                'new_value':        ev.shock.new_value,
                'affected_tickers': ev.affected_tickers,
                'atoms_written':    ev.atoms_written,
                'traversal_depth':  ev.traversal_depth,
            })
        return result
