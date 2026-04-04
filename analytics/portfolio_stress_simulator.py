"""
analytics/portfolio_stress_simulator.py — Probability-Weighted Portfolio Stress Simulation

Combines the transition engine (what regimes are likely next) with
regime-conditional historical returns (how each holding performs in each regime)
to produce a probability-weighted forward P&L map.

ALGORITHM
=========
1. Load open paper positions for user
2. Get current market state from latest global snapshot
3. Get transition probabilities from TransitionEngine
4. For each possible next state (transition destination):
   a. Map state → regime label
   b. For each position, look up return_in_{regime} atom
      Fallback chain: ticker → sector → SPY proxy
   c. Compute portfolio_delta = Σ(position_value × return_pct / 100)
5. Compute expected_value = Σ(probability × portfolio_delta)

OUTPUT (dict — no KB atoms)
============================
{
  account_value: float,
  scenarios: [
    {
      state_label: str,
      regime: str,
      probability: float,
      observation_count: int,
      portfolio_delta_pct: float,
      portfolio_delta_gbp: float,
      description: str,
      confidence: str,
    }
  ],
  expected_value_pct: float,
  expected_value_gbp: float,
  worst_case_pct: float,
  best_case_pct: float,
  confidence: str,  # high / moderate / low / insufficient
  current_regime: str,
  n_positions: int,
}

USAGE
=====
  from analytics.portfolio_stress_simulator import PortfolioStressSimulator
  sim = PortfolioStressSimulator(db_path)
  result = sim.run(user_id='a2_0mk9r')
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Regime label normalisation ─────────────────────────────────────────────────
_REGIME_ALIASES = {
    'risk_on':           'risk_on_expansion',
    'risk_off':          'risk_off_contraction',
    'expansion':         'risk_on_expansion',
    'contraction':       'risk_off_contraction',
    'bull':              'risk_on_expansion',
    'bear':              'risk_off_contraction',
    'stag':              'stagflation',
    'recover':           'recovery',
    'recovery_mode':     'recovery',
}

# SPY fallback returns per regime (monthly %, from regime_history typical values)
_SPY_PROXY: Dict[str, float] = {
    'risk_on_expansion':     3.2,
    'risk_off_contraction': -4.1,
    'stagflation':          -1.5,
    'recovery':              2.8,
}


def _normalise_regime(raw: str) -> str:
    r = raw.lower().strip()
    return _REGIME_ALIASES.get(r, r)


class PortfolioStressSimulator:

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_open_positions(self, conn: sqlite3.Connection, user_id: str) -> List[dict]:
        rows = conn.execute(
            """SELECT ticker, direction, entry_price, quantity, t1, stop
               FROM paper_positions
               WHERE user_id=? AND status='open'""",
            (user_id,),
        ).fetchall()
        positions = []
        for r in rows:
            ep = float(r['entry_price'] or 0)
            qty = float(r['quantity'] or 0)
            positions.append({
                'ticker':    r['ticker'],
                'direction': r['direction'],
                'entry_price': ep,
                'quantity':  qty,
                'notional':  round(ep * qty, 2),
            })
        return positions

    def _get_account_value(self, conn: sqlite3.Connection, user_id: str) -> float:
        row = conn.execute(
            "SELECT virtual_balance FROM paper_account WHERE user_id=?",
            (user_id,),
        ).fetchone()
        return float(row['virtual_balance']) if row else 0.0

    def _get_ticker_sector(self, conn: sqlite3.Connection, ticker: str) -> Optional[str]:
        row = conn.execute(
            """SELECT object FROM facts
               WHERE subject=? AND predicate='sector'
               ORDER BY timestamp DESC LIMIT 1""",
            (ticker.lower(),),
        ).fetchone()
        return row['object'].lower() if row else None

    def _get_regime_return(
        self,
        conn: sqlite3.Connection,
        subject: str,
        regime: str,
    ) -> Optional[float]:
        """Read return_in_{regime} atom for a subject (ticker, sector, or 'spy')."""
        pred = f'return_in_{regime}'
        row = conn.execute(
            """SELECT object FROM facts
               WHERE subject=? AND predicate=?
               ORDER BY timestamp DESC LIMIT 1""",
            (subject.lower(), pred),
        ).fetchone()
        if row:
            try:
                return float(row['object'])
            except Exception:
                pass
        return None

    def _expected_return_for_position(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        direction: str,
        regime: str,
    ) -> float:
        """
        Get expected monthly return % for a position in a given regime.
        Fallback: sector average → SPY proxy.
        Direction-adjusted: bearish positions benefit from negative returns.
        """
        # 1. Try ticker directly
        ret = self._get_regime_return(conn, ticker, regime)

        # 2. Try sector average
        if ret is None:
            sector = self._get_ticker_sector(conn, ticker)
            if sector:
                ret = self._get_regime_return(conn, sector, regime)

        # 3. SPY proxy fallback
        if ret is None:
            ret = _SPY_PROXY.get(regime, 0.0)

        # Direction adjustment: bearish positions profit when ticker falls
        if direction == 'bearish':
            ret = -ret

        return ret

    def _get_current_state_and_transitions(
        self,
        conn: sqlite3.Connection,
    ):
        """
        Returns (current_regime, transitions_list) using TransitionEngine.
        transitions_list: list of TransitionProbability objects.
        """
        try:
            from analytics.state_transitions import TransitionEngine
            engine = TransitionEngine(self._db)
            forecast = engine.get_current_state_forecast(scope='global', subject='market')
            if forecast and forecast.transitions:
                # Extract regime from current state
                cs = forecast.current_state
                regime = getattr(cs, 'regime_label', None) or getattr(cs, 'price_regime', 'unknown')
                return regime, forecast.transitions, forecast.confidence
        except Exception as _e:
            _log.debug('TransitionEngine failed: %s', _e)

        # Fallback: try reading regime from facts
        row = conn.execute(
            """SELECT object FROM facts
               WHERE predicate IN ('regime_label','market_regime','price_regime')
               ORDER BY timestamp DESC LIMIT 1"""
        ).fetchone()
        regime = row['object'] if row else 'unknown'
        return regime, [], 'insufficient'

    def _map_transition_to_regime(self, transition) -> str:
        """Extract regime label from a TransitionProbability object."""
        try:
            cs = transition.to_state
            regime = (
                getattr(cs, 'regime_label', None) or
                getattr(cs, 'price_regime', None) or
                'unknown'
            )
            return _normalise_regime(str(regime))
        except Exception:
            return 'unknown'

    def run(self, user_id: str) -> dict:
        """Run stress simulation for user's open paper positions."""
        conn = self._conn()
        try:
            positions = self._get_open_positions(conn, user_id)
            account_value = self._get_account_value(conn, user_id)

            if not positions:
                return {
                    'account_value': account_value,
                    'scenarios': [],
                    'expected_value_pct': 0.0,
                    'expected_value_gbp': 0.0,
                    'worst_case_pct': 0.0,
                    'best_case_pct': 0.0,
                    'confidence': 'insufficient',
                    'current_regime': 'unknown',
                    'n_positions': 0,
                    'message': 'No open positions to stress-test.',
                }

            total_notional = sum(p['notional'] for p in positions)
            current_regime, transitions, engine_confidence = \
                self._get_current_state_and_transitions(conn)

            # If no transitions, use static regime scenarios
            if not transitions:
                static_regimes = [
                    ('risk_on_expansion',    0.30),
                    ('risk_off_contraction', 0.25),
                    ('stagflation',          0.20),
                    ('recovery',             0.25),
                ]
                scenarios_input = [
                    (r, p, 0, 'low') for r, p in static_regimes
                ]
            else:
                scenarios_input = [
                    (
                        self._map_transition_to_regime(t),
                        t.probability,
                        t.observation_count,
                        t.confidence,
                    )
                    for t in transitions[:8]  # cap at 8 scenarios
                ]

            scenarios = []
            for regime, prob, obs_count, conf in scenarios_input:
                if regime == 'unknown':
                    continue

                # Compute portfolio delta for this regime
                port_delta_gbp = 0.0
                for pos in positions:
                    ret_pct = self._expected_return_for_position(
                        conn, pos['ticker'], pos['direction'], regime
                    )
                    pos_delta = pos['notional'] * ret_pct / 100.0
                    port_delta_gbp += pos_delta

                port_delta_pct = (
                    round(port_delta_gbp / account_value * 100, 2)
                    if account_value > 0 else 0.0
                )

                # Human-readable description
                sign = '+' if port_delta_pct >= 0 else ''
                desc = (
                    f"Regime transitions to {regime.replace('_', ' ')} "
                    f"(prob={prob:.0%}): portfolio {sign}{port_delta_pct:.1f}%"
                )
                if obs_count:
                    desc += f" (based on {obs_count} historical transitions)"

                scenarios.append({
                    'state_label':         regime.replace('_', ' ').title(),
                    'regime':              regime,
                    'probability':         round(prob, 4),
                    'observation_count':   obs_count,
                    'portfolio_delta_pct': port_delta_pct,
                    'portfolio_delta_gbp': round(port_delta_gbp, 2),
                    'description':         desc,
                    'confidence':          conf,
                })

            # Sort by probability descending
            scenarios.sort(key=lambda s: s['probability'], reverse=True)

            # Expected value = probability-weighted sum
            ev_pct = round(
                sum(s['probability'] * s['portfolio_delta_pct'] for s in scenarios), 2
            )
            ev_gbp = round(
                sum(s['probability'] * s['portfolio_delta_gbp'] for s in scenarios), 2
            )

            worst = min((s['portfolio_delta_pct'] for s in scenarios), default=0.0)
            best  = max((s['portfolio_delta_pct'] for s in scenarios), default=0.0)

            # Overall confidence
            total_obs = sum(s['observation_count'] for s in scenarios)
            if total_obs >= 30:
                overall_conf = 'high'
            elif total_obs >= 10:
                overall_conf = 'moderate'
            elif total_obs >= 3:
                overall_conf = 'low'
            else:
                overall_conf = 'insufficient'

            return {
                'account_value':      round(account_value, 2),
                'total_notional':     round(total_notional, 2),
                'scenarios':          scenarios,
                'expected_value_pct': ev_pct,
                'expected_value_gbp': ev_gbp,
                'worst_case_pct':     round(worst, 2),
                'best_case_pct':      round(best, 2),
                'confidence':         overall_conf,
                'current_regime':     current_regime,
                'n_positions':        len(positions),
                'positions_summary':  [
                    {'ticker': p['ticker'], 'notional': p['notional']}
                    for p in positions
                ],
            }

        finally:
            conn.close()
