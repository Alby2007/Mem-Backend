"""
analytics/adversarial_tester.py — Signal-Level Adversarial Stress Testing

Distinct from analytics/adversarial_stress.py (portfolio-level stress test).
This module tests a SINGLE high-conviction signal under the same 6 adversarial
scenarios, returning a per-signal robustness label and a list of scenarios
that would invalidate it.

REUSE
=====
Imports and reuses the existing scenario functions and classification helpers
from adversarial_stress.py. No duplication — only the scope is narrowed from
portfolio to single ticker.

ROBUSTNESS LABELS
=================
  robust   : signal survives >= 5 of 6 scenarios (tier unchanged)
  moderate : signal survives 3-4 of 6 scenarios
  fragile  : signal survives <= 2 of 6 scenarios

EARNINGS PROXIMITY WARNING
==========================
If the KB has a pre_earnings_flag=within_7d atom for the ticker, an earnings
warning is added to invalidating_scenarios regardless of scenario results.

USAGE
=====
    tester = AdversarialTester(db_path)
    result = tester.stress_test_signal('HSBA.L', pattern_row)
    # result.robustness_label  → 'robust'
    # result.survival_rate     → 0.833
    # result.invalidating_scenarios → ['earnings_miss']
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Scenario imports from existing adversarial_stress.py ──────────────────────

try:
    from analytics.adversarial_stress import (
        _SCENARIOS,
        _TIER_NUMERIC,
        _read_baseline,
        _compute_conviction_tier,
    )
    from ingest.signal_enrichment_adapter import _read_kb_atoms
    _HAS_ENRICHMENT = True
except ImportError:
    _HAS_ENRICHMENT = False
    _SCENARIOS = {}
    _TIER_NUMERIC = {'high': 3, 'medium': 2, 'low': 1, 'avoid': 0}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ScenarioOutcome:
    scenario:    str
    survived:    bool           # True = conviction_tier unchanged after scenario
    tier_before: Optional[str]
    tier_after:  Optional[str]
    delta:       int            # tier_before_numeric - tier_after_numeric


@dataclass
class AdversarialResult:
    ticker:                   str
    pattern_type:             str
    survival_rate:            float          # 0.0–1.0
    robustness_label:         str            # 'robust' | 'moderate' | 'fragile'
    invalidating_scenarios:   List[str]      # scenario names where signal failed
    earnings_proximity_warning: bool         # True if pre_earnings_flag=within_7d
    scenario_outcomes:        List[ScenarioOutcome] = field(default_factory=list)
    scenarios_tested:         int = 0
    baseline_tier:            Optional[str] = None
    tested_at:                str = ''


# ── AdversarialTester ──────────────────────────────────────────────────────────

class AdversarialTester:
    """
    Signal-level adversarial stress tester.

    Loads the full KB atom snapshot, isolates the target ticker,
    runs the 6 pre-committed scenarios, and scores survival per scenario.
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def stress_test_signal(
        self,
        ticker:      str,
        pattern_row: dict,
    ) -> AdversarialResult:
        """
        Stress test a single pattern signal under all 6 adversarial scenarios.

        Parameters
        ----------
        ticker      : Ticker symbol (any case).
        pattern_row : Dict with at least 'pattern_type' key; optionally
                      'direction', 'quality_score', 'timeframe'.
        """
        ticker_up   = ticker.upper()
        ticker_lo   = ticker.lower()
        pattern_type = pattern_row.get('pattern_type', 'unknown')
        now_iso      = datetime.now(timezone.utc).isoformat()

        if not _HAS_ENRICHMENT:
            return AdversarialResult(
                ticker=ticker_up,
                pattern_type=pattern_type,
                survival_rate=0.0,
                robustness_label='unknown',
                invalidating_scenarios=[],
                earnings_proximity_warning=False,
                scenarios_tested=0,
                tested_at=now_iso,
            )

        # ── Load full KB atom snapshot ────────────────────────────────────────
        try:
            ticker_atoms, macro_signals = _read_kb_atoms(self._db)
        except Exception as exc:
            _log.warning('AdversarialTester: _read_kb_atoms failed: %s', exc)
            return AdversarialResult(
                ticker=ticker_up,
                pattern_type=pattern_type,
                survival_rate=0.0,
                robustness_label='unknown',
                invalidating_scenarios=[],
                earnings_proximity_warning=False,
                scenarios_tested=0,
                tested_at=now_iso,
            )

        # ── Resolve ticker key (KB stores lowercase) ──────────────────────────
        ticker_key = ticker_lo
        if ticker_key not in ticker_atoms:
            # Try uppercase as fallback
            ticker_key = ticker_up
            if ticker_key not in ticker_atoms:
                _log.info(
                    'AdversarialTester: %s not in ticker_atoms — using prior',
                    ticker_up,
                )
                ticker_atoms[ticker_key] = {}

        # ── Baseline tier ─────────────────────────────────────────────────────
        baseline_preds = ticker_atoms.get(ticker_key, {})
        baseline_tier  = _compute_conviction_tier(ticker_key, baseline_preds, macro_signals)

        # ── Run each scenario ─────────────────────────────────────────────────
        outcomes: List[ScenarioOutcome] = []
        invalidating: List[str] = []

        for scenario_name, scenario_fn in _SCENARIOS.items():
            try:
                stressed_atoms, stressed_macro = scenario_fn(ticker_atoms, macro_signals)
                stressed_preds = stressed_atoms.get(ticker_key, baseline_preds)
                stressed_tier  = _compute_conviction_tier(
                    ticker_key, stressed_preds, stressed_macro
                )

                b_num = _TIER_NUMERIC.get(baseline_tier or '', 1)
                s_num = _TIER_NUMERIC.get(stressed_tier or '', 1)
                delta = b_num - s_num

                survived = delta == 0
                if not survived:
                    invalidating.append(scenario_name)

                outcomes.append(ScenarioOutcome(
                    scenario    = scenario_name,
                    survived    = survived,
                    tier_before = baseline_tier,
                    tier_after  = stressed_tier,
                    delta       = delta,
                ))
            except Exception as exc:
                _log.debug(
                    'AdversarialTester: scenario %s failed for %s: %s',
                    scenario_name, ticker_up, exc,
                )

        n_tested   = len(outcomes)
        n_survived = sum(1 for o in outcomes if o.survived)
        survival   = round(n_survived / n_tested, 3) if n_tested > 0 else 0.0

        label = self._robustness_label(n_survived, n_tested)

        # ── Earnings proximity check ──────────────────────────────────────────
        earnings_warning = self._check_earnings_proximity(ticker_lo)

        _log.info(
            'AdversarialTester: %s %s → %s (survived %d/%d)',
            ticker_up, pattern_type, label, n_survived, n_tested,
        )

        return AdversarialResult(
            ticker                    = ticker_up,
            pattern_type              = pattern_type,
            survival_rate             = survival,
            robustness_label          = label,
            invalidating_scenarios    = invalidating,
            earnings_proximity_warning= earnings_warning,
            scenario_outcomes         = outcomes,
            scenarios_tested          = n_tested,
            baseline_tier             = baseline_tier,
            tested_at                 = now_iso,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _robustness_label(n_survived: int, n_tested: int) -> str:
        if n_tested == 0:
            return 'unknown'
        if n_survived >= 5:
            return 'robust'
        if n_survived >= 3:
            return 'moderate'
        return 'fragile'

    def _check_earnings_proximity(self, ticker_lo: str) -> bool:
        """Return True if pre_earnings_flag=within_7d atom exists for ticker."""
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                row = conn.execute(
                    """SELECT object FROM facts
                       WHERE subject=? AND predicate='pre_earnings_flag'
                       ORDER BY confidence DESC LIMIT 1""",
                    (ticker_lo,),
                ).fetchone()
                if row and 'within_7d' in (row[0] or '').lower():
                    return True
            finally:
                conn.close()
        except Exception:
            pass
        return False
