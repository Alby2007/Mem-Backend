"""
analytics/signal_forecaster.py — Probabilistic Signal Forecasting

Converts a (ticker, pattern_type, timeframe) tuple into a probability
distribution over outcomes using historical calibration data adjusted by
current market conditions (IV rank, macro confirmation, short tension).

DESIGN
======
1. Fetches the signal_calibration row for (ticker, pattern_type, timeframe,
   current_regime) — falls back to regime-agnostic row if absent.
2. Applies three KB-sourced adjustments:
   - IV rank   : high IV (>70) → wider distribution, lower T2 probability
   - Macro      : confirmed macro → probability boost; unconfirmed → penalty
   - Short      : heavy FCA short interest → slight headwind on T1 probability
3. Runs a Monte Carlo simulation (n=5,000 Bernoulli trials) over adjusted
   probabilities to produce a full outcome distribution.
4. Computes expected value in account currency using PositionRecommendation R:R.

SEEDING
=======
When called from TipScheduler (recording to prediction ledger), pass:
    seed=f"{ticker}{pattern_type}{issued_at_iso}"
This makes Monte Carlo deterministic — same signal always records the same
stated probability. When called from GET /forecast/ (exploratory), leave
seed=None for natural variance.

FALLBACK BEHAVIOUR
==================
If signal_calibration has no row (< 10 samples), falls back to population-
level priors:  p_t1=0.55, p_t2=0.38, p_stop=0.20.
These are conservative estimates reflecting typical ICT pattern base rates
from the historical_calibration backfill (3-year window).

PERFORMANCE
===========
Pure Python, no numpy. ~5ms per call (5,000 trials).
"""

from __future__ import annotations

import logging
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from analytics.signal_calibration import get_calibration, CalibrationResult

_log = logging.getLogger(__name__)

# ── Population-level priors (used when calibration row absent) ─────────────────

_PRIOR_P_T1   = 0.55
_PRIOR_P_T2   = 0.38
_PRIOR_P_STOP = 0.20
_PRIOR_MEDIAN_DAYS = 5.0

# ── IV rank adjustment thresholds ─────────────────────────────────────────────

_IV_HIGH_THRESHOLD       = 70.0   # IV rank above → bearish for breakout probability
_IV_COMPRESSED_THRESHOLD = 30.0   # IV rank below → favourable for breakout

_IV_HIGH_T2_MULTIPLIER        = 0.85
_IV_COMPRESSED_T2_MULTIPLIER  = 1.10

# ── Macro / short tension adjustments ─────────────────────────────────────────

_MACRO_CONFIRMED_MULT   = 1.15
_MACRO_UNCONFIRMED_MULT = 0.85
_SHORT_TENSION_THRESHOLD = 3.0    # FCA disclosed short % above → headwind
_SHORT_TENSION_MULT      = 0.90

# ── Monte Carlo params ─────────────────────────────────────────────────────────

_MC_TRIALS = 5_000

# ── Probability floor / ceiling ────────────────────────────────────────────────

_P_MIN = 0.05
_P_MAX = 0.95


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ForecastResult:
    """Full probability distribution over pattern outcomes."""
    ticker:               str
    pattern_type:         str
    timeframe:            str
    market_regime:        Optional[str]

    # Probabilities (adjusted, post-Monte Carlo)
    # p_hit_t1 + p_hit_t2 + p_stopped_out + p_expired = 1.0
    p_hit_t1:             float   # probability of reaching T1
    p_hit_t2:             float   # probability of reaching T2
    p_stopped_out:        float   # probability of stop hit before T1
    p_expired:            float   # probability of expiry without hitting T1 or stop

    # Expected value
    expected_value_gbp:   float   # EV at stated risk; positive = favourable
    ci_90_low:            float   # 90% confidence interval lower bound (currency)
    ci_90_high:           float   # 90% confidence interval upper bound (currency)
    days_to_target_median: float  # median days to T1 based on calibration

    # Adjustment breakdown
    regime_adjustment_pct: float  # % change applied due to regime (0 = neutral)
    iv_adjustment_pct:     float  # % change applied due to IV rank
    macro_adjustment_pct:  float  # % change applied due to macro confirmation
    short_adjustment_pct:  float  # % change applied due to short interest

    # Metadata
    calibration_samples:  int     # sample size backing the base rates (0 = prior)
    used_prior:           bool    # True if falling back to population-level priors
    generated_at:         str


@dataclass
class _MCResult:
    """Raw Monte Carlo output before currency mapping."""
    p_t1:          float
    p_t2:          float
    p_stop:        float
    median_days:   float
    ci_90_low_r:   float   # in units of R (risk multiples)
    ci_90_high_r:  float


# ── KB atom helpers ────────────────────────────────────────────────────────────

def _read_kb_atom(db_path: str, subject: str, predicate: str) -> Optional[str]:
    """Read a single atom value from the KB. Returns None if absent."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
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


def _get_iv_rank(db_path: str, ticker: str) -> Optional[float]:
    """Return iv_rank atom value as float, or None."""
    val = _read_kb_atom(db_path, ticker, 'iv_rank')
    if val is None:
        return None
    try:
        return float(val.split()[0].rstrip('%'))
    except (ValueError, AttributeError):
        return None


def _get_macro_confirmed(db_path: str, ticker: str) -> Optional[bool]:
    """
    Return True if macro_confirmation atom is 'confirmed', False if
    'unconfirmed', None if absent.
    """
    val = _read_kb_atom(db_path, ticker, 'macro_confirmation')
    if val is None:
        return None
    v = val.lower()
    if 'confirmed' in v and 'un' not in v:
        return True
    if 'unconfirmed' in v or 'not confirmed' in v:
        return False
    return None


def _get_short_pct(db_path: str, ticker: str) -> Optional[float]:
    """
    Return FCA short interest as percentage float, or None.
    Handles values like '3.45% (Bridgewater Associates)'.
    """
    val = _read_kb_atom(db_path, ticker, 'fca_short_interest')
    if val is None:
        return None
    try:
        return float(val.split('%')[0].strip())
    except (ValueError, AttributeError):
        return None


def _get_current_regime(db_path: str) -> Optional[str]:
    """Return the current macro regime from uk_macro or global_macro_regime."""
    val = _read_kb_atom(db_path, 'uk_macro', 'regime_label')
    if val:
        return val.strip()
    val = _read_kb_atom(db_path, 'us_macro', 'regime_label')
    return val.strip() if val else None


# ── Monte Carlo simulation ─────────────────────────────────────────────────────

def _run_monte_carlo(
    p_t1: float,
    p_t2_given_t1: float,
    p_stop: float,
    avg_days: float,
    n: int = _MC_TRIALS,
) -> _MCResult:
    """
    Run n Bernoulli trials.
    Returns empirical probabilities and R-denominated confidence interval.

    Outcome per trial:
      - Draw stop first: if random() < p_stop → stopped out
      - Else draw T1:    if random() < p_t1   → hit T1
        - If T1 hit, draw T2: if random() < p_t2_given_t1 → hit T2
    """
    hit_t1 = 0
    hit_t2 = 0
    stopped = 0
    outcomes_r: list[float] = []

    for _ in range(n):
        if random.random() < p_stop:
            stopped += 1
            outcomes_r.append(-1.0)      # -1R (stop out)
        elif random.random() < p_t1:
            hit_t1 += 1
            if random.random() < p_t2_given_t1:
                hit_t2 += 1
                outcomes_r.append(2.0)   # +2R
            else:
                outcomes_r.append(1.0)   # +1R
        else:
            outcomes_r.append(0.0)       # expired, neither T1 nor stopped

    # Sort for percentile computation
    outcomes_r.sort()
    idx_5  = int(0.05 * n)
    idx_95 = int(0.95 * n)
    ci_low  = outcomes_r[idx_5]
    ci_high = outcomes_r[idx_95]

    return _MCResult(
        p_t1        = hit_t1  / n,
        p_t2        = hit_t2  / n,
        p_stop      = stopped / n,
        median_days = avg_days if avg_days else _PRIOR_MEDIAN_DAYS,
        ci_90_low_r  = ci_low,
        ci_90_high_r = ci_high,
    )


# ── SignalForecaster ───────────────────────────────────────────────────────────

class SignalForecaster:
    """
    Produces a ForecastResult for a (ticker, pattern_type, timeframe) tuple.

    Usage
    -----
    forecaster = SignalForecaster(db_path)

    # Deterministic (tip issuance — always seed for ledger reproducibility)
    result = forecaster.forecast(
        ticker='HSBA.L', pattern_type='fvg', timeframe='1d',
        account_size=10_000, risk_pct=1.0,
        seed='HSBA.Lfvg2026-02-27T08:00:00+00:00'
    )

    # Exploratory (GET /forecast/ endpoint — unseeded, natural variance)
    result = forecaster.forecast('NVDA', 'order_block', '4h', 10_000, 1.0)
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def forecast(
        self,
        ticker: str,
        pattern_type: str,
        timeframe: str = '1d',
        account_size: float = 10_000.0,
        risk_pct: float = 1.0,
        seed: Optional[str] = None,
    ) -> ForecastResult:
        """
        Compute probability distribution over pattern outcomes.

        Parameters
        ----------
        ticker        : Ticker symbol (any case; normalised internally).
        pattern_type  : Pattern key e.g. 'fvg', 'order_block', 'breaker'.
        timeframe     : Timeframe key e.g. '1d', '4h', '1h'.
        account_size  : User account size in account_currency (for EV calc).
        risk_pct      : Max risk per trade as percentage (e.g. 1.0 = 1%).
        seed          : If provided, Monte Carlo is seeded deterministically.
                        Pass f"{ticker}{pattern_type}{issued_at_iso}" from
                        TipScheduler. Leave None for exploratory calls.
        """
        ticker_up = ticker.upper()
        now_iso   = datetime.now(timezone.utc).isoformat()

        # ── 1. Fetch calibration base rates ───────────────────────────────────
        regime     = _get_current_regime(self._db)
        cal: Optional[CalibrationResult] = get_calibration(
            ticker_up, pattern_type, timeframe, self._db, market_regime=regime
        )

        if cal is not None:
            base_p_t1   = cal.hit_rate_t1   or _PRIOR_P_T1
            base_p_t2   = cal.hit_rate_t2   or _PRIOR_P_T2
            base_p_stop = cal.stopped_out_rate or _PRIOR_P_STOP
            avg_days    = (cal.avg_time_to_target_hours or (_PRIOR_MEDIAN_DAYS * 8)) / 8
            used_prior  = False
            n_samples   = cal.sample_size
        else:
            base_p_t1   = _PRIOR_P_T1
            base_p_t2   = _PRIOR_P_T2
            base_p_stop = _PRIOR_P_STOP
            avg_days    = _PRIOR_MEDIAN_DAYS
            used_prior  = True
            n_samples   = 0

        # ── 2. Compute p_t2_given_t1 (conditional) ────────────────────────────
        # p(T2) = p(T1) × p(T2 | T1)  ⟹  p(T2|T1) = p(T2) / p(T1)
        p_t2_given_t1 = (base_p_t2 / base_p_t1) if base_p_t1 > 0 else 0.5
        p_t2_given_t1 = min(p_t2_given_t1, 0.95)

        # ── 3. KB-sourced adjustments ─────────────────────────────────────────
        adj_p_t1   = base_p_t1
        adj_p_t2   = base_p_t2
        adj_p_stop = base_p_stop

        iv_adj_pct    = 0.0
        macro_adj_pct = 0.0
        short_adj_pct = 0.0

        # IV rank
        iv_rank = _get_iv_rank(self._db, ticker_up)
        if iv_rank is not None:
            if iv_rank > _IV_HIGH_THRESHOLD:
                adj_p_t2     *= _IV_HIGH_T2_MULTIPLIER
                iv_adj_pct    = (_IV_HIGH_T2_MULTIPLIER - 1.0) * 100
            elif iv_rank < _IV_COMPRESSED_THRESHOLD:
                adj_p_t2     *= _IV_COMPRESSED_T2_MULTIPLIER
                iv_adj_pct    = (_IV_COMPRESSED_T2_MULTIPLIER - 1.0) * 100

        # Macro confirmation
        macro_ok = _get_macro_confirmed(self._db, ticker_up)
        if macro_ok is True:
            adj_p_t1   *= _MACRO_CONFIRMED_MULT
            adj_p_t2   *= _MACRO_CONFIRMED_MULT
            macro_adj_pct = (_MACRO_CONFIRMED_MULT - 1.0) * 100
        elif macro_ok is False:
            adj_p_t1   *= _MACRO_UNCONFIRMED_MULT
            adj_p_t2   *= _MACRO_UNCONFIRMED_MULT
            macro_adj_pct = (_MACRO_UNCONFIRMED_MULT - 1.0) * 100

        # Short tension
        short_pct = _get_short_pct(self._db, ticker_up)
        if short_pct is not None and short_pct > _SHORT_TENSION_THRESHOLD:
            adj_p_t1   *= _SHORT_TENSION_MULT
            short_adj_pct = (_SHORT_TENSION_MULT - 1.0) * 100

        # Clamp all probabilities
        adj_p_t1   = max(_P_MIN, min(_P_MAX, adj_p_t1))
        adj_p_t2   = max(_P_MIN, min(_P_MAX, adj_p_t2))
        adj_p_stop = max(_P_MIN, min(_P_MAX, adj_p_stop))

        # Recompute conditional after adjustments
        p_t2_given_t1 = min((adj_p_t2 / adj_p_t1) if adj_p_t1 > 0 else 0.5, 0.95)

        # ── 4. Monte Carlo ────────────────────────────────────────────────────
        if seed is not None:
            random.seed(hash(seed) & 0xFFFFFFFF)

        mc = _run_monte_carlo(adj_p_t1, p_t2_given_t1, adj_p_stop, avg_days)

        # Reset seed state so caller's random is not affected
        if seed is not None:
            random.seed()

        # ── 5. Currency mapping (R → £/$ amounts) ─────────────────────────────
        risk_amount = account_size * risk_pct / 100.0
        ev_gbp      = (mc.p_t1 * risk_amount
                       + mc.p_t2 * risk_amount * 2
                       - mc.p_stop * risk_amount)
        ci_low_gbp  = mc.ci_90_low_r  * risk_amount
        ci_high_gbp = mc.ci_90_high_r * risk_amount

        # ── 6. Regime adjustment pct ──────────────────────────────────────────
        # Summarise net regime effect relative to prior baseline
        net_t1_change = adj_p_t1 - base_p_t1
        regime_adj_pct = round((net_t1_change / base_p_t1) * 100, 1) if base_p_t1 > 0 else 0.0

        _log.debug(
            'SignalForecaster: %s %s %s → p_t1=%.2f p_t2=%.2f p_stop=%.2f '
            'ev=%.2f samples=%d prior=%s',
            ticker_up, pattern_type, timeframe,
            mc.p_t1, mc.p_t2, mc.p_stop, ev_gbp, n_samples, used_prior,
        )

        p_expired = round(max(0.0, 1.0 - mc.p_t1 - mc.p_t2 - mc.p_stop), 3)

        return ForecastResult(
            ticker                = ticker_up,
            pattern_type          = pattern_type,
            timeframe             = timeframe,
            market_regime         = regime,
            p_hit_t1              = round(mc.p_t1,  3),
            p_hit_t2              = round(mc.p_t2,  3),
            p_stopped_out         = round(mc.p_stop, 3),
            p_expired             = p_expired,
            expected_value_gbp    = round(ev_gbp,    2),
            ci_90_low             = round(ci_low_gbp,  2),
            ci_90_high            = round(ci_high_gbp, 2),
            days_to_target_median = round(mc.median_days, 1),
            regime_adjustment_pct = round(regime_adj_pct, 1),
            iv_adjustment_pct     = round(iv_adj_pct, 1),
            macro_adjustment_pct  = round(macro_adj_pct, 1),
            short_adjustment_pct  = round(short_adj_pct, 1),
            calibration_samples   = n_samples,
            used_prior            = used_prior,
            generated_at          = now_iso,
        )
