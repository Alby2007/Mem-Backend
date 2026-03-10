"""
analytics/calibration_correction.py — Correction Factor for Bot-Only Calibration

When both bot and user observations exist for the same calibration cell, computes
the performance gap and a correction multiplier. Applied by get_calibration() to
bot-only cells to adjust for paper-trading optimism.

Returns 1.0 initially — no correction until real user data arrives. As users
generate outcomes the correction factor emerges naturally and is applied to
all bot-only calibration cells.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import List, Optional

_log = logging.getLogger(__name__)

_MIN_OBS = 10  # minimum observations on each side to compute a correction


@dataclass
class CorrectionFactor:
    cell_key: str            # "NVDA|fvg|4h|recovery"
    bot_hit_rate: float
    user_hit_rate: float
    gap_pct: float           # user_hr - bot_hr (negative = bots overperform)
    bot_n: int
    user_n: int
    correction: float        # multiplier to apply to bot-only cells
    confidence: str          # 'high' (both ≥20) | 'low' (either <10)


def compute_correction_factors(db_path: str) -> List[CorrectionFactor]:
    """
    For each calibration cell where both bot and user observations ≥ _MIN_OBS,
    compute the correction multiplier = user_hr / bot_hr.

    Returns an empty list if there is insufficient data (cold start).
    """
    factors: List[CorrectionFactor] = []
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            # Check table exists
            tbl_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='calibration_observations'"
            ).fetchone()
            if not tbl_exists:
                return factors

            # Get all cells that have both bot and user observations
            cells = conn.execute(
                """SELECT ticker, pattern_type, timeframe, market_regime
                   FROM signal_calibration
                   WHERE COALESCE(bot_observations, 0) >= ?
                     AND COALESCE(user_observations, 0) >= ?""",
                (_MIN_OBS, _MIN_OBS),
            ).fetchall()

            for cell in cells:
                ticker       = cell['ticker']
                pattern_type = cell['pattern_type']
                timeframe    = cell['timeframe']
                market_regime = cell['market_regime']

                # Bot hit rate from observation log
                bot_rows = conn.execute(
                    """SELECT outcome FROM calibration_observations
                       WHERE source='paper_bot'
                         AND ticker=? AND pattern_type=? AND timeframe=?
                         AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
                    (ticker, pattern_type, timeframe, market_regime, market_regime),
                ).fetchall()

                # User hit rate from observation log
                user_rows = conn.execute(
                    """SELECT outcome FROM calibration_observations
                       WHERE source IN ('user', 'user_feedback')
                         AND ticker=? AND pattern_type=? AND timeframe=?
                         AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
                    (ticker, pattern_type, timeframe, market_regime, market_regime),
                ).fetchall()

                bot_n  = len(bot_rows)
                user_n = len(user_rows)
                if bot_n < _MIN_OBS or user_n < _MIN_OBS:
                    continue

                _t1_outcomes = ('hit_t1', 'hit_t2', 'hit_t3')
                bot_hr  = sum(1 for r in bot_rows  if r['outcome'] in _t1_outcomes) / bot_n
                user_hr = sum(1 for r in user_rows if r['outcome'] in _t1_outcomes) / user_n

                correction = (user_hr / bot_hr) if bot_hr > 0 else 1.0
                confidence = 'high' if (bot_n >= 20 and user_n >= 20) else 'low'
                cell_key   = f"{ticker}|{pattern_type}|{timeframe}|{market_regime or 'any'}"

                factors.append(CorrectionFactor(
                    cell_key=cell_key,
                    bot_hit_rate=round(bot_hr, 4),
                    user_hit_rate=round(user_hr, 4),
                    gap_pct=round(user_hr - bot_hr, 4),
                    bot_n=bot_n,
                    user_n=user_n,
                    correction=round(correction, 4),
                    confidence=confidence,
                ))
        finally:
            conn.close()
    except Exception as e:
        _log.debug('compute_correction_factors failed: %s', e)

    return factors


def get_global_correction(db_path: str) -> float:
    """
    Weighted average correction factor across all cells with sufficient data.
    Returns 1.0 when there is no user data to compare against (cold start).
    Applied by get_calibration() to bot-only cells.
    """
    try:
        factors = compute_correction_factors(db_path)
        if not factors:
            return 1.0

        # Weight by user_n (larger real-world samples carry more weight)
        total_weight = sum(f.user_n for f in factors)
        if total_weight == 0:
            return 1.0

        weighted_correction = sum(f.correction * f.user_n for f in factors)
        return round(weighted_correction / total_weight, 4)
    except Exception as e:
        _log.debug('get_global_correction failed: %s', e)
        return 1.0
