"""
analytics/signal_calibration.py — Collective Signal Calibration

Tracks collective hit rates per (ticker, pattern_type, timeframe, market_regime)
across all users' tip feedback. Used by tip_scheduler and tip_formatter to
surface historically grounded confidence levels.

Confidence gating:
  < 10 samples   → None (not surfaced)
  10–29 samples  → 'low'
  30–99 samples  → 'moderate'
  >= 100 samples → 'established'
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)


# ── CalibrationResult ─────────────────────────────────────────────────────────

@dataclass
class CalibrationResult:
    ticker: str
    pattern_type: str
    timeframe: str
    market_regime: Optional[str]
    sample_size: int
    hit_rate_t1: Optional[float]
    hit_rate_t2: Optional[float]
    hit_rate_t3: Optional[float]
    stopped_out_rate: Optional[float]
    avg_time_to_target_hours: Optional[float]
    calibration_confidence: float   # 0.0–1.0
    confidence_label: str           # 'low' | 'moderate' | 'established'
    last_updated: str


def _confidence_label(sample_size: int) -> str:
    if sample_size >= 100:
        return 'established'
    if sample_size >= 30:
        return 'moderate'
    if sample_size >= 10:
        return 'low'
    return 'insufficient'


def _confidence_score(sample_size: int) -> float:
    """Map sample_size to a 0.0–1.0 calibration_confidence score."""
    if sample_size >= 100:
        return min(1.0, 0.50 + sample_size / 1000)
    if sample_size >= 30:
        return 0.30 + (sample_size - 30) / 140 * 0.20
    if sample_size >= 10:
        return 0.10 + (sample_size - 10) / 20 * 0.20
    return 0.0


# ── Schema helper ─────────────────────────────────────────────────────────────

def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_calibration (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker                   TEXT NOT NULL,
            pattern_type             TEXT NOT NULL,
            timeframe                TEXT NOT NULL,
            market_regime            TEXT,
            sample_size              INTEGER DEFAULT 0,
            hit_rate_t1              REAL,
            hit_rate_t2              REAL,
            hit_rate_t3              REAL,
            stopped_out_rate         REAL,
            avg_time_to_target_hours REAL,
            calibration_confidence   REAL DEFAULT 0.0,
            last_updated             TEXT NOT NULL,
            UNIQUE(ticker, pattern_type, timeframe, market_regime)
        )
    """)
    conn.commit()


# ── update_calibration ────────────────────────────────────────────────────────

def update_calibration(
    ticker: str,
    pattern_type: str,
    timeframe: str,
    market_regime: Optional[str],
    outcome: str,
    db_path: str,
) -> None:
    """
    Update the calibration row for (ticker, pattern_type, timeframe, market_regime)
    based on one new outcome from POST /feedback.

    outcome values: 'hit_t1' | 'hit_t2' | 'hit_t3' | 'stopped_out' | 'pending' | 'skipped'
    Only hit_t* and stopped_out are counted (pending/skipped ignored).
    """
    if outcome not in ('hit_t1', 'hit_t2', 'hit_t3', 'stopped_out'):
        return

    now = datetime.now(timezone.utc).isoformat()
    ticker = ticker.upper()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_table(conn)

        # Fetch existing row
        row = conn.execute(
            """SELECT sample_size, hit_rate_t1, hit_rate_t2, hit_rate_t3,
                      stopped_out_rate, avg_time_to_target_hours
               FROM signal_calibration
               WHERE ticker=? AND pattern_type=? AND timeframe=?
                 AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
            (ticker, pattern_type, timeframe, market_regime, market_regime),
        ).fetchone()

        if row:
            n, hr1, hr2, hr3, sor, atth = row
            n = n or 0
            hr1 = hr1 or 0.0
            hr2 = hr2 or 0.0
            hr3 = hr3 or 0.0
            sor = sor or 0.0
        else:
            n, hr1, hr2, hr3, sor, atth = 0, 0.0, 0.0, 0.0, 0.0, None

        # Incremental mean update: new_mean = (old_mean * n + new_val) / (n + 1)
        new_n = n + 1
        new_hr1 = (hr1 * n + (1.0 if outcome == 'hit_t1' else 0.0)) / new_n
        new_hr2 = (hr2 * n + (1.0 if outcome in ('hit_t2', 'hit_t3') else 0.0)) / new_n
        new_hr3 = (hr3 * n + (1.0 if outcome == 'hit_t3' else 0.0)) / new_n
        new_sor = (sor * n + (1.0 if outcome == 'stopped_out' else 0.0)) / new_n

        conf_score = _confidence_score(new_n)

        conn.execute(
            """INSERT INTO signal_calibration
               (ticker, pattern_type, timeframe, market_regime, sample_size,
                hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                calibration_confidence, last_updated)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(ticker, pattern_type, timeframe, market_regime)
               DO UPDATE SET
                 sample_size=excluded.sample_size,
                 hit_rate_t1=excluded.hit_rate_t1,
                 hit_rate_t2=excluded.hit_rate_t2,
                 hit_rate_t3=excluded.hit_rate_t3,
                 stopped_out_rate=excluded.stopped_out_rate,
                 calibration_confidence=excluded.calibration_confidence,
                 last_updated=excluded.last_updated""",
            (ticker, pattern_type, timeframe, market_regime, new_n,
             round(new_hr1, 4), round(new_hr2, 4), round(new_hr3, 4),
             round(new_sor, 4), round(conf_score, 4), now),
        )
        conn.commit()
    finally:
        conn.close()


# ── get_global_baseline ───────────────────────────────────────────────────────

def get_global_baseline(
    pattern_type: str,
    timeframe: str,
    db_path: str,
    min_samples: int = 30,
) -> Optional[float]:
    """
    Return the sample-weighted mean hit_rate_t1 across ALL tickers for a
    given (pattern_type, timeframe), pooling every regime.

    Only rows with sample_size >= min_samples contribute to avoid noisy
    one-trade tickers dragging the baseline.  Returns None when total
    pooled samples < min_samples (no meaningful baseline yet).

    This gives the natural hit rate for the pattern structure itself,
    independent of which ticker is being evaluated, so the log-ratio
    log(hr / baseline) measures genuine alpha above pattern-type expectation
    rather than against an arbitrary 0.50.

    Future improvements (defer until sample sizes are large enough):
    - TODO(baseline-sector): stratify by (pattern_type, timeframe, sector)
        e.g. FVG 4h in tech vs utilities may have meaningfully different
        natural hit rates.  Only reliable once each sector bucket has ≥100
        pooled samples.
    - TODO(baseline-regime): stratify by market_regime for regime-aware
        baselines once per-regime sample counts are sufficient.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_table(conn)
        rows = conn.execute(
            """SELECT hit_rate_t1, sample_size
               FROM signal_calibration
               WHERE pattern_type=? AND timeframe=?
                 AND hit_rate_t1 IS NOT NULL
                 AND sample_size >= ?""",
            (pattern_type, timeframe, min_samples),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    total_n    = sum(r[1] for r in rows)
    if total_n < min_samples:
        return None

    # Sample-weighted mean
    weighted   = sum(r[0] * r[1] for r in rows)
    return weighted / total_n


# ── get_calibration ───────────────────────────────────────────────────────────

def get_calibration(
    ticker: str,
    pattern_type: str,
    timeframe: str,
    db_path: str,
    market_regime: Optional[str] = None,
) -> Optional[CalibrationResult]:
    """
    Return CalibrationResult for the given key, or None if < 10 samples.
    If market_regime is provided, tries exact match first then falls back
    to regime-agnostic row.
    """
    ticker = ticker.upper()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_table(conn)

        row = None
        if market_regime:
            row = conn.execute(
                """SELECT ticker, pattern_type, timeframe, market_regime, sample_size,
                          hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                          avg_time_to_target_hours, calibration_confidence, last_updated
                   FROM signal_calibration
                   WHERE ticker=? AND pattern_type=? AND timeframe=? AND market_regime=?""",
                (ticker, pattern_type, timeframe, market_regime),
            ).fetchone()

        if row is None:
            row = conn.execute(
                """SELECT ticker, pattern_type, timeframe, market_regime, sample_size,
                          hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                          avg_time_to_target_hours, calibration_confidence, last_updated
                   FROM signal_calibration
                   WHERE ticker=? AND pattern_type=? AND timeframe=?
                   ORDER BY sample_size DESC LIMIT 1""",
                (ticker, pattern_type, timeframe),
            ).fetchone()

        if row is None:
            return None

        sample_size = row[4] or 0
        if sample_size < 10:
            return None

        return CalibrationResult(
            ticker=row[0],
            pattern_type=row[1],
            timeframe=row[2],
            market_regime=row[3],
            sample_size=sample_size,
            hit_rate_t1=row[5],
            hit_rate_t2=row[6],
            hit_rate_t3=row[7],
            stopped_out_rate=row[8],
            avg_time_to_target_hours=row[9],
            calibration_confidence=row[10] or 0.0,
            confidence_label=_confidence_label(sample_size),
            last_updated=row[11],
        )
    finally:
        conn.close()
