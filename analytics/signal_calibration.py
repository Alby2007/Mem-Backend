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
    # S1: State-vector columns for Historical State Matching (idempotent)
    # Check existing columns once via PRAGMA to avoid 5 exception-per-call overhead
    _existing_cols = {
        row[1] for row in conn.execute('PRAGMA table_info(signal_calibration)').fetchall()
    }
    for _col, _type in [
        ('volatility_regime',    'TEXT'),
        ('sector',               'TEXT'),
        ('central_bank_stance',  'TEXT'),
        ('gdelt_tension_level',  'TEXT'),
        ('outcome_r_multiple',   'REAL'),
        ('bot_observations',     'INTEGER DEFAULT 0'),
        ('user_observations',    'INTEGER DEFAULT 0'),
    ]:
        if _col not in _existing_cols:
            conn.execute(f'ALTER TABLE signal_calibration ADD COLUMN {_col} {_type}')
    # Performance index for state-matching scan
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_calibration_pattern_tf '
        'ON signal_calibration(pattern_type, timeframe)'
    )
    # Observation-level log for source tracking, correction factor, and auditing
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_observations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            pattern_type  TEXT NOT NULL,
            timeframe     TEXT NOT NULL,
            market_regime TEXT,
            outcome       TEXT NOT NULL,
            source        TEXT NOT NULL DEFAULT 'user',
            bot_id        TEXT,
            pnl_r         REAL,
            entry_price   REAL,
            exit_price    REAL,
            holding_hours REAL,
            observed_at   TEXT NOT NULL
        )
    """)
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_cal_obs_source '
        'ON calibration_observations(source)'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_cal_obs_cell '
        'ON calibration_observations(ticker, pattern_type, timeframe)'
    )
    conn.commit()


# ── update_calibration ────────────────────────────────────────────────────────

def update_calibration(
    ticker: str,
    pattern_type: str,
    timeframe: str,
    market_regime: Optional[str],
    outcome: str,
    db_path: str,
    source: str = 'user',
    bot_id: Optional[str] = None,
    conn=None,
    pnl_r: Optional[float] = None,
) -> None:
    """
    Update the calibration row for (ticker, pattern_type, timeframe, market_regime)
    based on one new outcome from POST /feedback or a position close.

    outcome values: 'hit_t1' | 'hit_t2' | 'hit_t3' | 'stopped_out' | 'pending' | 'skipped'
    Only hit_t* and stopped_out are counted (pending/skipped ignored).
    source: 'user' | 'paper_bot' | 'system' | 'backtest'
    pnl_r: actual R-multiple realised (optional — written to calibration_observations
           and used to update outcome_r_multiple on the calibration row)
    """
    if outcome not in ('hit_t1', 'hit_t2', 'hit_t3', 'stopped_out'):
        return

    now = datetime.now(timezone.utc).isoformat()
    ticker = ticker.upper()
    _owns_conn = conn is None
    if _owns_conn:
        conn = sqlite3.connect(db_path, timeout=10)
    try:
        if _owns_conn:
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
        # T1 counts if T1, T2, or T3 was hit (T2 hit implies T1 was hit first).
        # T2 counts if T2 or T3 was hit (T3 hit implies T2 was hit first).
        new_n = n + 1
        new_hr1 = (hr1 * n + (1.0 if outcome in ('hit_t1', 'hit_t2', 'hit_t3') else 0.0)) / new_n
        new_hr2 = (hr2 * n + (1.0 if outcome in ('hit_t2', 'hit_t3') else 0.0)) / new_n
        new_hr3 = (hr3 * n + (1.0 if outcome == 'hit_t3' else 0.0)) / new_n
        new_sor = (sor * n + (1.0 if outcome == 'stopped_out' else 0.0)) / new_n

        conf_score = _confidence_score(new_n)

        # Lookup sector for this ticker (used on INSERT; existing rows retain theirs)
        _sector_row = conn.execute(
            "SELECT object FROM facts WHERE UPPER(subject)=? AND predicate='sector'"
            " ORDER BY timestamp DESC LIMIT 1",
            (ticker,),
        ).fetchone()
        _sector = _sector_row[0] if _sector_row else None

        # Compute updated outcome_r_multiple if pnl_r provided
        _r_mult_update = ''
        if pnl_r is not None:
            _r_mult_update = ', outcome_r_multiple=ROUND((COALESCE(outcome_r_multiple,0)*sample_size + ?) / (sample_size+1), 4)'

        conn.execute(
            f"""INSERT INTO signal_calibration
               (ticker, pattern_type, timeframe, market_regime, sample_size,
                hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                calibration_confidence, last_updated, sector)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(ticker, pattern_type, timeframe, market_regime)
               DO UPDATE SET
                 sample_size=excluded.sample_size,
                 hit_rate_t1=excluded.hit_rate_t1,
                 hit_rate_t2=excluded.hit_rate_t2,
                 hit_rate_t3=excluded.hit_rate_t3,
                 stopped_out_rate=excluded.stopped_out_rate,
                 calibration_confidence=excluded.calibration_confidence,
                 last_updated=excluded.last_updated,
                 sector=COALESCE(signal_calibration.sector, excluded.sector)
                 {_r_mult_update}""",
            (ticker, pattern_type, timeframe, market_regime, new_n,
             round(new_hr1, 4), round(new_hr2, 4), round(new_hr3, 4),
             round(new_sor, 4), round(conf_score, 4), now, _sector)
            + ((pnl_r,) if pnl_r is not None else ()),
        )
        # Increment per-source observation counter on the calibration row
        if source == 'paper_bot':
            conn.execute(
                """UPDATE signal_calibration
                   SET bot_observations = COALESCE(bot_observations, 0) + 1
                   WHERE ticker=? AND pattern_type=? AND timeframe=?
                     AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
                (ticker, pattern_type, timeframe, market_regime, market_regime),
            )
        elif source in ('user', 'user_feedback'):
            conn.execute(
                """UPDATE signal_calibration
                   SET user_observations = COALESCE(user_observations, 0) + 1
                   WHERE ticker=? AND pattern_type=? AND timeframe=?
                     AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
                (ticker, pattern_type, timeframe, market_regime, market_regime),
            )
        # Write observation-level log — never let this break the main path
        try:
            conn.execute(
                """INSERT INTO calibration_observations
                   (ticker, pattern_type, timeframe, market_regime,
                    outcome, source, bot_id, pnl_r, observed_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (ticker, pattern_type, timeframe, market_regime,
                 outcome, source, bot_id, pnl_r, now),
            )
            # Cascade: T2/T3 hit implies T1 was also hit — insert synthetic record
            # so the observation log is consistent with the incremental mean update.
            if outcome in ('hit_t2', 'hit_t3'):
                conn.execute(
                    """INSERT INTO calibration_observations
                       (ticker, pattern_type, timeframe, market_regime,
                        outcome, source, bot_id, pnl_r, observed_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (ticker, pattern_type, timeframe, market_regime,
                     'hit_t1', source, bot_id, None, now),
                )
        except Exception:
            pass
        if _owns_conn:
            conn.commit()
    finally:
        if _owns_conn:
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

def _get_user_obs_count(
    conn: sqlite3.Connection,
    ticker: str,
    pattern_type: str,
    timeframe: str,
    market_regime: Optional[str],
) -> int:
    """Return user_observations count for a calibration cell."""
    row = conn.execute(
        """SELECT COALESCE(user_observations, 0)
           FROM signal_calibration
           WHERE ticker=? AND pattern_type=? AND timeframe=?
             AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
        (ticker, pattern_type, timeframe, market_regime, market_regime),
    ).fetchone()
    return row[0] if row else 0


def get_calibration(
    ticker: str,
    pattern_type: str,
    timeframe: str,
    db_path: str,
    market_regime: Optional[str] = None,
    corrected: bool = True,
) -> Optional[CalibrationResult]:
    """
    Return CalibrationResult for the given key, or None if < 10 samples.
    If market_regime is provided, tries exact match first then falls back
    to regime-agnostic row.
    When corrected=True and the cell has no user observations, applies the
    global correction factor from calibration_correction to adjust for
    paper-trading optimism.
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

        result = CalibrationResult(
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
        if corrected and sample_size >= 10:
            user_obs = _get_user_obs_count(conn, row[0], row[1], row[2], row[3])
            if user_obs == 0:
                try:
                    from analytics.calibration_correction import get_global_correction
                    factor = get_global_correction(db_path)
                    if factor != 1.0:
                        if result.hit_rate_t1 is not None:
                            result.hit_rate_t1 = round(result.hit_rate_t1 * factor, 4)
                        if result.hit_rate_t2 is not None:
                            result.hit_rate_t2 = round(result.hit_rate_t2 * factor, 4)
                        result.confidence_label += '_bot_corrected'
                except Exception:
                    pass
        return result
    finally:
        conn.close()


def get_pattern_baseline(
    pattern_type: str,
    timeframe: str,
    db_path: str,
    min_proven_cells: int = 10,
) -> Optional[float]:
    """
    Return the aggregate T1 hit rate across all proven cells for a given
    pattern_type + timeframe. Used as a fallback probability when no
    ticker-specific calibration exists, and as a ceiling cap on individual
    ticker calibration to prevent overconfidence.

    Only cells with sample_size >= 20 (proven) contribute.
    Returns None if fewer than min_proven_cells proven cells exist.

    Typical values (from 5M+ historical samples):
      breaker        4h → 27.7%
      mitigation     4h → 34.6%
      ifvg           4h → 25.4%
      liquidity_void 4h → 42.7%
      order_block    4h → 23.0%
      fvg            4h → 11.6%
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        row = conn.execute(
            """SELECT AVG(hit_rate_t1) as avg_t1,
                      MAX(hit_rate_t1) as max_t1,
                      COUNT(*) as proven_cells,
                      SUM(sample_size) as total_samples
               FROM signal_calibration
               WHERE pattern_type=? AND timeframe=? AND sample_size >= 20
                 AND hit_rate_t1 IS NOT NULL""",
            (pattern_type, timeframe),
        ).fetchone()

        if not row or not row[0] or (row[2] or 0) < min_proven_cells:
            return None

        return round(float(row[0]), 4)
    except Exception:
        return None
    finally:
        conn.close()


def get_regime_aware_baseline(
    pattern_type: str,
    timeframe: str,
    market_regime: str,
    db_path: str,
    min_cells: int = 5,
    min_samples: int = 1000,
) -> Optional[float]:
    """
    Return the sample-weighted mean hit_rate_t1 for a given
    (pattern_type, timeframe, market_regime) combination.

    Implements a two-level fallback:
    1. Exact regime match — most accurate, regime-aware probability
    2. Returns None if insufficient data — caller falls back to overall baseline

    This replaces the TODO(baseline-regime) in get_pattern_baseline.
    With 5M+ samples across regimes, per-regime baselines are now reliable.

    Typical regime-specific values vs overall baseline (1h):
      breaker  risk_off_contraction: ~30%   vs 35.2% overall
      breaker  risk_on_expansion:    ~58%   vs 35.2% overall  (small sample)
      mitigation risk_off_contraction: ~44% vs 38.4% overall
      liquidity_void risk_off_contraction: ~51% vs 46.1% overall
    """
    if not market_regime:
        return None
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        rows = conn.execute(
            """SELECT hit_rate_t1, sample_size
               FROM signal_calibration
               WHERE pattern_type=? AND timeframe=? AND market_regime=?
                 AND hit_rate_t1 IS NOT NULL AND sample_size >= 20""",
            (pattern_type, timeframe, market_regime),
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < min_cells:
        return None

    total_n  = sum(r[1] for r in rows)
    if total_n < min_samples:
        return None

    weighted = sum(r[0] * r[1] for r in rows)
    return weighted / total_n


def get_global_baseline_with_n(
    pattern_type: str,
    timeframe: str,
    db_path: str,
    min_samples: int = 30,
) -> tuple:
    """
    Same as get_global_baseline but returns (hit_rate, total_n) tuple so
    callers can scale their Bayesian prior proportionally to baseline quality.

    Used by signal_enrichment_adapter for TODO(prior-scaling).
    Returns (None, 0) when insufficient data.
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
        return (None, 0)

    total_n  = sum(r[1] for r in rows)
    if total_n < min_samples:
        return (None, 0)

    weighted = sum(r[0] * r[1] for r in rows)
    return (weighted / total_n, total_n)
