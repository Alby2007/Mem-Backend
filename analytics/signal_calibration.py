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


def _has_pg():
    """Lazy check — import each call so load_dotenv() timing doesn't matter."""
    from db import HAS_POSTGRES
    return HAS_POSTGRES


def _pg():
    from db import get_pg
    return get_pg()


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


# ── Schema helper (SQLite only — PG schema managed by scripts/pg_schema.sql) ──

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
        ('direction',            'TEXT'),
    ]:
        if _col not in _existing_cols:
            conn.execute(f'ALTER TABLE signal_calibration ADD COLUMN {_col} {_type}')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_calibration_pattern_tf '
        'ON signal_calibration(pattern_type, timeframe)'
    )
    conn.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_directional '
        'ON signal_calibration(ticker, pattern_type, timeframe, market_regime, direction) '
        'WHERE direction IS NOT NULL'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_sc_direction '
        'ON signal_calibration(pattern_type, direction, timeframe)'
    )
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_observations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            pattern_type  TEXT NOT NULL,
            timeframe     TEXT NOT NULL,
            market_regime TEXT,
            direction     TEXT,
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
    _existing_obs_cols = {
        row[1] for row in conn.execute('PRAGMA table_info(calibration_observations)').fetchall()
    }
    if 'direction' not in _existing_obs_cols:
        conn.execute('ALTER TABLE calibration_observations ADD COLUMN direction TEXT')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_cal_obs_source '
        'ON calibration_observations(source)'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_cal_obs_cell '
        'ON calibration_observations(ticker, pattern_type, timeframe)'
    )
    conn.commit()


# ── Helper: extract row values from PG dict-cursor or SQLite tuple ────────────

def _row_vals(row, *keys):
    """Extract values from a row that may be a dict (PG) or tuple (SQLite)."""
    if isinstance(row, dict):
        return tuple(row[k] for k in keys)
    return tuple(row[i] for i in range(len(keys)))


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
    direction: Optional[str] = None,
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

    if _has_pg():
        try:
            _update_calibration_pg(ticker, pattern_type, timeframe, market_regime,
                                   outcome, now, source, bot_id, pnl_r, direction)
            return
        except Exception as e:
            _log.warning('update_calibration PG failed, falling back to SQLite: %s', e)

    _owns_conn = conn is None
    if _owns_conn:
        conn = sqlite3.connect(db_path, timeout=10)
    try:
        if _owns_conn:
            _ensure_table(conn)
        _update_calibration_sqlite(conn, ticker, pattern_type, timeframe, market_regime,
                                   outcome, now, source, bot_id, pnl_r, direction)
        if _owns_conn:
            conn.commit()
    finally:
        if _owns_conn:
            conn.close()


def _fetch_existing_row(cur, ticker, pattern_type, timeframe, market_regime, direction, ph):
    """Fetch existing calibration row. ph='%s' for PG, '?' for SQLite."""
    _regime_clause = f"(market_regime={ph} OR (market_regime IS NULL AND {ph} IS NULL))"
    if direction is not None:
        cur.execute(
            f"""SELECT sample_size, hit_rate_t1, hit_rate_t2, hit_rate_t3,
                      stopped_out_rate, avg_time_to_target_hours
               FROM signal_calibration
               WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}
                 AND {_regime_clause} AND direction={ph}""",
            (ticker, pattern_type, timeframe, market_regime, market_regime, direction),
        )
    else:
        cur.execute(
            f"""SELECT sample_size, hit_rate_t1, hit_rate_t2, hit_rate_t3,
                      stopped_out_rate, avg_time_to_target_hours
               FROM signal_calibration
               WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}
                 AND {_regime_clause} AND direction IS NULL""",
            (ticker, pattern_type, timeframe, market_regime, market_regime),
        )
    return cur.fetchone()


def _compute_new_rates(row, outcome):
    """Compute incremental mean update from existing row + new outcome."""
    if row:
        if isinstance(row, dict):
            n = row['sample_size'] or 0
            hr1 = row['hit_rate_t1'] or 0.0
            hr2 = row['hit_rate_t2'] or 0.0
            hr3 = row['hit_rate_t3'] or 0.0
            sor = row['stopped_out_rate'] or 0.0
        else:
            n, hr1, hr2, hr3, sor, _atth = row
            n = n or 0; hr1 = hr1 or 0.0; hr2 = hr2 or 0.0
            hr3 = hr3 or 0.0; sor = sor or 0.0
    else:
        n, hr1, hr2, hr3, sor = 0, 0.0, 0.0, 0.0, 0.0
    new_n = n + 1
    new_hr1 = (hr1 * n + (1.0 if outcome in ('hit_t1', 'hit_t2', 'hit_t3') else 0.0)) / new_n
    new_hr2 = (hr2 * n + (1.0 if outcome in ('hit_t2', 'hit_t3') else 0.0)) / new_n
    new_hr3 = (hr3 * n + (1.0 if outcome == 'hit_t3' else 0.0)) / new_n
    new_sor = (sor * n + (1.0 if outcome == 'stopped_out' else 0.0)) / new_n
    return new_n, new_hr1, new_hr2, new_hr3, new_sor


def _update_calibration_pg(ticker, pattern_type, timeframe, market_regime,
                           outcome, now, source, bot_id, pnl_r, direction):
    """PG path for update_calibration."""
    with _pg() as pgconn:
        cur = pgconn.cursor()
        row = _fetch_existing_row(cur, ticker, pattern_type, timeframe,
                                  market_regime, direction, '%s')
        new_n, new_hr1, new_hr2, new_hr3, new_sor = _compute_new_rates(row, outcome)
        conf_score = _confidence_score(new_n)

        # Sector lookup from PG facts
        cur.execute(
            "SELECT object FROM facts WHERE UPPER(subject)=UPPER(%s) AND predicate='sector'"
            " ORDER BY timestamp DESC LIMIT 1", (ticker,))
        _sr = cur.fetchone()
        _sector = _sr['object'] if _sr else None

        _r_mult_update = ''
        if pnl_r is not None:
            _r_mult_update = ', outcome_r_multiple=ROUND((COALESCE(outcome_r_multiple,0)*sample_size + %s) / (sample_size+1), 4)'

        _base_vals = (round(new_hr1, 4), round(new_hr2, 4), round(new_hr3, 4),
                      round(new_sor, 4), round(conf_score, 4), now, _sector)

        if direction is not None:
            cur.execute(
                f"""INSERT INTO signal_calibration
                   (ticker, pattern_type, timeframe, market_regime, direction, sample_size,
                    hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                    calibration_confidence, last_updated, sector)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(ticker, pattern_type, timeframe, market_regime, direction)
                   WHERE direction IS NOT NULL
                   DO UPDATE SET
                     sample_size=EXCLUDED.sample_size,
                     hit_rate_t1=EXCLUDED.hit_rate_t1,
                     hit_rate_t2=EXCLUDED.hit_rate_t2,
                     hit_rate_t3=EXCLUDED.hit_rate_t3,
                     stopped_out_rate=EXCLUDED.stopped_out_rate,
                     calibration_confidence=EXCLUDED.calibration_confidence,
                     last_updated=EXCLUDED.last_updated,
                     sector=COALESCE(signal_calibration.sector, EXCLUDED.sector)
                     {_r_mult_update}""",
                (ticker, pattern_type, timeframe, market_regime, direction, new_n)
                + _base_vals + ((pnl_r,) if pnl_r is not None else ()),
            )
        else:
            cur.execute(
                f"""INSERT INTO signal_calibration
                   (ticker, pattern_type, timeframe, market_regime, sample_size,
                    hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                    calibration_confidence, last_updated, sector)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(ticker, pattern_type, timeframe, market_regime)
                   DO UPDATE SET
                     sample_size=EXCLUDED.sample_size,
                     hit_rate_t1=EXCLUDED.hit_rate_t1,
                     hit_rate_t2=EXCLUDED.hit_rate_t2,
                     hit_rate_t3=EXCLUDED.hit_rate_t3,
                     stopped_out_rate=EXCLUDED.stopped_out_rate,
                     calibration_confidence=EXCLUDED.calibration_confidence,
                     last_updated=EXCLUDED.last_updated,
                     sector=COALESCE(signal_calibration.sector, EXCLUDED.sector)
                     {_r_mult_update}""",
                (ticker, pattern_type, timeframe, market_regime, new_n)
                + _base_vals + ((pnl_r,) if pnl_r is not None else ()),
            )
        # Observation counter
        _regime_clause = "(market_regime=%s OR (market_regime IS NULL AND %s IS NULL))"
        if source == 'paper_bot':
            cur.execute(
                f"""UPDATE signal_calibration
                   SET bot_observations = COALESCE(bot_observations, 0) + 1
                   WHERE ticker=%s AND pattern_type=%s AND timeframe=%s
                     AND {_regime_clause}""",
                (ticker, pattern_type, timeframe, market_regime, market_regime),
            )
        elif source in ('user', 'user_feedback'):
            cur.execute(
                f"""UPDATE signal_calibration
                   SET user_observations = COALESCE(user_observations, 0) + 1
                   WHERE ticker=%s AND pattern_type=%s AND timeframe=%s
                     AND {_regime_clause}""",
                (ticker, pattern_type, timeframe, market_regime, market_regime),
            )
        # Observation log
        try:
            cur.execute(
                """INSERT INTO calibration_observations
                   (ticker, pattern_type, timeframe, market_regime, direction,
                    outcome, source, bot_id, pnl_r, observed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (ticker, pattern_type, timeframe, market_regime, direction,
                 outcome, source, bot_id, pnl_r, now),
            )
            if outcome in ('hit_t2', 'hit_t3'):
                cur.execute(
                    """INSERT INTO calibration_observations
                       (ticker, pattern_type, timeframe, market_regime, direction,
                        outcome, source, bot_id, pnl_r, observed_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (ticker, pattern_type, timeframe, market_regime, direction,
                     'hit_t1', source, bot_id, None, now),
                )
        except Exception:
            pass


def _update_calibration_sqlite(conn, ticker, pattern_type, timeframe, market_regime,
                               outcome, now, source, bot_id, pnl_r, direction):
    """SQLite path for update_calibration."""
    row = _fetch_existing_row(conn, ticker, pattern_type, timeframe,
                              market_regime, direction, '?')
    new_n, new_hr1, new_hr2, new_hr3, new_sor = _compute_new_rates(row, outcome)
    conf_score = _confidence_score(new_n)

    _sector_row = conn.execute(
        "SELECT object FROM facts WHERE UPPER(subject)=? AND predicate='sector'"
        " ORDER BY timestamp DESC LIMIT 1", (ticker,)).fetchone()
    _sector = _sector_row[0] if _sector_row else None

    _r_mult_update = ''
    if pnl_r is not None:
        _r_mult_update = ', outcome_r_multiple=ROUND((COALESCE(outcome_r_multiple,0)*sample_size + ?) / (sample_size+1), 4)'

    if direction is not None:
        conn.execute(
            f"""INSERT INTO signal_calibration
               (ticker, pattern_type, timeframe, market_regime, direction, sample_size,
                hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                calibration_confidence, last_updated, sector)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(ticker, pattern_type, timeframe, market_regime, direction)
               WHERE direction IS NOT NULL
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
            (ticker, pattern_type, timeframe, market_regime, direction, new_n,
             round(new_hr1, 4), round(new_hr2, 4), round(new_hr3, 4),
             round(new_sor, 4), round(conf_score, 4), now, _sector)
            + ((pnl_r,) if pnl_r is not None else ()),
        )
    else:
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
    try:
        conn.execute(
            """INSERT INTO calibration_observations
               (ticker, pattern_type, timeframe, market_regime, direction,
                outcome, source, bot_id, pnl_r, observed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (ticker, pattern_type, timeframe, market_regime, direction,
             outcome, source, bot_id, pnl_r, now),
        )
        if outcome in ('hit_t2', 'hit_t3'):
            conn.execute(
                """INSERT INTO calibration_observations
                   (ticker, pattern_type, timeframe, market_regime, direction,
                    outcome, source, bot_id, pnl_r, observed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ticker, pattern_type, timeframe, market_regime, direction,
                 'hit_t1', source, bot_id, None, now),
            )
    except Exception:
        pass


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
    rows = None
    if _has_pg():
        try:
            with _pg() as pgconn:
                cur = pgconn.cursor()
                cur.execute(
                    """SELECT hit_rate_t1, sample_size
                       FROM signal_calibration
                       WHERE pattern_type=%s AND timeframe=%s
                         AND hit_rate_t1 IS NOT NULL
                         AND sample_size >= %s""",
                    (pattern_type, timeframe, min_samples),
                )
                rows = [(r['hit_rate_t1'], r['sample_size']) for r in cur.fetchall()]
        except Exception as e:
            _log.warning('get_global_baseline PG failed: %s', e)
            rows = None
    if rows is None:
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

def _get_user_obs_count_pg(cur, ticker, pattern_type, timeframe, market_regime):
    """Return user_observations count via PG cursor."""
    cur.execute(
        """SELECT COALESCE(user_observations, 0) AS cnt
           FROM signal_calibration
           WHERE ticker=%s AND pattern_type=%s AND timeframe=%s
             AND (market_regime=%s OR (market_regime IS NULL AND %s IS NULL))""",
        (ticker, pattern_type, timeframe, market_regime, market_regime),
    )
    row = cur.fetchone()
    return row['cnt'] if row else 0


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


def _calibration_tiered_query(cur, ticker, pattern_type, timeframe, market_regime, direction, ph):
    """Run the 4-tier fallback query. Returns a row or None."""
    _sel = ("SELECT ticker, pattern_type, timeframe, market_regime, sample_size,"
            "       hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,"
            "       avg_time_to_target_hours, calibration_confidence, last_updated"
            " FROM signal_calibration")
    row = None
    if market_regime and direction:
        cur.execute(
            f"{_sel} WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}"
            f" AND market_regime={ph} AND direction={ph}",
            (ticker, pattern_type, timeframe, market_regime, direction))
        row = cur.fetchone()
    if row is None and market_regime:
        cur.execute(
            f"{_sel} WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}"
            f" AND market_regime={ph} AND direction IS NULL",
            (ticker, pattern_type, timeframe, market_regime))
        row = cur.fetchone()
    if row is None and direction:
        cur.execute(
            f"{_sel} WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}"
            f" AND market_regime IS NULL AND direction={ph}",
            (ticker, pattern_type, timeframe, direction))
        row = cur.fetchone()
    if row is None:
        cur.execute(
            f"{_sel} WHERE ticker={ph} AND pattern_type={ph} AND timeframe={ph}"
            " AND direction IS NULL ORDER BY sample_size DESC LIMIT 1",
            (ticker, pattern_type, timeframe))
        row = cur.fetchone()
    return row


def _row_to_calibration_result(row, is_dict=False):
    """Build CalibrationResult from PG dict-row or SQLite tuple-row."""
    if is_dict:
        ss = row['sample_size'] or 0
        return CalibrationResult(
            ticker=row['ticker'], pattern_type=row['pattern_type'],
            timeframe=row['timeframe'], market_regime=row['market_regime'],
            sample_size=ss, hit_rate_t1=row['hit_rate_t1'],
            hit_rate_t2=row['hit_rate_t2'], hit_rate_t3=row['hit_rate_t3'],
            stopped_out_rate=row['stopped_out_rate'],
            avg_time_to_target_hours=row['avg_time_to_target_hours'],
            calibration_confidence=row['calibration_confidence'] or 0.0,
            confidence_label=_confidence_label(ss),
            last_updated=row['last_updated'],
        )
    ss = row[4] or 0
    return CalibrationResult(
        ticker=row[0], pattern_type=row[1], timeframe=row[2],
        market_regime=row[3], sample_size=ss, hit_rate_t1=row[5],
        hit_rate_t2=row[6], hit_rate_t3=row[7], stopped_out_rate=row[8],
        avg_time_to_target_hours=row[9],
        calibration_confidence=row[10] or 0.0,
        confidence_label=_confidence_label(ss), last_updated=row[11],
    )


def get_calibration(
    ticker: str,
    pattern_type: str,
    timeframe: str,
    db_path: str,
    market_regime: Optional[str] = None,
    corrected: bool = True,
    direction: Optional[str] = None,
) -> Optional[CalibrationResult]:
    """
    Return CalibrationResult for the given key, or None if < 10 samples.

    Fallback hierarchy (most specific → least specific):
      1. (ticker, pattern, tf, regime,  direction)   — most precise
      2. (ticker, pattern, tf, regime,  NULL)         — regime-aware, undirected
      3. (ticker, pattern, tf, NULL,    direction)    — direction-aware, regime-agnostic
      4. (ticker, pattern, tf, NULL,    NULL)         — historical baseline

    When corrected=True and the cell has no user observations, applies the
    global correction factor from calibration_correction to adjust for
    paper-trading optimism.
    """
    ticker = ticker.upper()

    if _has_pg():
        try:
            with _pg() as pgconn:
                cur = pgconn.cursor()
                row = _calibration_tiered_query(cur, ticker, pattern_type, timeframe,
                                                market_regime, direction, '%s')
                if row is None:
                    return None
                ss = row['sample_size'] or 0
                if ss < 10:
                    return None
                result = _row_to_calibration_result(row, is_dict=True)
                if corrected and ss >= 10:
                    user_obs = _get_user_obs_count_pg(
                        cur, row['ticker'], row['pattern_type'],
                        row['timeframe'], row['market_regime'])
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
        except Exception as e:
            _log.warning('get_calibration PG failed: %s', e)

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_table(conn)
        row = _calibration_tiered_query(conn, ticker, pattern_type, timeframe,
                                        market_regime, direction, '?')
        if row is None:
            return None
        sample_size = row[4] or 0
        if sample_size < 10:
            return None
        result = _row_to_calibration_result(row, is_dict=False)
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
    direction: Optional[str] = None,
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
    if _has_pg():
        try:
            with _pg() as pgconn:
                cur = pgconn.cursor()
                _dir_clause = 'AND direction=%s' if direction else 'AND (direction IS NULL OR direction=direction)'
                _dir_params = (direction,) if direction else ()
                cur.execute(
                    f"""SELECT AVG(hit_rate_t1) as avg_t1,
                              MAX(hit_rate_t1) as max_t1,
                              COUNT(*) as proven_cells,
                              SUM(sample_size) as total_samples
                       FROM signal_calibration
                       WHERE pattern_type=%s AND timeframe=%s AND sample_size >= 20
                         AND hit_rate_t1 IS NOT NULL
                         {_dir_clause}""",
                    (pattern_type, timeframe) + _dir_params,
                )
                row = cur.fetchone()
                if not row or not row['avg_t1'] or (row['proven_cells'] or 0) < min_proven_cells:
                    return None
                return round(float(row['avg_t1']), 4)
        except Exception as e:
            _log.warning('get_pattern_baseline PG failed: %s', e)

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _dir_clause = 'AND direction=?' if direction else 'AND (direction IS NULL OR direction=direction)'
        _dir_params = (direction,) if direction else ()
        row = conn.execute(
            f"""SELECT AVG(hit_rate_t1) as avg_t1,
                      MAX(hit_rate_t1) as max_t1,
                      COUNT(*) as proven_cells,
                      SUM(sample_size) as total_samples
               FROM signal_calibration
               WHERE pattern_type=? AND timeframe=? AND sample_size >= 20
                 AND hit_rate_t1 IS NOT NULL
                 {_dir_clause}""",
            (pattern_type, timeframe) + _dir_params,
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
    direction: Optional[str] = None,
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

    rows = None
    if _has_pg():
        try:
            with _pg() as pgconn:
                cur = pgconn.cursor()
                _dir_clause = 'AND direction=%s' if direction else 'AND (direction IS NULL OR direction=direction)'
                _dir_params = (direction,) if direction else ()
                cur.execute(
                    f"""SELECT hit_rate_t1, sample_size
                       FROM signal_calibration
                       WHERE pattern_type=%s AND timeframe=%s AND market_regime=%s
                         AND hit_rate_t1 IS NOT NULL AND sample_size >= 20
                         {_dir_clause}""",
                    (pattern_type, timeframe, market_regime) + _dir_params,
                )
                rows = [(r['hit_rate_t1'], r['sample_size']) for r in cur.fetchall()]
        except Exception as e:
            _log.warning('get_regime_aware_baseline PG failed: %s', e)
            rows = None

    if rows is None:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            _dir_clause = 'AND direction=?' if direction else 'AND (direction IS NULL OR direction=direction)'
            _dir_params = (direction,) if direction else ()
            rows = conn.execute(
                f"""SELECT hit_rate_t1, sample_size
                   FROM signal_calibration
                   WHERE pattern_type=? AND timeframe=? AND market_regime=?
                     AND hit_rate_t1 IS NOT NULL AND sample_size >= 20
                     {_dir_clause}""",
                (pattern_type, timeframe, market_regime) + _dir_params,
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
    rows = None
    if _has_pg():
        try:
            with _pg() as pgconn:
                cur = pgconn.cursor()
                cur.execute(
                    """SELECT hit_rate_t1, sample_size
                       FROM signal_calibration
                       WHERE pattern_type=%s AND timeframe=%s
                         AND hit_rate_t1 IS NOT NULL
                         AND sample_size >= %s""",
                    (pattern_type, timeframe, min_samples),
                )
                rows = [(r['hit_rate_t1'], r['sample_size']) for r in cur.fetchall()]
        except Exception as e:
            _log.warning('get_global_baseline_with_n PG failed: %s', e)
            rows = None

    if rows is None:
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
