"""
analytics/prediction_ledger.py — Prediction Ledger with Brier Scoring

Records every tip issued as an explicit prediction with stated probabilities.
Scores outcomes automatically when prices are updated intraday. Computes
Brier scores and calibration curves for the public GET /ledger/performance
endpoint.

DESIGN
======
System-level dedup: one row per (ticker, pattern_type, date(issued_at)).
The same signal issued to multiple users on the same day creates one ledger
entry. This makes the public performance record a clean count of distinct
signals, not inflated by user count.

INTRADAY RESOLUTION
===================
on_price_written(ticker, price) is called by KnowledgeGraph.add_fact()
every time a last_price atom is written (every ~5 minutes from YFinanceAdapter).
This checks all open predictions for that ticker and resolves immediately
if the price has crossed T1, T2, or stop. Predictions that never resolve
are auto-expired after 20 trading days via expire_stale_predictions()
(run daily by the ingest scheduler).

BRIER SCORING
=============
Brier score = mean((p_stated - outcome_binary)²) over all resolved rows.
  Perfect calibration = 0.0
  Random guessing     = 0.25
  Worse than random   = > 0.25

brier_t1 per row = (p_hit_t1 - hit_t1_binary)²

CALIBRATION CURVE
=================
Groups resolved rows into 10 probability buckets (0–10%, 10–20%, …, 90–100%).
For each bucket: stated = bucket midpoint, actual = empirical hit rate.
A perfectly calibrated system has stated ≈ actual in every bucket.

SCHEMA
======
prediction_ledger table — see _CREATE_TABLE below.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS prediction_ledger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    pattern_type    TEXT    NOT NULL,
    timeframe       TEXT,
    entry_price     REAL,
    target_1        REAL,
    target_2        REAL,
    stop_loss       REAL,
    p_hit_t1        REAL,
    p_hit_t2        REAL,
    p_stopped_out   REAL,
    market_regime   TEXT,
    conviction_tier TEXT,
    issued_at       TEXT    NOT NULL,
    expires_at      TEXT,
    outcome         TEXT,
    resolved_at     TEXT,
    resolved_price  REAL,
    brier_t1        REAL,
    source          TEXT    DEFAULT 'system'
)
"""

# SQLite does not allow expressions in inline UNIQUE constraints.
# The system-level dedup (one row per ticker+pattern_type per calendar day)
# is enforced by this partial unique index on the date() of issued_at.
_CREATE_DEDUP_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_daily_dedup
ON prediction_ledger(ticker, pattern_type, date(issued_at))
"""

# ── Trading day helpers ────────────────────────────────────────────────────────

_TRADING_DAYS_PER_WEEK = 5
_CALENDAR_DAYS_PER_TRADING_DAY = 7.0 / 5.0   # ~1.4 calendar days per trading day
_EXPIRY_TRADING_DAYS = 20


def _add_trading_days(dt: datetime, n: int) -> datetime:
    """Approximate n trading days by adding 1.4× calendar days per trading day."""
    delta = timedelta(days=int(n * _CALENDAR_DAYS_PER_TRADING_DAY + 0.5))
    return dt + delta


# ── PredictionLedger ───────────────────────────────────────────────────────────

class PredictionLedger:
    """
    Records, resolves, and scores system-level signal predictions.

    Lifecycle
    ---------
    1. TipScheduler calls record_prediction() after a tip is sent.
    2. KnowledgeGraph calls on_price_written() every ~5 min per ticker.
    3. Ingest scheduler calls expire_stale_predictions() once daily.
    4. GET /ledger/performance calls get_performance_report().
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path
        self._ensure_table()

    # ── Schema init ──────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        conn = self._connect()
        try:
            conn.execute(_CREATE_TABLE)
            conn.execute(_CREATE_DEDUP_INDEX)
            # Idempotent: add source column if missing (existing DBs)
            _cols = {r[1] for r in conn.execute('PRAGMA table_info(prediction_ledger)').fetchall()}
            if 'source' not in _cols:
                conn.execute("ALTER TABLE prediction_ledger ADD COLUMN source TEXT DEFAULT 'system'")
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db, timeout=10)

    # ── Record ───────────────────────────────────────────────────────────────

    def record_prediction(
        self,
        ticker:         str,
        pattern_type:   str,
        timeframe:      str,
        entry_price:    float,
        target_1:       float,
        target_2:       float,
        stop_loss:      float,
        p_hit_t1:       float,
        p_hit_t2:       float,
        p_stopped_out:  float,
        market_regime:  Optional[str],
        conviction_tier: Optional[str],
        source:         str = 'system',
        conn=None,
    ) -> bool:
        """
        Insert a new prediction row. Returns True if inserted, False if the
        UNIQUE constraint fires (same ticker+pattern_type on same calendar day).
        """
        now       = datetime.now(timezone.utc)
        issued_at = now.isoformat()
        expires_at = _add_trading_days(now, _EXPIRY_TRADING_DAYS).isoformat()

        _own_conn = conn is None
        _conn = self._connect() if _own_conn else conn
        try:
            _conn.execute(
                """INSERT OR IGNORE INTO prediction_ledger
                   (ticker, pattern_type, timeframe, entry_price,
                    target_1, target_2, stop_loss,
                    p_hit_t1, p_hit_t2, p_stopped_out,
                    market_regime, conviction_tier,
                    issued_at, expires_at, source)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ticker.upper(), pattern_type, timeframe, entry_price,
                 target_1, target_2, stop_loss,
                 round(p_hit_t1, 4), round(p_hit_t2, 4), round(p_stopped_out, 4),
                 market_regime, conviction_tier,
                 issued_at, expires_at, source),
            )
            inserted = _conn.total_changes > 0
            if _own_conn:
                _conn.commit()
            if inserted:
                _log.info(
                    'PredictionLedger: recorded %s %s %s p_t1=%.2f p_t2=%.2f',
                    ticker.upper(), pattern_type, timeframe, p_hit_t1, p_hit_t2,
                )
            return inserted
        finally:
            if _own_conn:
                _conn.close()

    # ── Intraday resolution hook ─────────────────────────────────────────────

    def on_price_written(self, ticker: str, current_price: float) -> None:
        """
        Called by KnowledgeGraph.add_fact() when a last_price atom is written.
        Resolves any open predictions for this ticker if price has crossed
        T1, T2, or stop.

        This is the primary resolution path — predictions can resolve within
        5 minutes of the target being hit.
        """
        if current_price <= 0:
            return

        ticker_up = ticker.upper()
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT id, ticker, entry_price, target_1, target_2,
                          stop_loss, p_hit_t1
                   FROM prediction_ledger
                   WHERE ticker=? AND outcome IS NULL""",
                (ticker_up,),
            ).fetchall()
        finally:
            conn.close()

        for row in rows:
            pred_id, _ticker, entry, t1, t2, stop, p_t1 = row
            outcome = self._classify_outcome(
                current_price, entry, t1, t2, stop
            )
            if outcome:
                self._resolve(pred_id, outcome, current_price, p_t1)

    def _classify_outcome(
        self,
        price:  float,
        entry:  float,
        t1:     Optional[float],
        t2:     Optional[float],
        stop:   Optional[float],
    ) -> Optional[str]:
        """
        Returns outcome string if price has crossed a target, else None.
        Checks T2 first (implies T1 was also hit), then T1, then stop.
        Direction inferred from entry vs t1 (bullish if t1 > entry).
        """
        if t1 is None:
            return None

        bullish = t1 > entry if entry and t1 else True

        if t2 is not None:
            if bullish and price >= t2:
                return 'hit_t2'
            if not bullish and price <= t2:
                return 'hit_t2'

        if bullish and price >= t1:
            return 'hit_t1'
        if not bullish and price <= t1:
            return 'hit_t1'

        if stop is not None:
            if bullish and price <= stop:
                return 'stopped_out'
            if not bullish and price >= stop:
                return 'stopped_out'

        return None

    def _resolve(
        self,
        pred_id:       int,
        outcome:       str,
        resolved_price: float,
        p_hit_t1:      float,
    ) -> None:
        """Write outcome + Brier contribution to the ledger row."""
        now = datetime.now(timezone.utc).isoformat()
        hit_t1_binary = 1.0 if outcome in ('hit_t1', 'hit_t2', 'hit_t3') else 0.0
        brier_t1 = round((p_hit_t1 - hit_t1_binary) ** 2, 6)

        conn = self._connect()
        try:
            conn.execute(
                """UPDATE prediction_ledger
                   SET outcome=?, resolved_at=?, resolved_price=?, brier_t1=?
                   WHERE id=?""",
                (outcome, now, resolved_price, brier_t1, pred_id),
            )
            conn.commit()
            _log.info(
                'PredictionLedger: resolved id=%d outcome=%s price=%.4f brier=%.4f',
                pred_id, outcome, resolved_price, brier_t1,
            )
        finally:
            conn.close()

    # ── Daily expiry job ─────────────────────────────────────────────────────

    def expire_stale_predictions(self) -> int:
        """
        Mark open predictions past their expires_at as 'expired'.
        Called once daily by the ingest scheduler.
        Returns count of rows expired.
        """
        now = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """UPDATE prediction_ledger
                   SET outcome='expired', resolved_at=?
                   WHERE outcome IS NULL AND expires_at < ?""",
                (now, now),
            )
            expired = conn.total_changes
            conn.commit()
            if expired:
                _log.info('PredictionLedger: expired %d stale predictions', expired)
            return expired
        finally:
            conn.close()

    # ── Performance report ────────────────────────────────────────────────────

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Build the full performance report for GET /ledger/performance.
        Returns a dict suitable for direct jsonify().
        """
        conn = self._connect()
        try:
            total    = conn.execute(
                "SELECT COUNT(*) FROM prediction_ledger"
            ).fetchone()[0]

            resolved = conn.execute(
                "SELECT COUNT(*) FROM prediction_ledger WHERE outcome IS NOT NULL"
            ).fetchone()[0]

            open_    = total - resolved

            # Brier score over all resolved rows with brier_t1 computed
            brier_rows = conn.execute(
                "SELECT brier_t1 FROM prediction_ledger WHERE brier_t1 IS NOT NULL"
            ).fetchall()
            brier_score = None
            if brier_rows:
                brier_score = round(sum(r[0] for r in brier_rows) / len(brier_rows), 4)

            regime_breakdown = self._regime_breakdown(conn)
            calibration_curve = self._calibration_curve(conn)

            return {
                'total_predictions': total,
                'resolved':          resolved,
                'open':              open_,
                'brier_score':       brier_score,
                'vs_benchmark':      round((brier_score - 0.25), 4) if brier_score is not None else None,
                'note':              (
                    'vs_benchmark < 0 means better than random guessing (0.25). '
                    'Perfect calibration = 0.0.'
                ),
                'regime_breakdown':  regime_breakdown,
                'calibration_curve': calibration_curve,
            }
        finally:
            conn.close()

    def _regime_breakdown(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Brier score per market regime."""
        rows = conn.execute(
            """SELECT market_regime, COUNT(*), AVG(brier_t1)
               FROM prediction_ledger
               WHERE brier_t1 IS NOT NULL
               GROUP BY market_regime""",
        ).fetchall()
        result: Dict[str, Any] = {}
        for regime, n, avg_brier in rows:
            key = regime if regime else 'unknown'
            result[key] = {
                'brier': round(avg_brier, 4) if avg_brier is not None else None,
                'n':     n,
            }
        return result

    def _calibration_curve(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """
        Groups resolved predictions into 10 probability buckets.
        For each bucket: stated probability midpoint vs actual empirical hit rate.
        """
        rows = conn.execute(
            """SELECT p_hit_t1, outcome
               FROM prediction_ledger
               WHERE outcome IS NOT NULL AND p_hit_t1 IS NOT NULL""",
        ).fetchall()

        # Buckets: 0–0.1, 0.1–0.2, …, 0.9–1.0
        buckets: Dict[int, Dict[str, Any]] = {
            i: {'stated_mid': round(i * 0.1 + 0.05, 2), 'hits': 0, 'n': 0}
            for i in range(10)
        }

        for p_t1, outcome in rows:
            if p_t1 is None:
                continue
            bucket_idx = min(int(p_t1 * 10), 9)
            buckets[bucket_idx]['n'] += 1
            if outcome in ('hit_t1', 'hit_t2', 'hit_t3'):
                buckets[bucket_idx]['hits'] += 1

        curve = []
        for i in range(10):
            b = buckets[i]
            n = b['n']
            actual = round(b['hits'] / n, 4) if n > 0 else None
            lo = round(i * 0.1, 2)
            hi = round(lo + 0.1, 2)
            curve.append({
                'bucket':  f'{int(lo*100)}-{int(hi*100)}%',
                'stated':  b['stated_mid'],
                'actual':  actual,
                'n':       n,
            })
        return curve

    # ── Summary helpers ───────────────────────────────────────────────────────

    def get_open_predictions(self) -> List[Dict[str, Any]]:
        """Return all open (unresolved) prediction rows as dicts."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT id, ticker, pattern_type, timeframe, entry_price,
                          target_1, target_2, stop_loss,
                          p_hit_t1, p_hit_t2, p_stopped_out,
                          market_regime, conviction_tier, issued_at, expires_at
                   FROM prediction_ledger
                   WHERE outcome IS NULL
                   ORDER BY issued_at DESC""",
            ).fetchall()
            cols = [
                'id', 'ticker', 'pattern_type', 'timeframe', 'entry_price',
                'target_1', 'target_2', 'stop_loss',
                'p_hit_t1', 'p_hit_t2', 'p_stopped_out',
                'market_regime', 'conviction_tier', 'issued_at', 'expires_at',
            ]
            return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()
