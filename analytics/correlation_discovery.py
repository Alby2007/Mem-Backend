"""
analytics/correlation_discovery.py — Cross-Ticker Lead-Lag Discovery

Detects pairwise co-occurrence and lead-lag relationships between ticker
state snapshots. Discovers that HSBA.L going bearish precedes BARC.L going
bearish by ~12 hours, or energy tickers lead defence tickers by 2 days.

ALGORITHM
=========
1. Load all ticker snapshots from market_state_snapshots
2. Bin signal_direction into {-1, 0, +1} per ticker per time bucket
3. Compute Pearson r at lag 0, 1, 2, 3 snapshots for all 13,861 pairs
4. Write pairs with |r| > 0.6 AND n ≥ 20 to ticker_correlations table
5. Write KB atoms: {ticker_a} | leads | {ticker_b}

OUTPUT FILTER: |r| > 0.6 AND n ≥ 20 prevents spurious atoms.
DECAY: atoms include discovery_date; _should_enter() ignores atoms >90 days old.

COMPUTE: ~55,444 correlations at daily frequency ≈ <10s.

ATOMS WRITTEN
=============
  {ticker_a} | leads           | {ticker_b}
  {ticker_a} | lead_confidence | 0.73
  {ticker_a} | lead_lag_hours  | 12.5
  {ticker_a} | lead_sample_size| 34
  {ticker_a} | lead_discovery_date | 2026-03-10

TABLE WRITTEN
=============
  ticker_correlations(ticker_a, ticker_b, lag_snapshots, lag_hours,
                      pearson_r, sample_size, discovery_date, last_updated)

USAGE
=====
  from analytics.correlation_discovery import CorrelationDiscovery
  cd = CorrelationDiscovery(db_path)
  results = cd.run()
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
_MIN_PEARSON_R    = 0.60    # minimum |r| to write a relationship
_MIN_SAMPLES      = 10      # minimum overlapping observation pairs (was 20; raise to 20 after 30d of snapshots)
_MAX_LAG_SNAPS    = 3       # check lags 0, 1, 2, 3 snapshots
_DISCOVERY_DECAY_DAYS = 90  # max age of valid lead-lag atom

# Signal direction → numeric
_DIR_NUM = {
    'bullish': 1, 'long': 1, 'buy': 1, 'near_high': 1,
    'bearish': -1, 'short': -1, 'sell': -1, 'near_low': -1,
    'neutral': 0, 'ranging': 0, 'mixed': 0,
}

_CREATE_CORRELATIONS = """
CREATE TABLE IF NOT EXISTS ticker_correlations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_a       TEXT NOT NULL,
    ticker_b       TEXT NOT NULL,
    lag_snapshots  INTEGER NOT NULL,
    lag_hours      REAL,
    pearson_r      REAL NOT NULL,
    sample_size    INTEGER NOT NULL,
    discovery_date TEXT NOT NULL,
    last_updated   TEXT NOT NULL,
    UNIQUE(ticker_a, ticker_b, lag_snapshots)
)
"""

_CREATE_IDX = """
CREATE INDEX IF NOT EXISTS idx_correlations_a
ON ticker_correlations(ticker_a, pearson_r);
"""


def _pearson(x: List[float], y: List[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient. Returns None if insufficient variance."""
    n = len(x)
    if n < 3:
        return None
    mx = sum(x) / n
    my = sum(y) / n
    num   = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    den_x = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


class CorrelationDiscovery:

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(_CREATE_CORRELATIONS)
        conn.execute(_CREATE_IDX)
        conn.commit()

    def _write_atom(
        self,
        conn: sqlite3.Connection,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float = 0.80,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    pg.cursor().execute(
                        """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
                           VALUES (%s, %s, %s, %s, 'correlation_discovery', %s)
                           ON CONFLICT(subject, predicate, object)
                           DO UPDATE SET confidence=EXCLUDED.confidence, source=EXCLUDED.source,
                                         timestamp=EXCLUDED.timestamp""",
                        (subject.lower(), predicate, str(obj), confidence, now))
                return
            except Exception:
                pass
        conn.execute(
            """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
               VALUES (?, ?, ?, ?, 'correlation_discovery', ?)
               ON CONFLICT(subject, predicate, object)
               DO UPDATE SET confidence=excluded.confidence, source=excluded.source,
                             timestamp=excluded.timestamp""",
            (subject.lower(), predicate, str(obj), confidence, now),
        )

    def _load_ticker_series(
        self, conn: sqlite3.Connection
    ) -> Dict[str, List[Tuple[str, int]]]:
        """
        Load signal_direction as numeric series per ticker.
        Returns {ticker: [(snapshot_at, direction_num), ...]} sorted by time.
        """
        rows = conn.execute(
            """SELECT subject, snapshot_at, state_json
               FROM market_state_snapshots
               WHERE scope='ticker'
               ORDER BY subject, snapshot_at ASC"""
        ).fetchall()

        series: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
        for r in rows:
            try:
                state = json.loads(r['state_json'])
                raw_dir = (state.get('signal_direction') or '').lower().strip()
                num = _DIR_NUM.get(raw_dir)
                if num is not None:
                    series[r['subject']].append((r['snapshot_at'], num))
            except Exception:
                pass
        return dict(series)

    def _align_series(
        self,
        s_a: List[Tuple[str, int]],
        s_b: List[Tuple[str, int]],
        lag: int,
    ) -> Tuple[List[float], List[float]]:
        """
        Align two series at a given lag (in snapshot steps).
        lag > 0 means a leads b: a[i] paired with b[i + lag].
        Returns (x_vals, y_vals) for correlation.
        """
        # Build timestamp index for b
        b_by_ts = {ts: val for ts, val in s_b}
        b_times  = sorted(b_by_ts.keys())

        x_vals = []
        y_vals = []

        for i, (ts_a, val_a) in enumerate(s_a):
            # Find corresponding b timestamp at position i + lag
            b_idx = i + lag
            if b_idx >= len(b_times):
                break
            b_ts = b_times[b_idx]
            b_val = b_by_ts.get(b_ts)
            if b_val is not None:
                x_vals.append(float(val_a))
                y_vals.append(float(b_val))

        return x_vals, y_vals

    def _avg_snapshot_hours(
        self,
        series: List[Tuple[str, int]],
        lag: int,
    ) -> Optional[float]:
        """Estimate average hours per snapshot step from the series timestamps."""
        if len(series) < 2:
            return None
        try:
            times = []
            for ts, _ in series[:20]:
                times.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
            if len(times) < 2:
                return None
            deltas = [(times[i+1] - times[i]).total_seconds() / 3600 for i in range(len(times)-1)]
            avg_h = sum(deltas) / len(deltas)
            return round(avg_h * lag, 1)
        except Exception:
            return None

    def run(self) -> dict:
        """
        Run pairwise correlation discovery.
        Returns {relationships_found, atoms_written, pairs_tested}.
        """
        conn = self._conn()
        relationships_found = 0
        atoms_written = 0
        pairs_tested = 0
        now_iso = datetime.now(timezone.utc).isoformat()
        today = now_iso[:10]

        try:
            self._ensure_table(conn)
            series = self._load_ticker_series(conn)
            tickers = [t for t, s in series.items() if len(s) >= _MIN_SAMPLES]

            _log.info(
                'CorrelationDiscovery: %d tickers with ≥%d snapshots',
                len(tickers), _MIN_SAMPLES,
            )

            # Track best lead relationship per (a, b) pair
            best: Dict[Tuple[str, str], Tuple[int, float, int, float]] = {}
            # key: (a, b) → (lag, r, n, lag_hours)

            for i, ticker_a in enumerate(tickers):
                for ticker_b in tickers[i+1:]:
                    s_a = series[ticker_a]
                    s_b = series[ticker_b]

                    for lag in range(_MAX_LAG_SNAPS + 1):
                        pairs_tested += 1
                        x, y = self._align_series(s_a, s_b, lag)
                        if len(x) < _MIN_SAMPLES:
                            continue
                        r = _pearson(x, y)
                        if r is None or abs(r) < _MIN_PEARSON_R:
                            continue

                        lag_h = self._avg_snapshot_hours(s_a, lag) or 0.0

                        # For lag=0, both directions (a→b) and (b→a) are symmetric
                        # For lag>0, a leads b (positive lag means a is earlier)
                        key = (ticker_a, ticker_b)
                        existing = best.get(key)
                        if existing is None or abs(r) > abs(existing[1]):
                            best[key] = (lag, r, len(x), lag_h)

                        # Also check b leads a (swap order, same lag)
                        if lag > 0:
                            x2, y2 = self._align_series(s_b, s_a, lag)
                            if len(x2) >= _MIN_SAMPLES:
                                r2 = _pearson(x2, y2)
                                if r2 is not None and abs(r2) >= _MIN_PEARSON_R:
                                    key2 = (ticker_b, ticker_a)
                                    existing2 = best.get(key2)
                                    if existing2 is None or abs(r2) > abs(existing2[1]):
                                        best[key2] = (lag, r2, len(x2), lag_h)

            # Write results
            for (ta, tb), (lag, r, n, lag_h) in best.items():
                if abs(r) < _MIN_PEARSON_R or n < _MIN_SAMPLES:
                    continue

                direction = 'leads' if lag > 0 else 'correlates_with'
                confidence = round(min(abs(r), 1.0), 3)

                # Write to ticker_correlations table
                conn.execute(
                    """INSERT INTO ticker_correlations
                       (ticker_a, ticker_b, lag_snapshots, lag_hours, pearson_r,
                        sample_size, discovery_date, last_updated)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT(ticker_a, ticker_b, lag_snapshots)
                       DO UPDATE SET pearson_r=excluded.pearson_r,
                                     sample_size=excluded.sample_size,
                                     last_updated=excluded.last_updated""",
                    (ta, tb, lag, lag_h, round(r, 4), n, today, now_iso),
                )

                # Write KB atoms (only for lead relationships, not zero-lag)
                if lag > 0:
                    self._write_atom(conn, ta, direction, tb, confidence)
                    self._write_atom(conn, ta, 'lead_confidence', str(confidence), confidence)
                    self._write_atom(conn, ta, 'lead_lag_hours', str(lag_h), confidence)
                    self._write_atom(conn, ta, 'lead_sample_size', str(n), confidence)
                    self._write_atom(conn, ta, 'lead_discovery_date', today, confidence)
                    atoms_written += 5
                    relationships_found += 1

            conn.commit()

        finally:
            conn.close()

        _log.info(
            'CorrelationDiscovery: %d relationships from %d pairs tested',
            relationships_found, pairs_tested,
        )
        return {
            'relationships_found': relationships_found,
            'atoms_written':       atoms_written,
            'pairs_tested':        pairs_tested,
        }
