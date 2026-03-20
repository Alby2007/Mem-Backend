"""
analytics/anomaly_detector.py — Market State Anomaly Detector

Computes rolling baselines from market_state_snapshots and flags tickers
whose current state deviates significantly from their historical norm.
Also detects global market anomalies when many tickers diverge simultaneously.

BASELINE WARM-UP GUARD
======================
A ticker needs ≥20 snapshots (~5 days at 4/day) before anomaly detection
activates. Immature tickers are silently skipped.

DEVIATION CRITERIA
==================
Current state differs from median baseline on ≥2 state vector dimensions.

GLOBAL ANOMALY TRIGGERS (either condition)
==========================================
1. >40% of watchlist has individual anomaly scores
2. State flip rate in the last 6h is >3× the rolling average flip rate

ATOMS WRITTEN
=============
  {ticker} | anomaly_detected     | company_specific  (or sector_wide / market_wide)
  {ticker} | anomaly_severity     | 0.82
  {ticker} | anomaly_description  | "volatility_regime jumped extreme..."
  market   | global_anomaly       | true
  market   | global_anomaly_description | "42% of watchlist diverged..."

USAGE
=====
  from analytics.anomaly_detector import AnomalyDetector
  detector = AnomalyDetector(db_path)
  results = detector.run()
"""

from __future__ import annotations

import logging
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_MIN_SNAPSHOTS       = 10     # warm-up guard: skip tickers below this (was 20; matches CorrelationDiscovery)
_BASELINE_WINDOW     = 30     # number of snapshots for baseline
_DEVIATION_DIMS      = 2      # dimensions that must differ for anomaly
_GLOBAL_PCT_THRESH   = 0.40   # >40% of watchlist = global anomaly
_FLIP_RATE_MULT      = 3.0    # flip rate >3× rolling average = global event
_SEVERITY_SCALE      = 5      # max dimensions to normalise severity

# Dimensions tracked for deviation
_STATE_DIMS = [
    'signal_direction',
    'conviction_tier',
    'volatility_regime',
    'price_regime',
    'macro_confirmation',
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _modal(values: List[str]) -> Optional[str]:
    """Return the most common non-None value in a list."""
    if not values:
        return None
    counts = Counter(v for v in values if v)
    return counts.most_common(1)[0][0] if counts else None


def _count_deviations(baseline: Dict[str, str], current: Dict[str, str]) -> List[str]:
    """Return list of dimension names that differ between baseline and current."""
    deviating = []
    for dim in _STATE_DIMS:
        b = baseline.get(dim)
        c = current.get(dim)
        if b and c and b != c:
            deviating.append(dim)
    return deviating


def _severity(n_deviating: int) -> float:
    """Map number of deviating dimensions to 0.0–1.0 severity score."""
    return round(min(n_deviating / _SEVERITY_SCALE, 1.0), 3)


# ── AnomalyDetector ────────────────────────────────────────────────────────────

class AnomalyDetector:

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _write_atom(
        self,
        conn: sqlite3.Connection,
        subject: str,
        predicate: str,
        obj: str,
        confidence: float = 0.90,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    pg.cursor().execute(
                        """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
                           VALUES (%s, %s, %s, %s, 'anomaly_detector', %s)
                           ON CONFLICT(subject, predicate, object)
                           DO UPDATE SET confidence=EXCLUDED.confidence, source=EXCLUDED.source,
                                         timestamp=EXCLUDED.timestamp""",
                        (subject.lower(), predicate, str(obj), confidence, now))
                return
            except Exception:
                pass
        conn.execute(
            """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
               VALUES (?, ?, ?, ?, 'anomaly_detector', ?)
               ON CONFLICT(subject, predicate, source)
               DO UPDATE SET object=excluded.object, confidence=excluded.confidence,
                             timestamp=excluded.timestamp""",
            (subject.lower(), predicate, str(obj), confidence, now),
        )

    def _clear_anomaly(self, conn: sqlite3.Connection, subject: str) -> None:
        """Remove anomaly atoms for a ticker that is no longer anomalous."""
        conn.execute(
            """DELETE FROM facts WHERE subject=? AND source='anomaly_detector'
               AND predicate IN ('anomaly_detected','anomaly_severity','anomaly_description')""",
            (subject.lower(),),
        )

    def _get_snapshots(
        self,
        conn: sqlite3.Connection,
        subject: str,
        limit: int = _BASELINE_WINDOW + 1,
    ) -> List[dict]:
        """Return last `limit` snapshots for a subject, newest last."""
        import json
        rows = conn.execute(
            """SELECT snapshot_at, state_json FROM market_state_snapshots
               WHERE scope='ticker' AND subject=?
               ORDER BY snapshot_at ASC
               LIMIT ?""",
            (subject, limit),
        ).fetchall()
        result = []
        for r in rows:
            try:
                state = json.loads(r['state_json'])
                result.append({'snapshot_at': r['snapshot_at'], 'state': state})
            except Exception:
                pass
        return result

    def _build_baseline(self, snapshots: List[dict]) -> Dict[str, str]:
        """Compute modal value per dimension across baseline snapshots."""
        dim_values: Dict[str, List[str]] = defaultdict(list)
        for snap in snapshots:
            state = snap['state']
            for dim in _STATE_DIMS:
                val = state.get(dim)
                if val:
                    dim_values[dim].append(str(val).lower())
        return {dim: _modal(vals) for dim, vals in dim_values.items() if vals}

    def _compute_flip_rate(self, snapshots: List[dict], window_hours: float = 6.0) -> Tuple[float, float]:
        """
        Returns (recent_flip_rate, rolling_flip_rate).
        flip_rate = fraction of consecutive snapshot pairs where signal_direction changed.
        """
        if len(snapshots) < 4:
            return 0.0, 0.0

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)

        recent_flips = 0
        recent_pairs = 0
        total_flips = 0
        total_pairs = 0

        for i in range(1, len(snapshots)):
            prev_dir = snapshots[i - 1]['state'].get('signal_direction', '')
            curr_dir = snapshots[i]['state'].get('signal_direction', '')
            if prev_dir and curr_dir:
                flipped = int(prev_dir != curr_dir)
                total_flips += flipped
                total_pairs += 1

                try:
                    ts = datetime.fromisoformat(
                        snapshots[i]['snapshot_at'].replace('Z', '+00:00')
                    )
                    if ts >= cutoff:
                        recent_flips += flipped
                        recent_pairs += 1
                except Exception:
                    pass

        recent_rate = recent_flips / recent_pairs if recent_pairs > 0 else 0.0
        rolling_rate = total_flips / total_pairs if total_pairs > 0 else 0.0
        return recent_rate, rolling_rate

    def _get_all_ticker_subjects(self, conn: sqlite3.Connection) -> List[str]:
        rows = conn.execute(
            """SELECT DISTINCT subject FROM market_state_snapshots
               WHERE scope='ticker'"""
        ).fetchall()
        return [r['subject'] for r in rows]

    def run(self) -> dict:
        """
        Run anomaly detection across all tracked tickers.

        Returns dict: {
            anomalous_tickers: [{ticker, severity, description, deviating_dims}],
            global_anomaly: bool,
            global_description: str,
            skipped_immature: int,
            total_scanned: int,
        }
        """
        conn = self._conn()
        anomalous: List[dict] = []
        skipped_immature = 0
        total_scanned = 0

        try:
            tickers = self._get_all_ticker_subjects(conn)
            _log.info('AnomalyDetector: scanning %d ticker subjects', len(tickers))

            for ticker in tickers:
                try:
                    snapshots = self._get_snapshots(conn, ticker, limit=_BASELINE_WINDOW + 1)

                    if len(snapshots) < _MIN_SNAPSHOTS:
                        skipped_immature += 1
                        continue

                    total_scanned += 1
                    baseline_snaps = snapshots[:-1]  # all but latest
                    current_snap   = snapshots[-1]

                    baseline = self._build_baseline(baseline_snaps)
                    current  = current_snap['state']

                    deviating = _count_deviations(baseline, current)
                    n_dev = len(deviating)

                    if n_dev >= _DEVIATION_DIMS:
                        sev = _severity(n_dev)
                        desc_parts = []
                        for dim in deviating:
                            b_val = baseline.get(dim, '?')
                            c_val = current.get(dim, '?')
                            desc_parts.append(f'{dim}: {b_val}→{c_val}')
                        description = '; '.join(desc_parts)

                        # Classify: company-specific vs sector/market
                        anomaly_type = 'company_specific'

                        is_first = not conn.execute(
                            "SELECT 1 FROM facts WHERE subject=? AND predicate='anomaly_severity' AND source='anomaly_detector'",
                            (ticker.lower(),)
                        ).fetchone()
                        self._write_atom(conn, ticker, 'anomaly_detected', anomaly_type, sev)
                        self._write_atom(conn, ticker, 'anomaly_severity', str(sev), sev)
                        self._write_atom(conn, ticker, 'anomaly_description', description, sev)
                        if is_first:
                            _log.info('AnomalyDetector: first atom for %s — sev=%.2f %s', ticker, sev, description)

                        anomalous.append({
                            'ticker':        ticker,
                            'severity':      sev,
                            'description':   description,
                            'deviating_dims': deviating,
                            'n_deviating':   n_dev,
                        })
                    else:
                        # Clear stale anomaly atoms
                        self._clear_anomaly(conn, ticker)

                except Exception as _e:
                    _log.debug('AnomalyDetector: error on %s: %s', ticker, _e)

            conn.commit()

            # ── Global anomaly check ───────────────────────────────────────────
            global_anomaly = False
            global_desc = ''

            if total_scanned > 0:
                anomaly_pct = len(anomalous) / total_scanned

                # Trigger 1: absolute fraction
                if anomaly_pct > _GLOBAL_PCT_THRESH:
                    global_anomaly = True
                    global_desc = (
                        f'{len(anomalous)}/{total_scanned} tickers '
                        f'({anomaly_pct:.0%}) diverged from baseline simultaneously'
                    )

                # Trigger 2: flip rate spike
                if not global_anomaly and len(tickers) > 0:
                    recent_rates = []
                    rolling_rates = []
                    for ticker in tickers[:50]:  # sample first 50 for speed
                        try:
                            snaps = self._get_snapshots(conn, ticker, limit=_BASELINE_WINDOW)
                            if len(snaps) >= 4:
                                recent, rolling = self._compute_flip_rate(snaps)
                                recent_rates.append(recent)
                                rolling_rates.append(rolling)
                        except Exception:
                            pass
                    if recent_rates and rolling_rates:
                        avg_recent  = sum(recent_rates)  / len(recent_rates)
                        avg_rolling = sum(rolling_rates) / len(rolling_rates)
                        if avg_rolling > 0 and avg_recent > avg_rolling * _FLIP_RATE_MULT:
                            global_anomaly = True
                            global_desc = (
                                f'State flip rate {avg_recent:.1%} is '
                                f'{avg_recent/avg_rolling:.1f}× the rolling average '
                                f'{avg_rolling:.1%} — possible market event'
                            )

            if global_anomaly:
                self._write_atom(conn, 'market', 'global_anomaly', 'true', 0.85)
                self._write_atom(conn, 'market', 'global_anomaly_description', global_desc, 0.85)
                _log.warning('AnomalyDetector: GLOBAL anomaly — %s', global_desc)
            else:
                self._clear_anomaly(conn, 'market')

            conn.commit()

        finally:
            conn.close()

        _log.info(
            'AnomalyDetector: %d anomalous / %d scanned (skipped %d immature)',
            len(anomalous), total_scanned, skipped_immature,
        )
        return {
            'anomalous_tickers':  anomalous,
            'global_anomaly':     global_anomaly,
            'global_description': global_desc,
            'skipped_immature':   skipped_immature,
            'total_scanned':      total_scanned,
        }
