"""
analytics/signal_decay_predictor.py — Signal Lifecycle / Decay Predictor

Estimates remaining validity time for each active pattern signal using
historical resolution times from signal_calibration, adjusted for the
current volatility regime.

DECAY MODEL
===========
  expected_hours = calibration.avg_time_to_target_hours  (or pattern-type default)
  vol_mult = {low: 1.5, medium: 1.0, high: 0.7, extreme: 0.5}[volatility_regime]
  expected_hours *= vol_mult
  decay_pct = hours_open / expected_hours   (capped at 1.0)

DEFAULTS (until calibration accumulates data)
=============================================
  fvg: 72h  |  ifvg: 60h  |  mitigation: 48h
  orderblock: 84h  |  breaker: 96h  |  other: 72h

ATOMS WRITTEN
=============
  {ticker} | pattern_estimated_expiry | 2026-03-12T14:00:00Z
  {ticker} | pattern_decay_pct        | 0.45
  {ticker} | pattern_hours_remaining  | 39.5

USAGE
=====
  from analytics.signal_decay_predictor import SignalDecayPredictor
  sdp = SignalDecayPredictor(db_path)
  results = sdp.run()
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

# ── Pattern-type defaults (hours until expected resolution) ──────────────────
_PATTERN_DEFAULTS: Dict[str, float] = {
    'fvg':          72.0,
    'ifvg':         60.0,
    'fair_value_gap': 72.0,
    'mitigation':   48.0,
    'mitigation_block': 48.0,
    'orderblock':   84.0,
    'order_block':  84.0,
    'breaker':      96.0,
    'breaker_block': 96.0,
    'bos':          36.0,
    'choch':        36.0,
    'liquidity':    48.0,
    'sweep':        24.0,
    'imbalance':    60.0,
    'support':      120.0,
    'resistance':   120.0,
}
_DEFAULT_HOURS = 72.0

# ── Volatility regime multiplier ─────────────────────────────────────────────
_VOL_MULT: Dict[str, float] = {
    'low':     1.5,
    'medium':  1.0,
    'normal':  1.0,
    'high':    0.7,
    'extreme': 0.5,
}


class SignalDecayPredictor:

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
        confidence: float = 0.85,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
               VALUES (?, ?, ?, ?, 'signal_decay_predictor', ?)
               ON CONFLICT(subject, predicate, source)
               DO UPDATE SET object=excluded.object, confidence=excluded.confidence,
                             timestamp=excluded.timestamp""",
            (subject.lower(), predicate, str(obj), confidence, now),
        )

    def _get_volatility_regime(self, conn: sqlite3.Connection) -> str:
        """Read current global volatility regime from latest snapshot or facts."""
        # Try global snapshot first
        try:
            import json
            row = conn.execute(
                """SELECT state_json FROM market_state_snapshots
                   WHERE scope='global' ORDER BY snapshot_at DESC LIMIT 1"""
            ).fetchone()
            if row:
                state = json.loads(row['state_json'])
                vol = state.get('volatility_regime', '')
                if vol:
                    return vol.lower()
        except Exception:
            pass

        # Fallback: facts table
        row = conn.execute(
            """SELECT object FROM facts
               WHERE predicate='volatility_regime' AND subject IN ('market','global_macro_regime','vix')
               ORDER BY timestamp DESC LIMIT 1"""
        ).fetchone()
        return (row['object'].lower() if row else 'medium')

    def _get_calibration_hours(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        pattern_type: str,
    ) -> Optional[float]:
        """Look up avg_time_to_target_hours from signal_calibration."""
        try:
            row = conn.execute(
                """SELECT avg_time_to_target_hours FROM signal_calibration
                   WHERE ticker=? AND pattern_type=?
                     AND avg_time_to_target_hours IS NOT NULL
                   ORDER BY sample_size DESC LIMIT 1""",
                (ticker.upper(), pattern_type),
            ).fetchone()
            if row and row['avg_time_to_target_hours']:
                return float(row['avg_time_to_target_hours'])
        except Exception:
            pass
        return None

    def _expected_hours(
        self,
        conn: sqlite3.Connection,
        ticker: str,
        pattern_type: str,
        vol_regime: str,
    ) -> float:
        """Compute expected resolution hours with volatility adjustment."""
        # Try calibration data first
        cal_h = self._get_calibration_hours(conn, ticker, pattern_type)

        if cal_h and cal_h > 0:
            base_h = cal_h
        else:
            # Pattern-type default
            pt = (pattern_type or '').lower()
            base_h = _PATTERN_DEFAULTS.get(pt, _DEFAULT_HOURS)

        # Apply volatility multiplier
        mult = _VOL_MULT.get(vol_regime, 1.0)
        return max(base_h * mult, 4.0)  # floor: 4h minimum

    def _get_active_patterns(self, conn: sqlite3.Connection) -> List[dict]:
        """Return all active (not filled/broken) pattern signals."""
        rows = conn.execute(
            """SELECT id, ticker, pattern_type, direction, created_at, quality_score
               FROM pattern_signals
               WHERE status NOT IN ('filled', 'broken', 'expired')
               ORDER BY created_at ASC"""
        ).fetchall()
        return [dict(r) for r in rows]

    def run(self) -> dict:
        """
        Compute decay for all active patterns and write KB atoms.
        Returns {patterns_processed, expired_count, atoms_written}.
        """
        conn = self._conn()
        processed = 0
        expired_count = 0
        atoms_written = 0
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        try:
            vol_regime = self._get_volatility_regime(conn)
            _log.info('SignalDecayPredictor: vol_regime=%s', vol_regime)

            patterns = self._get_active_patterns(conn)
            _log.info('SignalDecayPredictor: processing %d active patterns', len(patterns))

            # Group by ticker: track the best (lowest decay) per ticker
            ticker_best: Dict[str, dict] = {}

            for pat in patterns:
                ticker = pat['ticker']
                pattern_type = (pat.get('pattern_type') or '').lower()
                created_at_str = pat.get('created_at', '')

                try:
                    created_at = datetime.fromisoformat(
                        created_at_str.replace('Z', '+00:00')
                    )
                except Exception:
                    continue

                hours_open = (now - created_at).total_seconds() / 3600
                expected_h = self._expected_hours(conn, ticker, pattern_type, vol_regime)
                decay_pct  = min(round(hours_open / expected_h, 3), 1.0)
                hours_remaining = max(round(expected_h - hours_open, 1), 0.0)
                expiry_dt  = created_at + timedelta(hours=expected_h)
                expiry_iso = expiry_dt.isoformat()

                processed += 1
                if decay_pct >= 1.0:
                    expired_count += 1

                # Keep track of the pattern with the BEST (lowest decay) for this ticker
                existing = ticker_best.get(ticker)
                if existing is None or decay_pct < existing['decay_pct']:
                    ticker_best[ticker] = {
                        'decay_pct':       decay_pct,
                        'hours_remaining': hours_remaining,
                        'expiry_iso':      expiry_iso,
                        'pattern_type':    pattern_type,
                        'hours_open':      round(hours_open, 1),
                        'expected_h':      round(expected_h, 1),
                    }

            # Write atoms per ticker (best pattern's decay)
            for ticker, best in ticker_best.items():
                conf = 1.0 - best['decay_pct'] * 0.3  # high decay = lower confidence
                self._write_atom(conn, ticker, 'pattern_decay_pct',
                                 str(best['decay_pct']), round(conf, 3))
                self._write_atom(conn, ticker, 'pattern_estimated_expiry',
                                 best['expiry_iso'], round(conf, 3))
                self._write_atom(conn, ticker, 'pattern_hours_remaining',
                                 str(best['hours_remaining']), round(conf, 3))
                atoms_written += 3

            conn.commit()

        finally:
            conn.close()

        _log.info(
            'SignalDecayPredictor: %d patterns → %d tickers (%d expired), %d atoms',
            processed, len(ticker_best) if 'ticker_best' in dir() else 0,
            expired_count, atoms_written,
        )
        return {
            'patterns_processed': processed,
            'expired_count':      expired_count,
            'atoms_written':      atoms_written,
        }
