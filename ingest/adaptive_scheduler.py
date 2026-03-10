"""
ingest/adaptive_scheduler.py — Market-Aware Adaptive Scan Frequency

Runs a 15-minute background loop that reads the current market state and
adjusts adapter scan intervals dynamically. Uses run_now() for urgency
spikes (anomaly detected) and update_interval() for sustained frequency
changes based on volatility regime.

RULES
=====
  volatility_regime=extreme  → ÷3 interval for price/pattern adapters (floor 600s)
  volatility_regime=high     → ÷2 interval (floor 600s)
  volatility_regime=low      → ×2 interval (ceiling 3600s)
  volatility_regime=medium   → restore base intervals
  anomaly_severity > 0.7     → run_now(yfinance_adapter) immediately
  transition prob > 60%      → run_now(pattern_adapter)

DESIGN
======
- Non-breaking: if market state is unavailable, intervals stay at base values
- Idempotent: re-running with same state does nothing
- IngestScheduler needs update_interval() method (added in scheduler.py)

USAGE
=====
  from ingest.adaptive_scheduler import AdaptiveScheduler
  adaptive = AdaptiveScheduler(scheduler, db_path, base_intervals)
  adaptive.start()   # non-blocking background loop
  adaptive.stop()
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

_log = logging.getLogger(__name__)

# Adapters that respond to volatility-based frequency changes
_PRICE_ADAPTERS   = {'yfinance_adapter', 'kb_price_adapter', 'ohlcv_adapter'}
_PATTERN_ADAPTERS = {'pattern_adapter', 'pattern_signal_adapter'}
_FREQ_ADAPTERS    = _PRICE_ADAPTERS | _PATTERN_ADAPTERS

# Volatility → multiplier applied to base interval
_VOL_MULT: Dict[str, float] = {
    'extreme': 1/3,
    'high':    1/2,
    'medium':  1.0,
    'normal':  1.0,
    'low':     2.0,
}

_CHECK_INTERVAL_SEC = 900   # 15 minutes between state checks
_FLOOR_SEC          = 600   # never below 10 minutes
_CEILING_SEC        = 3600  # never above 60 minutes
_ANOMALY_THRESHOLD  = 0.7


class AdaptiveScheduler:
    """
    Wraps IngestScheduler with market-state-aware frequency adjustment.

    Parameters
    ----------
    scheduler       : IngestScheduler instance (already populated with adapters)
    db_path         : Path to SQLite KB
    base_intervals  : dict of {adapter_name: base_interval_sec} — the "normal" values
                      to restore in medium/normal vol. If not provided, current
                      scheduler intervals are used as base.
    """

    def __init__(self, scheduler, db_path: str, base_intervals: Optional[Dict[str, float]] = None) -> None:
        self._scheduler = scheduler
        self._db = db_path
        self._base_intervals = base_intervals or {}
        self._last_vol_regime: Optional[str] = None
        self._running = False
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the adaptive check loop. Non-blocking."""
        self._running = True
        _log.info('AdaptiveScheduler: starting (check every %ds)', _CHECK_INTERVAL_SEC)
        self._schedule_check()

    def stop(self) -> None:
        """Stop the adaptive check loop."""
        self._running = False
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
        _log.info('AdaptiveScheduler: stopped')

    def _schedule_check(self) -> None:
        if not self._running:
            return
        timer = threading.Timer(_CHECK_INTERVAL_SEC, self._run_check)
        timer.daemon = True
        timer.name = 'adaptive-scheduler-check'
        with self._lock:
            self._timer = timer
        timer.start()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_vol_regime(self, conn: sqlite3.Connection) -> str:
        """Read current volatility regime from global snapshot or facts."""
        try:
            row = conn.execute(
                """SELECT state_json FROM market_state_snapshots
                   WHERE scope='global' ORDER BY snapshot_at DESC LIMIT 1"""
            ).fetchone()
            if row:
                state = json.loads(row['state_json'])
                vol = state.get('volatility_regime', '')
                if vol:
                    return vol.lower().strip()
        except Exception:
            pass
        # Fallback to facts
        row = conn.execute(
            """SELECT object FROM facts
               WHERE predicate='volatility_regime'
                 AND subject IN ('market', 'global_macro_regime', '^VIX', 'vix')
               ORDER BY timestamp DESC LIMIT 1"""
        ).fetchone()
        return (row['object'].lower().strip() if row else 'medium')

    def _get_anomalous_tickers(self, conn: sqlite3.Connection) -> list:
        """Return tickers with anomaly_severity > threshold."""
        rows = conn.execute(
            """SELECT subject FROM facts
               WHERE predicate='anomaly_severity'
                 AND CAST(object AS REAL) > ?
               ORDER BY CAST(object AS REAL) DESC""",
            (_ANOMALY_THRESHOLD,),
        ).fetchall()
        return [r['subject'] for r in rows]

    def _get_transition_confidence(self, conn: sqlite3.Connection) -> float:
        """
        Check if any next-state transition has probability > 60%.
        Returns the max transition probability found, or 0.0.
        """
        try:
            from analytics.state_transitions import TransitionEngine
            engine = TransitionEngine(self._db)
            forecast = engine.get_current_state_forecast(scope='global', subject='market')
            if forecast and forecast.transitions:
                return max(t.probability for t in forecast.transitions)
        except Exception:
            pass
        return 0.0

    def _adjust_intervals(self, vol_regime: str) -> None:
        """Apply volatility-based interval changes to freq adapters."""
        mult = _VOL_MULT.get(vol_regime, 1.0)
        if mult == 1.0 and vol_regime == self._last_vol_regime:
            return  # no change needed

        for adapter_name in _FREQ_ADAPTERS:
            base = self._base_intervals.get(adapter_name)
            if base is None:
                # Try to read from scheduler status
                status = self._scheduler.get_status().get(adapter_name)
                if status:
                    base = status.get('interval_sec', 1800)
                    self._base_intervals[adapter_name] = base
                else:
                    continue

            new_interval = round(base * mult)
            new_interval = max(_FLOOR_SEC, min(_CEILING_SEC, new_interval))

            try:
                self._scheduler.update_interval(adapter_name, new_interval)
                _log.info(
                    'AdaptiveScheduler: %s interval → %ds (vol=%s mult=%.1f)',
                    adapter_name, new_interval, vol_regime, mult,
                )
            except Exception as _e:
                _log.debug('AdaptiveScheduler: update_interval failed for %s: %s', adapter_name, _e)

    def _run_check(self) -> None:
        """Single check cycle — called every _CHECK_INTERVAL_SEC seconds."""
        try:
            conn = self._conn()
            try:
                vol_regime = self._get_vol_regime(conn)
                anomalous  = self._get_anomalous_tickers(conn)
                trans_conf = self._get_transition_confidence(conn)
            finally:
                conn.close()

            # 1. Volatility-based interval adjustment
            if vol_regime != self._last_vol_regime:
                _log.info(
                    'AdaptiveScheduler: vol_regime changed %s → %s',
                    self._last_vol_regime, vol_regime,
                )
                self._adjust_intervals(vol_regime)
                self._last_vol_regime = vol_regime

            # 2. Anomaly urgency: trigger immediate yfinance re-scan
            if anomalous:
                _log.info(
                    'AdaptiveScheduler: %d anomalous tickers — triggering run_now',
                    len(anomalous),
                )
                for adapter_name in _PRICE_ADAPTERS:
                    self._scheduler.run_now(adapter_name)

            # 3. Regime shift imminent: pre-emptive pattern scan
            if trans_conf > 0.60:
                _log.info(
                    'AdaptiveScheduler: regime shift imminent (max_prob=%.0f%%) — run_now pattern',
                    trans_conf * 100,
                )
                for adapter_name in _PATTERN_ADAPTERS:
                    self._scheduler.run_now(adapter_name)

        except Exception as _e:
            _log.warning('AdaptiveScheduler check error: %s', _e)
        finally:
            self._schedule_check()  # always reschedule
