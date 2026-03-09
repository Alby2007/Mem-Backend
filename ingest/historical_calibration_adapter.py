"""
ingest/historical_calibration_adapter.py — Scheduled Historical Calibration

Thin BaseIngestAdapter wrapper around HistoricalCalibrator.calibrate_watchlist().
Registered in api_v2.py at interval_sec=86400, offset to 03:00 UTC so it runs
during off-peak hours and doesn't compete with real-time ingest adapters.

Produces no KB atoms directly — writes to signal_calibration table instead.
Returns a single summary atom so the ingest scheduler can track last-run status.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)


class HistoricalCalibrationAdapter(BaseIngestAdapter):
    """
    Nightly job that slides a detection window through historical OHLCV for all
    watchlist tickers and back-populates signal_calibration with real hit rates.

    Designed to run once per day at ~03:00 UTC. Skipped if calibration has
    already run within the last 20 hours (prevents double-runs on restarts).
    """

    name = "historical_calibration"

    def __init__(self, db_path: str, lookback_years: int = 3):
        self._db_path       = db_path
        self._lookback_years = lookback_years

    def fetch(self) -> dict:
        try:
            from analytics.historical_calibration import HistoricalCalibrator
        except ImportError as e:
            _logger.warning("HistoricalCalibrationAdapter: calibrator not available: %s", e)
            return {"skipped": True, "reason": str(e)}

        try:
            import sqlite3 as _sq3
            conn = _sq3.connect(self._db_path, timeout=5)
            row = conn.execute(
                "SELECT MAX(last_updated) FROM signal_calibration"
            ).fetchone()
            conn.close()
            if row and row[0]:
                from datetime import timedelta
                last_run = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
                hours_since = (datetime.now(timezone.utc) - last_run).total_seconds() / 3600
                if hours_since < 20:
                    _logger.info(
                        "HistoricalCalibrationAdapter: skipping — last run %.1fh ago",
                        hours_since,
                    )
                    return {"skipped": True, "reason": f"last_run_{hours_since:.1f}h_ago"}
        except Exception:
            pass

        _logger.info("HistoricalCalibrationAdapter: starting calibrate_watchlist (lookback=%dy)",
                     self._lookback_years)
        cal = HistoricalCalibrator(db_path=self._db_path)
        results = cal.calibrate_watchlist(lookback_years=self._lookback_years)

        total_patterns = sum(r.get("patterns_detected", 0) for r in results.values())
        total_rows     = sum(r.get("calibration_rows_written", 0) for r in results.values())
        _logger.info(
            "HistoricalCalibrationAdapter: done — %d tickers, %d patterns, %d rows",
            len(results), total_patterns, total_rows,
        )
        return {
            "tickers": len(results),
            "patterns_detected": total_patterns,
            "calibration_rows_written": total_rows,
        }

    def transform(self, raw: dict) -> List[RawAtom]:
        if raw.get("skipped"):
            return []
        now = datetime.now(timezone.utc).isoformat()
        return [
            RawAtom(
                subject    = "system",
                predicate  = "calibration_last_run",
                object     = now,
                confidence = 1.0,
                source     = "historical_calibration_adapter",
                metadata   = {
                    "tickers":                  str(raw.get("tickers", 0)),
                    "patterns_detected":        str(raw.get("patterns_detected", 0)),
                    "calibration_rows_written": str(raw.get("calibration_rows_written", 0)),
                },
                upsert = True,
            )
        ]
