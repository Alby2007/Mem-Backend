"""
ingest/signal_snapshot_adapter.py — Daily Signal Snapshot for Forward-Looking Backtest

Calls analytics.backtest.take_snapshot() once per calendar day.
Each snapshot captures the current conviction_tier, signal_quality, last_price,
and market_regime for every ticker that has a conviction_tier atom.

WHAT THIS ENABLES
=================
POST /analytics/backtest (run_backtest) compares snapshot T-1 conviction tiers
against price returns between T-1 and T. This is the only forward-looking
alpha validation in the system — it records what the system BELIEVED before
the return period, not after. Without daily snapshots, the backtest can only
run in backward-looking mode (wrong causal direction, documented as unreliable).

After 30+ snapshots (about 1 month), the backtest will have enough data to
produce a statistically meaningful alpha signal:
    alpha = True  iff  high_cohort_return > low_cohort_return + 1.0pp

DESIGN
======
- One run per calendar day (UTC). INSERT OR IGNORE on (ticker, snapshot_date)
  makes repeated calls idempotent.
- Registered at interval_sec=86400 — runs once daily alongside
  HistoricalCalibrationAdapter and CorrelationDiscoveryAdapter.
- Emits a single RawAtom for scheduler audit trail.

Registration in api_v2.py:
    from ingest.signal_snapshot_adapter import SignalSnapshotAdapter
    scheduler.register(SignalSnapshotAdapter(db_path=db_path), interval_sec=86400)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)


class SignalSnapshotAdapter(BaseIngestAdapter):
    """
    Daily adapter that takes a point-in-time snapshot of all conviction tiers
    into the signal_snapshots table for forward-looking backtest evaluation.
    """

    name = 'signal_snapshot'

    def __init__(self, db_path: str) -> None:
        super().__init__(name=self.name)
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        try:
            from analytics.backtest import take_snapshot, list_snapshots
        except ImportError as exc:
            _logger.warning('SignalSnapshotAdapter: import failed: %s', exc)
            return []

        try:
            # Check if today's snapshot already exists (idempotent)
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            existing = list_snapshots(self._db_path)
            if today in existing:
                _logger.debug(
                    'SignalSnapshotAdapter: snapshot for %s already exists — skipping', today
                )
                return []

            result = take_snapshot(self._db_path)
            _logger.info(
                'SignalSnapshotAdapter: snapshot %s — inserted=%d skipped=%d regime=%s',
                result.get('snapshot_date'),
                result.get('inserted', 0),
                result.get('skipped', 0),
                result.get('market_regime', 'unknown'),
            )

            # How many snapshots do we have now?
            all_dates = list_snapshots(self._db_path)
            n_snapshots = len(all_dates)
            _logger.info(
                'SignalSnapshotAdapter: %d total snapshot dates — '
                '%s forward-looking backtest',
                n_snapshots,
                'READY for' if n_snapshots >= 2 else f'need {2 - n_snapshots} more for',
            )

            return [RawAtom(
                subject    = 'system',
                predicate  = 'signal_snapshot_last_run',
                object     = today,
                confidence = 1.0,
                source     = 'signal_snapshot_adapter',
                metadata   = {
                    'snapshot_date':  today,
                    'inserted':       str(result.get('inserted', 0)),
                    'skipped':        str(result.get('skipped', 0)),
                    'market_regime':  str(result.get('market_regime', '')),
                    'total_snapshots': str(n_snapshots),
                },
                upsert = True,
            )]

        except Exception as exc:
            _logger.error('SignalSnapshotAdapter.fetch() failed: %s', exc)
            return []

    def transform(self, atoms: List[RawAtom]) -> List[RawAtom]:
        return atoms
