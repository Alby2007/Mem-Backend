"""
ingest/transition_builder_adapter.py — Daily State Transition Builder

Processes sequential market_state_snapshots into the state_transitions table.
Runs once per day (86400s). Idempotent — safely re-processes existing snapshots
via UNIQUE constraint on (scope, subject, transition_at).

Registration in api_v2.py:
    from ingest.transition_builder_adapter import TransitionBuilderAdapter
    scheduler.register(TransitionBuilderAdapter(db_path=db_path), interval_sec=86400)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)


class TransitionBuilderAdapter(BaseIngestAdapter):
    """
    Daily adapter that processes market_state_snapshots into state_transitions.

    Cycle:
    1. Build global transitions (scope='global', subject='market')
    2. For each watchlist ticker, build ticker transitions (scope='ticker')
    3. Emit a summary RawAtom for scheduler tracking
    """

    name = 'transition_builder'

    def __init__(self, db_path: str):
        super().__init__(name=self.name)
        self._db_path = db_path

    def _get_watchlist(self) -> List[str]:
        try:
            from ingest.dynamic_watchlist import DynamicWatchlistManager
            tickers = DynamicWatchlistManager.get_pattern_tickers(self._db_path)
            if tickers:
                return tickers
        except Exception:
            pass
        try:
            import sqlite3
            conn = sqlite3.connect(self._db_path, timeout=5)
            rows = conn.execute(
                """SELECT DISTINCT subject FROM market_state_snapshots
                   WHERE scope='ticker' ORDER BY subject"""
            ).fetchall()
            conn.close()
            return [r[0] for r in rows]
        except Exception:
            return []

    def fetch(self) -> List[RawAtom]:
        try:
            from analytics.state_transitions import TransitionEngine
        except ImportError as exc:
            _logger.warning('TransitionBuilderAdapter: import failed: %s', exc)
            return []

        engine = TransitionEngine(self._db_path)

        # Build global transitions
        global_written = 0
        try:
            global_written = engine.build_transitions(scope='global', subject='market')
            _logger.info('TransitionBuilderAdapter: global transitions written: %d', global_written)
        except Exception as exc:
            _logger.warning('TransitionBuilderAdapter: global build failed: %s', exc)

        # Build ticker transitions
        tickers = self._get_watchlist()
        ticker_written = 0
        ticker_errors  = 0
        for ticker in tickers:
            try:
                n = engine.build_transitions(scope='ticker', subject=ticker)
                ticker_written += n
            except Exception as exc:
                _logger.debug('TransitionBuilderAdapter: ticker %s failed: %s', ticker, exc)
                ticker_errors += 1

        _logger.info(
            'TransitionBuilderAdapter: done — %d tickers, %d global + %d ticker transitions, %d errors',
            len(tickers), global_written, ticker_written, ticker_errors,
        )

        now = datetime.now(timezone.utc).isoformat()
        return [
            RawAtom(
                subject   = 'system',
                predicate = 'transition_builder_last_run',
                object    = now,
                confidence= 1.0,
                source    = 'transition_builder_adapter',
                metadata  = {
                    'global_written':  str(global_written),
                    'ticker_written':  str(ticker_written),
                    'tickers_processed': str(len(tickers)),
                },
                upsert = True,
            )
        ]

    def transform(self, atoms: List[RawAtom]) -> List[RawAtom]:
        return atoms
