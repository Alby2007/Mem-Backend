"""
ingest/strategy_evolution_adapter.py — 6-hour scheduler wrapper for StrategyEvolution.

Runs evaluate() for every user who has active bots.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger('strategy_evolution_adapter')


class StrategyEvolutionAdapter(BaseIngestAdapter):
    name = 'strategy_evolution_adapter'
    interval_sec = 21600  # 6 hours

    def __init__(self, db_path: str):
        super().__init__(self.name)
        self.db_path = db_path

    def fetch(self) -> List[RawAtom]:
        try:
            from analytics.strategy_evolution import StrategyEvolution
            engine = StrategyEvolution(self.db_path)

            # Find all users with active bots
            conn = sqlite3.connect(self.db_path, timeout=10)
            rows = conn.execute(
                "SELECT DISTINCT user_id FROM paper_bot_configs WHERE active=1"
            ).fetchall()
            conn.close()

            for row in rows:
                user_id = row[0]
                try:
                    result = engine.evaluate(user_id)
                    _logger.info('[strategy_evolution] %s: %s', user_id, result)
                except Exception as e:
                    _logger.warning('[strategy_evolution] evaluate failed for %s: %s', user_id, e)

        except Exception as e:
            _logger.error('[strategy_evolution] adapter run error: %s', e)
            raise
        return []

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
