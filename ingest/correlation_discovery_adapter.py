"""
ingest/correlation_discovery_adapter.py — Scheduler wrapper for CorrelationDiscovery.
Registered in api_v2.py with interval_sec=86400 (once per day).
Silently accumulates until ≥20 snapshot pairs exist per ticker pair.
"""

from __future__ import annotations

import logging
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_log = logging.getLogger(__name__)


class CorrelationDiscoveryAdapter(BaseIngestAdapter):

    name = 'correlation_discovery_adapter'

    def __init__(self, db_path: str) -> None:
        super().__init__(self.name)
        self._db = db_path

    def fetch(self) -> List[RawAtom]:
        from analytics.correlation_discovery import CorrelationDiscovery
        cd = CorrelationDiscovery(self._db)
        results = cd.run()
        _log.info(
            'CorrelationDiscoveryAdapter: %d relationships from %d pairs tested',
            results.get('relationships_found', 0),
            results.get('pairs_tested', 0),
        )
        return []

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
