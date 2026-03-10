"""
ingest/signal_decay_adapter.py — Scheduler wrapper for SignalDecayPredictor.
Registered in api_v2.py with interval_sec=21600 (every 6 hours).
"""

from __future__ import annotations

import logging
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_log = logging.getLogger(__name__)


class SignalDecayAdapter(BaseIngestAdapter):

    name = 'signal_decay_adapter'

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def fetch(self) -> List[RawAtom]:
        from analytics.signal_decay_predictor import SignalDecayPredictor
        sdp = SignalDecayPredictor(self._db)
        results = sdp.run()
        _log.info(
            'SignalDecayAdapter: %d patterns → %d expired',
            results.get('patterns_processed', 0),
            results.get('expired_count', 0),
        )
        return []

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
