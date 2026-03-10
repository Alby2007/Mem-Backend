"""
ingest/thesis_generator_adapter.py — Scheduler wrapper for ThesisGenerator.
Registered in api_v2.py with interval_sec=21600 (every 6 hours).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from ingest.base import BaseIngestAdapter, RawAtom

_log = logging.getLogger(__name__)


class ThesisGeneratorAdapter(BaseIngestAdapter):

    name = 'thesis_generator_adapter'

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def fetch(self) -> List[RawAtom]:
        from analytics.thesis_generator import ThesisGenerator
        gen = ThesisGenerator(self._db)
        results = gen.run()
        atoms: List[RawAtom] = []
        for r in results:
            ticker = r['ticker'].lower()
            atoms.append(RawAtom(
                subject=ticker,
                predicate='auto_thesis',
                object=r['direction'],
                confidence=r['score'],
                source=self.name,
            ))
            atoms.append(RawAtom(
                subject=ticker,
                predicate='auto_thesis_score',
                object=str(r['score']),
                confidence=r['score'],
                source=self.name,
            ))
        _log.info('ThesisGeneratorAdapter: %d auto-theses generated', len(results))
        return atoms

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
