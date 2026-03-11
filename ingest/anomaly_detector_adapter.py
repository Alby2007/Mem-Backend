"""
ingest/anomaly_detector_adapter.py — Scheduler wrapper for AnomalyDetector.
Registered in api_v2.py with interval_sec=21600 (every 6 hours).
Run after state_snapshot_adapter so snapshots are fresh.
"""

from __future__ import annotations

import logging
from typing import List

from ingest.base import BaseIngestAdapter, RawAtom

_log = logging.getLogger(__name__)


class AnomalyDetectorAdapter(BaseIngestAdapter):

    name = 'anomaly_detector_adapter'

    def __init__(self, db_path: str) -> None:
        super().__init__(self.name)
        self._db = db_path

    def fetch(self) -> List[RawAtom]:
        from analytics.anomaly_detector import AnomalyDetector
        detector = AnomalyDetector(self._db)
        results = detector.run()
        atoms: List[RawAtom] = []
        for item in results.get('anomalous_tickers', []):
            ticker = item['ticker'].lower()
            sev = item['severity']
            atoms.append(RawAtom(
                subject=ticker,
                predicate='anomaly_detected',
                object='company_specific',
                confidence=sev,
                source=self.name,
            ))
            atoms.append(RawAtom(
                subject=ticker,
                predicate='anomaly_severity',
                object=str(sev),
                confidence=sev,
                source=self.name,
            ))
            atoms.append(RawAtom(
                subject=ticker,
                predicate='anomaly_description',
                object=item['description'],
                confidence=sev,
                source=self.name,
            ))
        if results.get('global_anomaly'):
            atoms.append(RawAtom(
                subject='market',
                predicate='global_anomaly',
                object='true',
                confidence=0.85,
                source=self.name,
            ))
        _log.info(
            'AnomalyDetectorAdapter: %d anomalous tickers, global=%s',
            len(results.get('anomalous_tickers', [])),
            results.get('global_anomaly', False),
        )
        return atoms

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
