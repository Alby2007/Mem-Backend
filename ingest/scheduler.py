"""
ingest/scheduler.py — Automated Ingest Scheduler (Trading KB)

Runs all registered ingest adapters on configurable intervals using
threading.Timer. Tracks health status (last run time, atom count, errors)
per adapter for monitoring.

Usage:
    from ingest.scheduler import IngestScheduler
    from knowledge import KnowledgeGraph

    kg = KnowledgeGraph()
    scheduler = IngestScheduler(kg)
    scheduler.start()          # non-blocking, runs in background threads
    scheduler.get_status()     # dict of per-adapter health
    scheduler.stop()           # graceful shutdown

The scheduler never blocks startup. If an adapter fails (missing API key,
rate limit, network error), it logs the error, records it in status, and
retries on the next interval. Other adapters are unaffected.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter

_logger = logging.getLogger(__name__)


# ── Per-adapter health record ─────────────────────────────────────────────────

@dataclass
class AdapterStatus:
    """Health record for a single adapter."""
    name:              str
    interval_sec:      float
    last_run_at:       Optional[str] = None
    last_success_at:   Optional[str] = None
    last_error:        Optional[str] = None
    last_error_at:     Optional[str] = None
    total_runs:        int = 0
    total_atoms:       int = 0
    total_errors:      int = 0
    is_running:        bool = False

    def to_dict(self) -> dict:
        return {
            'name':            self.name,
            'interval_sec':    self.interval_sec,
            'last_run_at':     self.last_run_at,
            'last_success_at': self.last_success_at,
            'last_error':      self.last_error,
            'last_error_at':   self.last_error_at,
            'total_runs':      self.total_runs,
            'total_atoms':     self.total_atoms,
            'total_errors':    self.total_errors,
            'is_running':      self.is_running,
        }


# ── Scheduler ─────────────────────────────────────────────────────────────────

class IngestScheduler:
    """
    Background scheduler that runs ingest adapters on fixed intervals.

    Each adapter gets its own timer thread. Failures in one adapter never
    affect others. Health status is queryable at any time via get_status().
    """

    def __init__(self, kg):
        """
        Args:
            kg: TradingKnowledgeGraph instance — adapters push atoms here.
        """
        self._kg = kg
        self._adapters: List[Tuple[BaseIngestAdapter, float]] = []
        self._status: Dict[str, AdapterStatus] = {}
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self._running = False

    def register(self, adapter: BaseIngestAdapter, interval_sec: float) -> None:
        """
        Register an adapter to run on a fixed interval.

        Args:
            adapter:      BaseIngestAdapter subclass instance
            interval_sec: seconds between runs (e.g. 900 = 15 min)
        """
        self._adapters.append((adapter, interval_sec))
        self._status[adapter.name] = AdapterStatus(
            name=adapter.name,
            interval_sec=interval_sec,
        )
        _logger.info(
            'Registered adapter %r (every %ds)', adapter.name, interval_sec
        )

    def start(self) -> None:
        """Start all registered adapters. Non-blocking."""
        if self._running:
            _logger.warning('Scheduler already running')
            return

        self._running = True
        _logger.info('Starting ingest scheduler with %d adapters', len(self._adapters))

        for adapter, interval_sec in self._adapters:
            # Run immediately on first start, then schedule repeats
            self._schedule(adapter, interval_sec, immediate=True)

    def stop(self) -> None:
        """Stop all timers gracefully."""
        self._running = False
        with self._lock:
            for name, timer in self._timers.items():
                timer.cancel()
                _logger.info('Stopped timer for %s', name)
            self._timers.clear()
        _logger.info('Ingest scheduler stopped')

    def get_status(self) -> Dict[str, dict]:
        """
        Return health status for all adapters.

        Returns:
            { 'adapter_name': { last_run_at, last_success_at, total_atoms, ... }, ... }
        """
        return {name: status.to_dict() for name, status in self._status.items()}

    def run_now(self, adapter_name: str) -> bool:
        """
        Trigger an out-of-schedule immediate run for a named adapter.

        Used by the domain_refresh_queue when decay_pressure is sustained and
        the EpistemicAdaptationEngine queues a refresh.

        Returns True if the adapter was found and dispatched, False otherwise.
        """
        target = None
        for adapter, interval_sec in self._adapters:
            if adapter.name == adapter_name:
                target = (adapter, interval_sec)
                break

        if target is None:
            _logger.warning('run_now: adapter %r not found', adapter_name)
            return False

        adapter, interval_sec = target
        if self._status[adapter_name].is_running:
            _logger.info('run_now: %r already running, skipping', adapter_name)
            return False

        thread = threading.Thread(
            target=self._run_adapter,
            args=(adapter, interval_sec),
            daemon=True,
            name=f'ingest-now-{adapter.name}',
        )
        thread.start()
        _logger.info('run_now: dispatched %r out-of-schedule', adapter_name)
        return True

    def _schedule(
        self,
        adapter: BaseIngestAdapter,
        interval_sec: float,
        immediate: bool = False,
    ) -> None:
        """Schedule the next run for an adapter."""
        if not self._running:
            return

        if immediate:
            # Run in a new thread to avoid blocking start()
            thread = threading.Thread(
                target=self._run_adapter,
                args=(adapter, interval_sec),
                daemon=True,
                name=f'ingest-{adapter.name}',
            )
            thread.start()
        else:
            timer = threading.Timer(
                interval_sec,
                self._run_adapter,
                args=(adapter, interval_sec),
            )
            timer.daemon = True
            timer.name = f'ingest-timer-{adapter.name}'
            with self._lock:
                self._timers[adapter.name] = timer
            timer.start()

    def _run_adapter(self, adapter: BaseIngestAdapter, interval_sec: float) -> None:
        """Execute a single adapter run, record status, schedule next."""
        status = self._status[adapter.name]
        now = datetime.now(timezone.utc).isoformat()
        status.is_running = True
        status.last_run_at = now
        status.total_runs += 1

        try:
            result = adapter.run_and_push(self._kg)
            ingested = result.get('ingested', 0)
            status.total_atoms += ingested
            status.last_success_at = now
            status.last_error = None
            _logger.info(
                '[%s] run #%d: ingested %d atoms (total: %d)',
                adapter.name, status.total_runs, ingested, status.total_atoms,
            )

        except Exception as e:
            status.total_errors += 1
            status.last_error = str(e)
            status.last_error_at = now
            _logger.error(
                '[%s] run #%d FAILED: %s (total errors: %d)',
                adapter.name, status.total_runs, e, status.total_errors,
            )

        finally:
            status.is_running = False
            # Schedule next run
            self._schedule(adapter, interval_sec, immediate=False)
