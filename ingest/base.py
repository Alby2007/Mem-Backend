"""
ingest/base.py — Ingest Interface Contract (Trading KB)

This module defines the base class and atom schema that ALL ingest adapters
must implement. The ingest team builds concrete subclasses for each data feed.

INTERFACE CONTRACT
==================
Every ingest adapter must:
  1. Subclass BaseIngestAdapter
  2. Implement fetch() → List[RawAtom]
  3. Optionally override transform() for source-specific cleaning

An atom is the atomic unit of knowledge:
  (subject, predicate, object, confidence, source, metadata)

PREDICATE VOCABULARY
====================
Use predicates from knowledge/kb_domain_schemas.py. Key ones:

  Trading instruments:
    has_ticker, signal_direction, signal_confidence, signal_source,
    time_horizon, sector, price_target, catalyst, risk_factor,
    invalidation_condition, correlation_to, liquidity_profile, volatility_regime

  Derived signals (computed by SignalEnrichmentAdapter over existing atoms):
    price_regime        near_52w_high | mid_range | near_52w_low
    upside_pct          % upside to consensus target (negative = overextended)
    signal_quality      strong | confirmed | extended | conflicted | weak
    macro_confirmation  confirmed | partial | unconfirmed | no_data

  Historical summaries (computed by HistoricalBackfillAdapter from 1y OHLCV):
    return_1w / 1m / 3m / 6m / 1y    % return over standard windows
    volatility_30d / 90d              annualised realised vol (% per year)
    drawdown_from_52w_high            % drawdown from 52-week high close
    high_52w / low_52w                52-week high / low close levels
    price_6m_ago / price_1y_ago       anchoring reference prices
    avg_volume_30d                    mean daily volume (30 trading days)
    return_vs_spy_1m / 3m             excess return vs SPY benchmark

  Market theses:
    premise, supporting_evidence, contradicting_evidence,
    entry_condition, exit_condition, invalidated_by,
    confidence_level, risk_reward_ratio, position_sizing_note

  Macro regime:
    regime_label, dominant_driver, asset_class_bias, sector_rotation,
    risk_on_off, central_bank_stance, inflation_environment, growth_environment

  Companies:
    sector, market_cap_tier, revenue_trend, earnings_quality,
    management_assessment, competitive_moat, debt_profile, catalyst

  Research reports:
    publisher, analyst, rating, price_target, key_finding,
    compared_to_consensus, time_horizon

SOURCE NAMING CONVENTION
========================
Source strings are prefix-matched against the authority table in
knowledge/authority.py. Use the correct prefix for your feed type:

  exchange_feed_<exchange>_<symbol>   → authority 1.0, half-life ~10min
  regulatory_filing_<id>              → authority 0.95, half-life 1yr
  earnings_<ticker>_<quarter>         → authority 0.85, half-life 30d
  broker_research_<firm>_<date>       → authority 0.80, half-life 21d
  macro_data_<source>                 → authority 0.80, half-life 60d
  model_signal_<model_name>           → authority 0.70, half-life 12h
  technical_<indicator>_<symbol>      → authority 0.65, half-life 6h
  news_wire_<outlet>                  → authority 0.60, half-life 1d
  alt_data_<provider>                 → authority 0.55, half-life 3d
  social_signal_<platform>            → authority 0.35, half-life 12h
  curated_<analyst_id>                → authority 0.90, half-life 6mo

CONFIDENCE GUIDELINES
=====================
  1.0  — directly observed, unambiguous (e.g. price = X from exchange)
  0.9  — strongly supported by high-authority source
  0.8  — well-supported, minor interpretation required
  0.7  — model output or derived, reasonable confidence
  0.5  — uncertain, placeholder, or low-signal
  0.3  — speculative, conflicting evidence, or noisy source
"""

from __future__ import annotations

import abc
import logging
import sqlite3 as _sqlite3
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar

_F = TypeVar('_F', bound=Callable)


def db_connect(db_path: str, timeout: float = 30.0) -> _sqlite3.Connection:
    """
    Open a SQLite connection with WAL mode and a 30-second busy-timeout.
    Use this instead of sqlite3.connect() in every adapter to prevent
    'database is locked' errors when multiple adapters write concurrently.
    """
    conn = _sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn


def with_retry(
    max_attempts: int = 3,
    base_delay_sec: float = 2.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    exclude: tuple = (),
) -> Callable[[_F], _F]:
    """
    Decorator: retry the wrapped function up to max_attempts times with
    exponential back-off on any exception in `exceptions`.

    Delays: base_delay_sec, base_delay_sec*backoff_factor, ...
    Final failure re-raises the last exception.

    exclude: tuple of exception types that are NOT retried (re-raised immediately).
             Use to avoid retrying TimeoutError or other non-transient failures.

    Usage:
        @with_retry(max_attempts=3, base_delay_sec=2.0, exclude=(TimeoutError,))
        def fetch(self) -> List[RawAtom]: ...
    """
    def decorator(fn: _F) -> _F:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            _log = logging.getLogger(__name__)
            delay = base_delay_sec
            last_exc: BaseException = RuntimeError('no attempts made')
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if exclude and isinstance(exc, exclude):
                        raise
                    last_exc = exc
                    if attempt < max_attempts:
                        _log.warning(
                            '%s attempt %d/%d failed: %s — retrying in %.1fs',
                            fn.__qualname__, attempt, max_attempts, exc, delay,
                        )
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        _log.error(
                            '%s failed after %d attempts: %s',
                            fn.__qualname__, max_attempts, exc,
                        )
            raise last_exc
        return wrapper  # type: ignore[return-value]
    return decorator

logger = logging.getLogger(__name__)


# ── Atom schema ────────────────────────────────────────────────────────────────

@dataclass
class RawAtom:
    """
    The atomic unit of knowledge for ingest.

    subject:    the entity being described (e.g. 'AAPL', 'fed_rate_hike_2024')
    predicate:  the relationship type (e.g. 'signal_direction', 'catalyst')
    object:     the value or target (e.g. 'long', 'earnings_beat')
    confidence: epistemic confidence [0.0, 1.0]
    source:     source string — MUST use a recognised prefix (see above)
    metadata:   optional dict of extra fields (analyst, date, url, etc.)
    upsert:     if True, key on (subject, predicate, source) and update the
                existing row instead of inserting a new one.  Use for
                time-series predicates (last_price, price_target, signal_direction)
                so repeat ingest runs update rather than append.
    """
    subject:    str
    predicate:  str
    object:     str
    confidence: float = 0.5
    source:     str   = 'unverified_ingest'
    metadata:   Dict[str, Any] = field(default_factory=dict)
    upsert:     bool  = False

    def validate(self) -> List[str]:
        """Return list of validation errors. Empty = valid."""
        errors = []
        if not self.subject or not self.subject.strip():
            errors.append('subject is required')
        if not self.predicate or not self.predicate.strip():
            errors.append('predicate is required')
        if not self.object or not self.object.strip():
            errors.append('object is required')
        if not (0.0 <= self.confidence <= 1.0):
            errors.append(f'confidence must be in [0, 1], got {self.confidence}')
        if not self.source or not self.source.strip():
            errors.append('source is required')
        return errors


# ── Base adapter ───────────────────────────────────────────────────────────────

class BaseIngestAdapter(abc.ABC):
    """
    Abstract base class for all ingest adapters.

    Subclass this for each data feed. Implement fetch() and optionally
    override transform() for source-specific normalization.

    Usage:
        class MyFeedAdapter(BaseIngestAdapter):
            def fetch(self) -> List[RawAtom]:
                # pull from your data source
                return [RawAtom(subject='AAPL', predicate='signal_direction',
                                object='long', confidence=0.8,
                                source='model_signal_my_model')]

        adapter = MyFeedAdapter(name='my_feed')
        atoms = adapter.run()    # fetch + transform + validate
        adapter.push(atoms, kg)  # push to TradingKnowledgeGraph
    """

    def __init__(self, name: str):
        self.name = name
        self._logger = logging.getLogger(f'ingest.{name}')

    @abc.abstractmethod
    def fetch(self) -> List[RawAtom]:
        """
        Pull raw data from the source and return as RawAtom list.
        Implement all source-specific logic here.
        """
        ...

    def transform(self, atoms: List[RawAtom]) -> List[RawAtom]:
        """
        Optional post-processing hook. Override for source-specific cleaning.
        Default: identity (no transformation).
        """
        return atoms

    def run(self) -> List[RawAtom]:
        """
        Execute fetch → transform → validate pipeline.
        Invalid atoms are logged and dropped (never silently accepted).
        fetch() is wrapped with 3-attempt exponential back-off retry so
        transient network/API errors are handled transparently.
        """
        _fetch_with_retry = with_retry(
            max_attempts=3, base_delay_sec=2.0, backoff_factor=2.0,
            exceptions=(Exception,),
            exclude=(TimeoutError,),
        )(self.fetch)
        try:
            raw = _fetch_with_retry()
        except Exception as e:
            self._logger.error(f'fetch() failed after retries: {e}')
            return []

        transformed = self.transform(raw)

        valid = []
        for atom in transformed:
            errors = atom.validate()
            if errors:
                self._logger.warning(
                    f'Dropping invalid atom ({atom.subject!r} | {atom.predicate!r}): {errors}'
                )
            else:
                valid.append(atom)

        self._logger.info(f'run(): {len(raw)} fetched, {len(valid)} valid, '
                          f'{len(raw) - len(valid)} dropped')
        return valid

    def push(self, atoms: List[RawAtom], kg) -> Dict[str, int]:
        """
        Push validated atoms into a TradingKnowledgeGraph instance.

        Returns: {'ingested': N, 'skipped': M}
        """
        ingested = 0
        skipped = 0
        for atom in atoms:
            ok = kg.add_fact(
                subject=atom.subject,
                predicate=atom.predicate,
                object=atom.object,
                confidence=atom.confidence,
                source=atom.source,
                metadata=atom.metadata or None,
                upsert=atom.upsert,
            )
            if ok:
                ingested += 1
            else:
                skipped += 1

        self._logger.info(f'push(): ingested={ingested}, skipped={skipped}')
        return {'ingested': ingested, 'skipped': skipped}

    def run_and_push(self, kg) -> Dict[str, int]:
        """Convenience: run() then push() in one call."""
        atoms = self.run()
        if not atoms:
            return {'ingested': 0, 'skipped': 0}
        return self.push(atoms, kg)


# ── Example stub adapters (replace with real implementations) ──────────────────

class ExampleSignalAdapter(BaseIngestAdapter):
    """
    STUB — replace with your real signal feed.

    Expected output: instrument-level directional signals from your model.
    """

    def __init__(self):
        super().__init__(name='example_signal')

    def fetch(self) -> List[RawAtom]:
        # TODO: replace with real signal pull (e.g. from database, API, message queue)
        return [
            RawAtom(
                subject='AAPL',
                predicate='signal_direction',
                object='long',
                confidence=0.75,
                source='model_signal_example_v1',
                metadata={'model_version': '1.0', 'generated_at': '2024-01-01T09:00:00Z'},
            ),
        ]


class ExampleMacroAdapter(BaseIngestAdapter):
    """
    STUB — replace with your real macro data feed.

    Expected output: regime-level macro facts (rates, inflation, growth).
    """

    def __init__(self):
        super().__init__(name='example_macro')

    def fetch(self) -> List[RawAtom]:
        # TODO: replace with real macro data pull (e.g. FRED, central bank APIs)
        return [
            RawAtom(
                subject='global_macro_regime',
                predicate='regime_label',
                object='high rates, slowing growth',
                confidence=0.80,
                source='macro_data_internal',
                metadata={'as_of': '2024-01-01'},
            ),
            RawAtom(
                subject='global_macro_regime',
                predicate='central_bank_stance',
                object='restrictive',
                confidence=0.90,
                source='macro_data_internal',
            ),
        ]
