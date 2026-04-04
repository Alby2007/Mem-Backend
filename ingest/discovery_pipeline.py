"""
ingest/discovery_pipeline.py — Universal Discovery Pipeline

Single entry-point for on-demand ticker discovery and enrichment.
Triggered by: user chat query, manual /discover endpoint, scheduled staleness.
Always commits back to the shared KB — personal KB is untouched.

DESIGN
======
DiscoveryPipeline reuses the existing 12 ingest adapters rather than
re-implementing data-fetch logic. Each "stage" maps to an adapter class.
A single-ticker run is performed by constructing the adapter with a
`tickers=[ticker]` override and calling `.run_and_push(kg)`.

STALENESS
=========
Staleness is assessed by reading `timestamp` from the `facts` table
(NOT `updated_at` — that column does not exist in the shared KB schema).
Missing predicates are treated as maximally stale (float('inf') minutes).

COVERAGE EFFECT
===============
Every `discover()` call calls `DynamicWatchlist.add_tickers()` which
increments `coverage_count` and auto-promotes the ticker to scheduled
ingest when it reaches the promotion threshold (coverage >= 3, vol >= 500k).

STAGES
======
  price         — YFinanceAdapter single-ticker (last_price, regime, fundamentals)
  historical    — HistoricalBackfillAdapter (returns, vol, 52w levels)
  options       — OptionsAdapter (iv_rank, skew, put_call_ratio)
  enrichment    — SignalEnrichmentAdapter (signal_quality, macro_confirmation)
  patterns      — Pattern detection on 1d OHLCV (SMC zones, FVG, OB)
  short_interest— FCAShortInterestAdapter (.L tickers only)
  flow          — LSEFlowAdapter (.L tickers only)

LOGGING
=======
All discovery runs are logged to `discovery_log` table.
This gives product intelligence on which tickers are being discovered,
how often, and who triggered them — the most frequently discovered
tickers should become part of the default watchlist.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

_logger = logging.getLogger(__name__)

# ── Module-level caches ───────────────────────────────────────────────────────
# FCA XLSX is ~3MB — cache today's download so multiple .L ticker discoveries
# in the same day don't re-download the file each time.
_FCA_XLSX_CACHE: dict = {}   # { 'YYYY-MM-DD': DataFrame }

# ── Staleness thresholds (minutes) ────────────────────────────────────────────

STALENESS_THRESHOLDS: Dict[str, float] = {
    'last_price':           30.0,    # price goes stale fast
    'signal_direction':     60.0,
    'iv_rank':              60.0,
    'institutional_flow':   60.0,
    'catalyst':             120.0,   # news moves fast
    'short_interest_pct':   1440.0,  # FCA updates daily
    'earnings_date':        1440.0,
}

# Predicates that indicate a ticker has already been through each stage.
# If ANY of these is fresh, the stage is skipped.
_STAGE_GUARD_PREDICATES: Dict[str, str] = {
    'price':          'last_price',
    'historical':     'return_1y',
    'options':        'iv_rank',
    'enrichment':     'signal_quality',
    'patterns':       'last_price',   # patterns stored in pattern_signals, guard via price freshness
    'short_interest': 'short_interest_pct',
    'flow':           'institutional_flow',
}


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class DiscoveryResult:
    ticker:        str
    status:        str                    # 'fresh' | 'enriched' | 'partial' | 'failed'
    stages_run:    List[str] = field(default_factory=list)
    stages_skipped:List[str] = field(default_factory=list)
    atoms_written: int = 0
    duration_ms:   int = 0
    error:         Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_discovery_log(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS discovery_log (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker            TEXT NOT NULL,
            trigger           TEXT NOT NULL,
            stages_run        TEXT,
            atoms_written     INTEGER DEFAULT 0,
            duration_ms       INTEGER,
            triggered_by_user TEXT,
            created_at        TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def _assess_staleness(ticker: str, db_path: str) -> Dict[str, float]:
    """
    Return dict of predicate → age_in_minutes for predicates that exceed
    their staleness threshold (or are entirely missing from the KB).

    Uses the `timestamp` column of the `facts` table.
    """
    stale: Dict[str, float] = {}
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        rows = conn.execute(
            "SELECT predicate, timestamp FROM facts WHERE LOWER(subject) = ?",
            (ticker.lower(),),
        ).fetchall()
        conn.close()
    except Exception as e:
        _logger.warning('discovery: staleness check failed for %s: %s', ticker, e)
        return {p: float('inf') for p in STALENESS_THRESHOLDS}

    existing: Dict[str, float] = {}
    now = datetime.now(timezone.utc)

    for predicate, ts in rows:
        if predicate not in STALENESS_THRESHOLDS:
            continue
        if not ts:
            existing[predicate] = float('inf')
            continue
        try:
            # Handle both offset-aware and naive ISO strings
            ts_clean = ts.replace('Z', '+00:00') if ts.endswith('Z') else ts
            if '+' not in ts_clean and 'T' in ts_clean:
                ts_clean += '+00:00'
            dt = datetime.fromisoformat(ts_clean)
            if dt.tzinfo is None:
                from datetime import timezone as tz
                dt = dt.replace(tzinfo=tz.utc)
            age_minutes = (now - dt).total_seconds() / 60.0
            existing[predicate] = age_minutes
        except Exception:
            existing[predicate] = float('inf')

    for predicate, threshold in STALENESS_THRESHOLDS.items():
        age = existing.get(predicate, float('inf'))
        if age > threshold:
            stale[predicate] = age

    return stale


def _stages_needed(stale: Dict[str, float]) -> List[str]:
    """Map stale predicates → discovery stages that need to run."""
    needed = []
    stale_preds = set(stale.keys())

    stage_order = ['price', 'historical', 'options', 'enrichment',
                   'patterns', 'short_interest', 'flow']

    for stage in stage_order:
        guard = _STAGE_GUARD_PREDICATES.get(stage)
        if guard and guard in stale_preds:
            needed.append(stage)

    return needed


def _is_lse_ticker(ticker: str) -> bool:
    return ticker.upper().endswith('.L') or ticker.upper().endswith('-L')


# ── Discovery Pipeline ────────────────────────────────────────────────────────

class DiscoveryPipeline:
    """
    Universal ticker discovery and enrichment pipeline.

    Triggered by user queries, the /discover endpoint, or staleness detection.
    All enrichment is committed to the shared KB. Personal KB is never touched.
    """

    def __init__(self, kg, db_path: str = 'trading_knowledge.db'):
        self._kg      = kg
        self._db_path = db_path

    # ── Public API ────────────────────────────────────────────────────────────

    def assess_staleness(self, ticker: str) -> Dict[str, float]:
        """
        Public access to the staleness check.
        Returns dict of stale predicate → age_in_minutes.
        Empty dict means the ticker is fully fresh.
        """
        return _assess_staleness(ticker, self._db_path)

    def discover(
        self,
        ticker:  str,
        trigger: str = 'user_query',
        user_id: Optional[str] = None,
    ) -> DiscoveryResult:
        """
        Run the full discovery pipeline for a ticker.

        trigger: 'user_query' | 'staleness' | 'scheduled' | 'manual'
        user_id: set when triggered by a specific user session

        Returns DiscoveryResult immediately. Stages run sequentially;
        failures in one stage never block subsequent stages.
        """
        ticker_upper = ticker.upper().strip()
        t0 = time.monotonic()

        stale = _assess_staleness(ticker_upper, self._db_path)

        if not stale:
            _logger.debug('discovery: %s is fully fresh, skipping', ticker_upper)
            return DiscoveryResult(
                ticker=ticker_upper, status='fresh',
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        stages_needed = _stages_needed(stale)
        _logger.info(
            'discovery: %s trigger=%s stale=%s stages=%s',
            ticker_upper, trigger, list(stale.keys()), stages_needed,
        )

        if not stages_needed:
            return DiscoveryResult(
                ticker=ticker_upper, status='fresh',
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        result = DiscoveryResult(ticker=ticker_upper, status='enriched')

        for stage in stages_needed:
            # Skip LSE-specific stages for non-LSE tickers
            if stage in ('short_interest', 'flow') and not _is_lse_ticker(ticker_upper):
                result.stages_skipped.append(stage)
                continue

            try:
                atoms_before = self._fact_count(ticker_upper)
                self._run_stage(ticker_upper, stage)
                atoms_after  = self._fact_count(ticker_upper)
                delta = max(0, atoms_after - atoms_before)
                result.stages_run.append(stage)
                result.atoms_written += delta
                _logger.info(
                    'discovery: %s stage=%s +%d atoms', ticker_upper, stage, delta
                )
            except Exception as e:
                _logger.warning(
                    'discovery: %s stage=%s failed: %s', ticker_upper, stage, e
                )
                result.stages_skipped.append(stage)

        if not result.stages_run:
            result.status = 'failed'

        result.duration_ms = int((time.monotonic() - t0) * 1000)

        # Increment coverage_count — may promote ticker to scheduled ingest
        self._update_coverage(ticker_upper)

        # Log to discovery_log
        self._log_discovery(result, trigger, user_id)

        _logger.info(
            'discovery: %s done status=%s stages=%s atoms=%d ms=%d',
            ticker_upper, result.status, result.stages_run,
            result.atoms_written, result.duration_ms,
        )
        return result

    # ── Stage runners ─────────────────────────────────────────────────────────

    def _run_stage(self, ticker: str, stage: str) -> None:
        """Dispatch a single discovery stage for the given ticker."""
        if stage == 'price':
            self._run_price(ticker)
        elif stage == 'historical':
            self._run_historical(ticker)
        elif stage == 'options':
            self._run_options(ticker)
        elif stage == 'enrichment':
            self._run_enrichment(ticker)
        elif stage == 'patterns':
            self._run_patterns(ticker)
        elif stage == 'short_interest':
            self._run_short_interest(ticker)
        elif stage == 'flow':
            self._run_flow(ticker)

    def _run_price(self, ticker: str) -> None:
        from ingest.yfinance_adapter import YFinanceAdapter
        adapter = YFinanceAdapter(tickers=[ticker])
        adapter.run_and_push(self._kg)

    def _run_historical(self, ticker: str) -> None:
        from ingest.historical_adapter import HistoricalBackfillAdapter
        adapter = HistoricalBackfillAdapter(tickers=[ticker])
        adapter.run_and_push(self._kg)

    def _run_options(self, ticker: str) -> None:
        from ingest.options_adapter import OptionsAdapter
        adapter = OptionsAdapter(tickers=[ticker])
        adapter.run_and_push(self._kg)

    def _run_enrichment(self, ticker: str) -> None:
        from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
        adapter = SignalEnrichmentAdapter(db_path=self._db_path, tickers=[ticker])
        adapter.run_and_push(self._kg)

    def _run_patterns(self, ticker: str) -> None:
        """Run SMC pattern detection for a single ticker across all timeframes."""
        try:
            import sqlite3 as _sqlite3
            import yfinance as yf
            from analytics.pattern_detector import detect_all_patterns, OHLCV
            from knowledge.working_memory import _YF_TICKER_MAP as YF_MAP
        except ImportError as e:
            _logger.warning('discovery: patterns stage import failed: %s', e)
            return

        # Get KB context atoms for the ticker
        try:
            conn = _sqlite3.connect(self._db_path, timeout=5)
            kb_rows = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT predicate, object FROM facts WHERE LOWER(subject) = ? "
                    "AND predicate IN ('conviction_tier','price_regime','signal_direction')",
                    (ticker.lower(),),
                ).fetchall()
            }
            conn.close()
        except Exception:
            kb_rows = {}

        # Timeframe configs: (timeframe_label, yf_interval, yf_period)
        tf_configs = [
            ('15m', '15m', '5d'),
            ('1h',  '1h',  '30d'),
            ('4h',  '1h',  '60d'),  # yfinance has no 4h; fetch 1h and resample
            ('1d',  '1d',  '90d'),
        ]

        yf_sym = YF_MAP.get(ticker.upper(), ticker)
        all_signals = []

        for tf_label, yf_interval, yf_period in tf_configs:
            try:
                hist = yf.Ticker(yf_sym).history(period=yf_period, interval=yf_interval, auto_adjust=True)
                if hist.empty or len(hist) < 10:
                    continue
                candles = [
                    OHLCV(
                        timestamp=ts.isoformat(),
                        open=float(row['Open']),  high=float(row['High']),
                        low=float(row['Low']),    close=float(row['Close']),
                        volume=float(row.get('Volume', 0) or 0),
                    )
                    for ts, row in hist.iterrows()
                ]

                # Resample 1h → 4h if needed
                if tf_label == '4h' and len(candles) >= 4:
                    resampled = []
                    for i in range(0, len(candles) - 3, 4):
                        group = candles[i:i + 4]
                        resampled.append(OHLCV(
                            timestamp=group[0].timestamp,
                            open=group[0].open,
                            high=max(c.high for c in group),
                            low=min(c.low for c in group),
                            close=group[-1].close,
                            volume=sum(c.volume for c in group),
                        ))
                    candles = resampled

                if len(candles) < 3:
                    continue

                signals = detect_all_patterns(
                    candles, ticker=ticker.upper(), timeframe=tf_label,
                    kb_conviction=kb_rows.get('conviction_tier', ''),
                    kb_regime=kb_rows.get('price_regime', ''),
                    kb_signal_dir=kb_rows.get('signal_direction', ''),
                )
                all_signals.extend(signals)
            except Exception as e:
                _logger.debug('discovery: patterns %s/%s fetch failed: %s', ticker, tf_label, e)
                continue

        if not all_signals:
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            conn = _sqlite3.connect(self._db_path, timeout=10)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pattern_signals (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker        TEXT NOT NULL,
                    pattern_type  TEXT NOT NULL,
                    direction     TEXT NOT NULL,
                    zone_high     REAL NOT NULL,
                    zone_low      REAL NOT NULL,
                    zone_size_pct REAL,
                    timeframe     TEXT NOT NULL,
                    formed_at     TEXT,
                    status        TEXT NOT NULL DEFAULT 'open',
                    filled_at     TEXT,
                    quality_score REAL,
                    kb_conviction TEXT DEFAULT '',
                    kb_regime     TEXT DEFAULT '',
                    kb_signal_dir TEXT DEFAULT '',
                    alerted_users TEXT DEFAULT '[]',
                    detected_at   TEXT
                )
            """)
            inserted = 0
            for sig in all_signals:
                exists = conn.execute(
                    "SELECT 1 FROM pattern_signals WHERE ticker=? AND pattern_type=? "
                    "AND formed_at=? AND timeframe=?",
                    (sig.ticker, sig.pattern_type, sig.formed_at, sig.timeframe),
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    "INSERT INTO pattern_signals (ticker,pattern_type,direction,"
                    "zone_high,zone_low,zone_size_pct,timeframe,formed_at,"
                    "quality_score,kb_conviction,kb_regime,kb_signal_dir,detected_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (sig.ticker, sig.pattern_type, sig.direction,
                     sig.zone_high, sig.zone_low, sig.zone_size_pct,
                     sig.timeframe, sig.formed_at, sig.quality_score,
                     kb_rows.get('conviction_tier', ''),
                     kb_rows.get('price_regime', ''),
                     kb_rows.get('signal_direction', ''),
                     now_iso),
                )
                inserted += 1
            conn.commit()
            conn.close()
            _logger.info('discovery: %s patterns +%d new', ticker, inserted)
        except Exception as e:
            _logger.warning('discovery: patterns DB write failed %s: %s', ticker, e)

    def _run_short_interest(self, ticker: str) -> None:
        """
        Run FCA short interest for a single .L ticker.
        Downloads XLSX once, filters to just this ticker, pushes atoms.
        Avoids re-downloading the full file for each ticker in a multi-ticker
        discovery batch by using a module-level cache keyed on today's date.
        """
        from ingest.fca_short_interest_adapter import (
            FCAShortInterestAdapter, _resolve_ticker, _classify_squeeze,
            _cross_ref_tension, _cross_ref_signal, _FCA_XLSX_URL, _TIMEOUT, _SOURCE,
        )
        import io
        import requests
        from datetime import date

        cache_key = str(date.today())
        cached = _FCA_XLSX_CACHE.get(cache_key)
        if cached is None:
            try:
                import pandas as pd
                resp = requests.get(
                    _FCA_XLSX_URL, timeout=_TIMEOUT,
                    headers={'User-Agent': 'TradingGalaxyKB/1.0'},
                )
                resp.raise_for_status()
                all_sheets = pd.read_excel(io.BytesIO(resp.content), sheet_name=None)
                sheet = next((v for k, v in all_sheets.items() if 'Current' in k), None)
                if sheet is None:
                    return
                sheet.columns = ['holder', 'issuer', 'isin', 'short_pct', 'position_date']
                sheet = sheet.dropna(subset=['isin', 'issuer', 'short_pct'])
                import pandas as _pd
                sheet['short_pct'] = _pd.to_numeric(sheet['short_pct'], errors='coerce')
                sheet = sheet.dropna(subset=['short_pct'])
                cached = sheet
                _FCA_XLSX_CACHE[cache_key] = cached
                # Evict old cache entries
                for k in list(_FCA_XLSX_CACHE.keys()):
                    if k != cache_key:
                        del _FCA_XLSX_CACHE[k]
            except Exception as e:
                _logger.warning('discovery: FCA XLSX download failed: %s', e)
                return

        import pandas as pd
        # Find rows for this ticker by ISIN/name resolution
        matched_rows = []
        for _, row in cached.iterrows():
            if _resolve_ticker(str(row['isin']).strip(), str(row['issuer']).strip()) == ticker.upper():
                matched_rows.append(row)

        if not matched_rows:
            _logger.debug('discovery: FCA — no short interest data for %s', ticker)
            return

        from datetime import datetime, timezone
        from ingest.base import RawAtom

        total_pct    = sum(float(r['short_pct']) for r in matched_rows)
        holder_count = len({str(r['holder']) for r in matched_rows})
        latest_date  = max(str(r['position_date'])[:10] for r in matched_rows)
        now_iso      = datetime.now(timezone.utc).isoformat()
        source       = f'{_SOURCE}_{ticker.lower().replace(".", "_")}'
        meta         = {'fetched_at': now_iso, 'isin': '', 'issuer': ticker,
                        'position_date': latest_date, 'holder_count': holder_count}

        signal_dir = _cross_ref_signal(ticker, self._db_path)
        tension    = _cross_ref_tension(signal_dir, total_pct)
        atoms = [
            RawAtom(subject=ticker, predicate='short_interest_pct',
                    object=f'{total_pct:.1f}', confidence=0.85, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='short_interest_holders',
                    object=str(holder_count), confidence=0.90, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='short_squeeze_potential',
                    object=_classify_squeeze(total_pct), confidence=0.70, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='short_vs_signal',
                    object=tension, confidence=0.65, source=source,
                    metadata=meta, upsert=True),
        ]
        for atom in atoms:
            try:
                self._kg.add_fact(
                    subject=atom.subject, predicate=atom.predicate,
                    object=atom.object, confidence=atom.confidence,
                    source=atom.source, metadata=atom.metadata, upsert=atom.upsert,
                )
            except Exception as e:
                _logger.debug('discovery: short_interest atom write failed: %s', e)

    def _run_flow(self, ticker: str) -> None:
        from ingest.lse_flow_adapter import _fetch_candles, _compute_flow_signals
        from ingest.base import RawAtom

        candles = _fetch_candles(ticker)
        if not candles:
            return
        signals = _compute_flow_signals(candles)
        if not signals:
            return

        now_iso  = datetime.now(timezone.utc).isoformat()
        source   = f'alt_data_lse_flow_{ticker.lower().replace(".", "_")}'
        meta     = {'fetched_at': now_iso, 'ticker': ticker}
        atoms = [
            RawAtom(subject=ticker, predicate='institutional_flow',
                    object=signals['flow'], confidence=0.60, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='block_volume_ratio',
                    object=str(signals['bvr']), confidence=0.80, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='flow_conviction',
                    object=signals['conviction'], confidence=0.60, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='volume_trend_5d',
                    object=signals['volume_trend'], confidence=0.75, source=source,
                    metadata=meta, upsert=True),
            RawAtom(subject=ticker, predicate='price_range_compression',
                    object=signals['range_compression'], confidence=0.70, source=source,
                    metadata=meta, upsert=True),
        ]
        for atom in atoms:
            try:
                self._kg.add_fact(
                    subject=atom.subject, predicate=atom.predicate,
                    object=atom.object, confidence=atom.confidence,
                    source=atom.source, metadata=atom.metadata,
                    upsert=atom.upsert,
                )
            except Exception as e:
                _logger.debug('discovery: flow atom write failed %s: %s', ticker, e)

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _fact_count(self, ticker: str) -> int:
        """Count facts for this ticker in the KB."""
        try:
            conn = sqlite3.connect(self._db_path, timeout=3)
            n = conn.execute(
                "SELECT COUNT(*) FROM facts WHERE LOWER(subject) = ?",
                (ticker.lower(),),
            ).fetchone()[0]
            conn.close()
            return n
        except Exception:
            return 0

    def _update_coverage(self, ticker: str) -> None:
        """Increment coverage_count via DynamicWatchlistManager (handles promotion logic)."""
        try:
            from ingest.dynamic_watchlist import DynamicWatchlistManager
            DynamicWatchlistManager.add_tickers(
                tickers=[ticker],
                user_id='discovery_pipeline',
                db_path=self._db_path,
            )
        except Exception as e:
            _logger.debug('discovery: coverage update failed for %s: %s', ticker, e)

    def _log_discovery(
        self,
        result: DiscoveryResult,
        trigger: str,
        user_id: Optional[str],
    ) -> None:
        """Append a row to discovery_log."""
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            _ensure_discovery_log(conn)
            conn.execute(
                "INSERT INTO discovery_log "
                "(ticker, trigger, stages_run, atoms_written, duration_ms, triggered_by_user) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    result.ticker,
                    trigger,
                    json.dumps(result.stages_run),
                    result.atoms_written,
                    result.duration_ms,
                    user_id,
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            _logger.debug('discovery: log write failed: %s', e)
