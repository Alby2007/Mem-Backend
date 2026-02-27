"""
ingest/sector_rotation_adapter.py — Sector Rotation Signal Adapter

Computes sector momentum and rotation signals from existing KB atoms.
No external API calls — reads return_1m atoms for sector ETFs already
in the KB and derives outperformance/underperformance vs SPY.

SOURCE
======
  Computed from KB atoms (return_1m for XLK, XLF, XLE, XLV, XLI, XLC,
  XLY, XLP, XLU, SPY). Requires HistoricalBackfillAdapter to have run
  for these tickers first.

ATOMS PRODUCED
==============
  market | sector_rotation_leader   | "technology"
  market | sector_rotation_laggard  | "utilities"
  market | risk_appetite            | risk_on | risk_off | neutral
  {ETF}  | sector_momentum          | outperforming | underperforming | inline
  {TICKER} | sector_tailwind        | positive | negative | neutral

SOURCE PREFIX
=============
  derived_signal_sector_rotation  (authority 0.65, half-life 7d)

INTERVAL
========
  3600s (1h) — recomputed hourly; sector returns update intraday
  but meaningful rotation only emerges over days/weeks.

SECTOR ETF → SECTOR NAME MAPPING
=================================
  XLK  → technology
  XLF  → financials
  XLE  → energy
  XLV  → healthcare
  XLI  → industrials
  XLC  → communication_services
  XLY  → consumer_discretionary
  XLP  → consumer_staples
  XLU  → utilities
  XLRE → real_estate
  XLB  → materials
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_SOURCE = 'derived_signal_sector_rotation'

# Sector ETF → sector name
_SECTOR_ETFS: Dict[str, str] = {
    'XLK':  'technology',
    'XLF':  'financials',
    'XLE':  'energy',
    'XLV':  'healthcare',
    'XLI':  'industrials',
    'XLC':  'communication_services',
    'XLY':  'consumer_discretionary',
    'XLP':  'consumer_staples',
    'XLU':  'utilities',
    'XLRE': 'real_estate',
    'XLB':  'materials',
}

# SPY return is the benchmark for excess return computation
_SPY = 'spy'

# Outperformance threshold (percentage points excess return vs SPY)
_OUTPERFORM_THRESHOLD  =  2.0   # >= +2pp vs SPY → outperforming
_UNDERPERFORM_THRESHOLD = -2.0  # <= -2pp vs SPY → underperforming

# Risk-on/off classification — based on relative sector performance
# Risk-on: XLK, XLY, XLF outperforming; risk-off: XLP, XLU, XLV leading
_RISK_ON_SECTORS  = {'technology', 'consumer_discretionary', 'financials', 'energy'}
_RISK_OFF_SECTORS = {'utilities', 'consumer_staples', 'healthcare', 'real_estate'}

# Ticker → sector mapping (for sector_tailwind computation)
# Built from yfinance sector atoms where available; hardcoded fallback
_TICKER_SECTOR_FALLBACK: Dict[str, str] = {
    'nvda': 'technology', 'aapl': 'technology', 'msft': 'technology',
    'googl': 'technology', 'meta': 'communication_services',
    'amzn': 'consumer_discretionary', 'coin': 'financials',
    'hood': 'financials', 'ma': 'financials', 'xyz': 'technology',
    'mstr': 'technology', 'pltr': 'technology',
    'arkk': 'technology',  # ARKK is tech-heavy ETF
    'gld': 'materials', 'slv': 'materials',
}


def _read_return_1m(db_path: str, ticker: str) -> Optional[float]:
    """Read the most recent return_1m atom for a ticker from the KB."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='return_1m' "
            "ORDER BY confidence DESC, timestamp DESC LIMIT 1",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _read_ticker_sector(db_path: str, ticker: str) -> Optional[str]:
    """Read the sector atom for a ticker from the KB."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='sector' "
            "ORDER BY confidence DESC LIMIT 1",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            val = row[0].lower()
            # Normalise: 'etf:technology' → 'technology'
            if ':' in val:
                val = val.split(':', 1)[1].strip()
            return val
    except Exception:
        pass
    return None


def _read_portfolio_tickers(db_path: str) -> List[str]:
    """Read all tickers that have a last_price atom (active universe)."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            "SELECT DISTINCT subject FROM facts WHERE predicate='last_price'"
        ).fetchall()
        conn.close()
        return [r[0] for r in rows if not r[0].endswith('.l')]  # US only
    except Exception:
        return []


class SectorRotationAdapter(BaseIngestAdapter):
    """
    Derives sector rotation signals from existing KB return_1m atoms.
    Produces market-level rotation leaders/laggards and per-ticker tailwinds.
    """

    def __init__(self, db_path: Optional[str] = None):
        super().__init__(name='sector_rotation')
        self.db_path = db_path

    def fetch(self) -> List[RawAtom]:
        if not self.db_path:
            _logger.warning('SectorRotationAdapter: db_path required, skipping')
            return []

        atoms: List[RawAtom] = []
        now  = datetime.now(timezone.utc).isoformat()
        meta = {'as_of': now}
        conf = 0.65

        # Read SPY 1m return as benchmark
        spy_return = _read_return_1m(self.db_path, _SPY)
        if spy_return is None:
            _logger.info('SectorRotationAdapter: SPY return_1m not in KB — run historical backfill first')
            return []

        # Compute excess returns for each sector ETF
        sector_excess: Dict[str, float] = {}   # sector_name → excess vs SPY
        etf_momentum:  Dict[str, str]   = {}   # ticker → outperforming/inline/underperforming

        available_sectors = 0
        for etf, sector in _SECTOR_ETFS.items():
            ret = _read_return_1m(self.db_path, etf)
            if ret is None:
                continue
            available_sectors += 1
            excess = ret - spy_return
            sector_excess[sector] = excess
            if excess >= _OUTPERFORM_THRESHOLD:
                momentum = 'outperforming'
            elif excess <= _UNDERPERFORM_THRESHOLD:
                momentum = 'underperforming'
            else:
                momentum = 'inline'
            etf_momentum[etf.lower()] = momentum

            atoms.append(RawAtom(
                subject=etf.lower(), predicate='sector_momentum', object=momentum,
                confidence=conf, source=_SOURCE,
                metadata={**meta, 'excess_return_1m': round(excess, 2), 'spy_return_1m': round(spy_return, 2)},
            ))

        if available_sectors < 3:
            _logger.info(
                'SectorRotationAdapter: only %d sector ETFs in KB — run historical backfill for XLK, XLF, XLE, XLV, XLI, XLC, XLY, XLP, XLU',
                available_sectors,
            )
            return atoms  # partial — emit what we have

        # Rank sectors by excess return
        ranked = sorted(sector_excess.items(), key=lambda x: x[1], reverse=True)
        leaders  = [s for s, _ in ranked[:2] if sector_excess[s] >= _OUTPERFORM_THRESHOLD]
        laggards = [s for s, _ in ranked[-2:] if sector_excess[s] <= _UNDERPERFORM_THRESHOLD]

        if leaders:
            atoms.append(RawAtom(
                subject='market', predicate='sector_rotation_leader',
                object=leaders[0], confidence=conf, source=_SOURCE, metadata=meta,
            ))
        if laggards:
            atoms.append(RawAtom(
                subject='market', predicate='sector_rotation_laggard',
                object=laggards[-1], confidence=conf, source=_SOURCE, metadata=meta,
            ))

        # Risk appetite: risk_on if cyclicals leading, risk_off if defensives leading
        leader_set  = set(leaders)
        laggard_set = set(laggards)
        risk_on_score  = len(leader_set & _RISK_ON_SECTORS)  - len(laggard_set & _RISK_ON_SECTORS)
        risk_off_score = len(leader_set & _RISK_OFF_SECTORS) - len(laggard_set & _RISK_OFF_SECTORS)

        if risk_on_score > 0 and risk_off_score <= 0:
            risk_appetite = 'risk_on'
        elif risk_off_score > 0 and risk_on_score <= 0:
            risk_appetite = 'risk_off'
        else:
            risk_appetite = 'neutral'

        atoms.append(RawAtom(
            subject='market', predicate='risk_appetite', object=risk_appetite,
            confidence=conf, source=_SOURCE, metadata=meta,
        ))

        # Per-ticker sector_tailwind
        active_tickers = _read_portfolio_tickers(self.db_path)
        for ticker in active_tickers:
            # Get sector from KB atom or fallback map
            sector = _read_ticker_sector(self.db_path, ticker)
            if not sector:
                sector = _TICKER_SECTOR_FALLBACK.get(ticker.lower())
            if not sector:
                continue

            excess = sector_excess.get(sector)
            if excess is None:
                continue

            if excess >= _OUTPERFORM_THRESHOLD:
                tailwind = 'positive'
            elif excess <= _UNDERPERFORM_THRESHOLD:
                tailwind = 'negative'
            else:
                tailwind = 'neutral'

            atoms.append(RawAtom(
                subject=ticker.lower(), predicate='sector_tailwind', object=tailwind,
                confidence=conf, source=_SOURCE,
                metadata={**meta, 'sector': sector, 'sector_excess_1m': round(excess, 2)},
            ))

        _logger.info(
            'SectorRotationAdapter: %d sector ETFs processed, risk_appetite=%s, leaders=%s, laggards=%s',
            available_sectors, risk_appetite, leaders, laggards,
        )

        return atoms
