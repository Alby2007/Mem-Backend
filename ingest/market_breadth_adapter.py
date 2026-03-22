"""
ingest/market_breadth_adapter.py — Market Breadth Adapter

Computes market-wide breadth indicators from ohlcv_cache across all tickers
and writes them as KB atoms under subject='market_breadth_global'.

Runs every 900s (registered in api_v2.py).

Predicates produced:
  - advance_decline_ratio : numeric (>1 = more advancers)
  - breadth_regime        : 'strong' | 'weak' | 'neutral'
  - pct_above_sma20       : numeric 0-100 (% of tickers closing above 20d SMA)
  - breadth_thrust         : 'bullish' | 'bearish' | 'none'
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

try:
    from db import HAS_POSTGRES, get_pg
except ImportError:
    HAS_POSTGRES = False
    get_pg = None  # type: ignore


class MarketBreadthAdapter(BaseIngestAdapter):
    """Compute market breadth indicators from ohlcv_cache."""

    name = "market_breadth"

    def __init__(self, db_path: str):
        super().__init__(self.name)
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        rows = self._get_ohlcv(cutoff)
        if not rows:
            _logger.debug("market_breadth_adapter: no OHLCV rows")
            return []

        # Group by ticker → list of closes (ordered by ts)
        ticker_closes: Dict[str, List[float]] = {}
        for ticker, close_val in rows:
            t = ticker.upper()
            if t not in ticker_closes:
                ticker_closes[t] = []
            ticker_closes[t].append(float(close_val))

        if len(ticker_closes) < 5:
            _logger.debug("market_breadth_adapter: too few tickers (%d)", len(ticker_closes))
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        src = "market_breadth_adapter"
        atoms: List[RawAtom] = []
        subj = "market_breadth_global"

        # ── Advance / Decline ─────────────────────────────────────────────
        advancers = 0
        decliners = 0
        above_sma20 = 0
        total_with_sma = 0

        for ticker, closes in ticker_closes.items():
            if len(closes) < 2:
                continue
            if closes[-1] > closes[-2]:
                advancers += 1
            elif closes[-1] < closes[-2]:
                decliners += 1

            # % above 20d SMA
            if len(closes) >= 20:
                sma20 = sum(closes[-20:]) / 20
                total_with_sma += 1
                if closes[-1] > sma20:
                    above_sma20 += 1

        # A/D ratio
        ad_ratio = round(advancers / max(decliners, 1), 3)
        atoms.append(RawAtom(
            subject=subj, predicate='advance_decline_ratio',
            object=str(ad_ratio), confidence=0.90,
            source=src, metadata={'advancers': advancers, 'decliners': decliners,
                                  'as_of': now_iso},
            upsert=True,
        ))

        # Breadth regime
        if ad_ratio > 1.5:
            regime = 'strong'
        elif ad_ratio < 0.67:
            regime = 'weak'
        else:
            regime = 'neutral'
        atoms.append(RawAtom(
            subject=subj, predicate='breadth_regime',
            object=regime, confidence=0.85,
            source=src, metadata={'ad_ratio': ad_ratio, 'as_of': now_iso},
            upsert=True,
        ))

        # % above SMA20
        if total_with_sma > 0:
            pct = round(above_sma20 / total_with_sma * 100, 1)
            atoms.append(RawAtom(
                subject=subj, predicate='pct_above_sma20',
                object=str(pct), confidence=0.90,
                source=src, metadata={'n': total_with_sma, 'as_of': now_iso},
                upsert=True,
            ))

            # Breadth thrust: >80% above SMA = bullish, <20% = bearish
            if pct > 80:
                thrust = 'bullish'
            elif pct < 20:
                thrust = 'bearish'
            else:
                thrust = 'none'
            atoms.append(RawAtom(
                subject=subj, predicate='breadth_thrust',
                object=thrust, confidence=0.85,
                source=src, metadata={'pct_above_sma20': pct, 'as_of': now_iso},
                upsert=True,
            ))

        _logger.info("market_breadth_adapter: produced %d atoms (%d tickers)",
                      len(atoms), len(ticker_closes))
        return atoms

    def transform(self, raw):
        return raw if isinstance(raw, list) else []

    # ── OHLCV fetch (PG-first) ────────────────────────────────────────────────

    def _get_ohlcv(self, cutoff: str):
        """Return (ticker, close) rows ordered by ticker, ts."""
        rows = []
        if HAS_POSTGRES and get_pg:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute(
                        "SELECT ticker, close FROM ohlcv_cache "
                        "WHERE interval='1d' AND ts >= %s AND close IS NOT NULL "
                        "ORDER BY ticker, ts ASC", (cutoff,))
                    rows = [(r['ticker'], r['close']) for r in cur.fetchall()]
            except Exception as e:
                _logger.warning("market_breadth_adapter: PG query failed: %s", e)
                rows = []
        if not rows:
            try:
                conn = sqlite3.connect(self._db_path, timeout=15)
                conn.execute('PRAGMA journal_mode=WAL')
                rows = conn.execute(
                    """SELECT ticker, close FROM ohlcv_cache
                       WHERE interval='1d' AND ts >= ? AND close IS NOT NULL
                       ORDER BY ticker, ts ASC""",
                    (cutoff,),
                ).fetchall()
                conn.close()
            except Exception as e:
                _logger.warning("market_breadth_adapter: SQLite query failed: %s", e)
        return rows
