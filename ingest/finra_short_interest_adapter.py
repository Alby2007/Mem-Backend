"""
ingest/finra_short_interest_adapter.py — FINRA Short Interest Adapter (US)

Fetches biweekly FINRA consolidated short interest data for US equities and
produces short interest KB atoms.  No API key required.

SOURCE
======
  FINRA publishes short interest data for all US-listed equities twice a month
  (settlement dates ~1st and ~15th).  The consolidated file covers FINRA-member
  broker-dealer reported positions across all venues (NYSE, NASDAQ, OTC, etc.).

  File URL pattern (tab-delimited .txt):
    https://cdn.finra.org/equity/regsho/biweekly/FNSQyyyymmdd.txt  (NASDAQ)
    https://cdn.finra.org/equity/regsho/biweekly/FNYXyyyymmdd.txt  (NYSE)

  Columns (tab-separated, header row present):
    Symbol | Date | ShortInterest | AvgDailyShareVolume | DaysToConvert | ...

  Data is published ~3 business days after the settlement date.

  No API key required — public free data.

ATOMS PRODUCED
==============
  {TICKER} | short_interest          | "52340000"   — shares sold short
  {TICKER} | short_interest_pct_fl   | "4.7"        — short interest as % of float
                                                       (requires float from KB; omitted if unavailable)
  {TICKER} | days_to_cover           | "3.2"        — short interest / avg daily volume
                                                       (short squeeze timer proxy)
  {TICKER} | short_squeeze_risk      | high | moderate | low | minimal
  {TICKER} | short_vs_signal         | tension | aligned | neutral
                                                       (cross-ref with KB signal_direction)

SOURCE PREFIX: finra_short_interest (authority 0.65, lagging ~3 business days)

INTERVAL: 86400s — adapter checks daily but only downloads when a new file is
available (tracks last-seen date to avoid redundant downloads).

NOTES
=====
  - Days-to-cover > 5 with rising short interest = elevated squeeze risk
  - Days-to-cover < 1 = very liquid / easily covered — less squeeze potential
  - Combined with put_call_oi_ratio from PolygonOptionsAdapter: two independent
    bearish sentiment signals that reinforce each other
  - Combined with signal_direction from KB: short_vs_signal surfaces conflicts
    (e.g. heavy short interest on a bullish setup = confirm or reduce)
"""

from __future__ import annotations

import json as _json
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_AUTHORITY   = 0.65
_SOURCE_PFX  = 'finra_short_interest'

# US tickers tracked — must overlap with KB watchlist
_US_TICKERS = frozenset({
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA',
    'MA', 'JPM', 'BAC', 'GS', 'MS',
    'SPY', 'QQQ', 'IWM',
    'COIN', 'HOOD', 'MSTR', 'PLTR',
    'GLD', 'SLV', 'TLT', 'IEF',
})

# FINRA REST API — publicly accessible, no key required, no IP block
_FINRA_API = 'https://services.finra.org/api/v1/equity/short'

# Days-to-cover thresholds for squeeze risk classification
_DTC_HIGH     = 5.0
_DTC_MODERATE = 2.5
_DTC_LOW      = 1.0


def _fetch_finra_api(symbol: str) -> Optional[dict]:
    """
    Query FINRA REST API for the most recent short interest record for a symbol.
    Returns dict with short_interest, avg_daily_vol, days_to_cover or None on failure.
    API docs: https://www.finra.org/finra-data/browse-catalog/equity-short-interest
    """
    url = (
        f'{_FINRA_API}?fields=issueSymbolIdentifier,settlementDate,'
        f'shortInterestQty,averageDailyShareVolume,daysToCoverQty'
        f'&compareFilters=issueSymbolIdentifier+eq+{symbol}'
        f'&sortFields=-settlementDate&limit=1'
    )
    try:
        req = urllib.request.Request(
            url,
            headers={'Accept': 'application/json', 'User-Agent': 'TradingKB/1.0'},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = _json.loads(r.read().decode('utf-8', errors='replace'))
        if not data:
            return None
        row = data[0] if isinstance(data, list) else data
        short_int = int(row.get('shortInterestQty') or 0)
        avg_vol   = int(row.get('averageDailyShareVolume') or 0)
        dtc_raw   = row.get('daysToCoverQty')
        try:
            dtc = float(dtc_raw) if dtc_raw is not None else (
                round(short_int / avg_vol, 2) if avg_vol > 0 else None
            )
        except (ValueError, TypeError):
            dtc = round(short_int / avg_vol, 2) if avg_vol > 0 else None
        return {
            'short_interest': short_int,
            'avg_daily_vol':  avg_vol,
            'days_to_cover':  dtc,
            'settlement_date': row.get('settlementDate', ''),
        }
    except Exception as exc:
        _logger.debug('[finra_short] API error for %s: %s', symbol, exc)
        return None


def _squeeze_risk(dtc: Optional[float], short_int: int) -> str:
    if dtc is None:
        return 'unknown'
    if dtc >= _DTC_HIGH:
        return 'high'
    if dtc >= _DTC_MODERATE:
        return 'moderate'
    if dtc >= _DTC_LOW:
        return 'low'
    return 'minimal'


def _vs_signal(short_int: int, avg_vol: int, direction: Optional[str]) -> str:
    """
    Cross-reference short interest with KB signal direction.
    High short on a bullish signal = tension.
    High short on a bearish signal = aligned.
    """
    if not direction or avg_vol == 0:
        return 'neutral'
    dtc = short_int / avg_vol
    is_heavy = dtc >= _DTC_MODERATE
    if direction in ('bullish', 'long') and is_heavy:
        return 'tension'
    if direction in ('bearish', 'short') and is_heavy:
        return 'aligned'
    return 'neutral'


class FINRAShortInterestAdapter(BaseIngestAdapter):
    """
    Downloads FINRA biweekly short interest data and emits KB atoms for US tickers.
    No API key required.  Gracefully skips if no recent file is found.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        tickers: Optional[frozenset] = None,
    ):
        super().__init__(name='finra_short_interest')
        self._db_path  = db_path
        self._tickers  = tickers or _US_TICKERS
        self._last_date: Optional[str] = None  # track last-fetched date to skip re-downloads

    def fetch(self) -> List[RawAtom]:
        # Query FINRA REST API per symbol — no CDN/IP-block issues
        parsed: Dict[str, dict] = {}
        for sym in sorted(self._tickers):
            data = _fetch_finra_api(sym)
            if data and data['short_interest'] > 0:
                parsed[sym] = data
            time.sleep(1.0)  # ~24 tickers × 1s = ~24s total, well within 86400s interval

        if not parsed:
            _logger.info('[finra_short] FINRA API returned no data for any watched ticker')
            return []

        # Optionally look up signal_direction from KB for vs_signal cross-ref
        signal_dirs: Dict[str, str] = {}
        if self._db_path:
            try:
                import sqlite3 as _sq
                _c = _sq.connect(self._db_path, timeout=5)
                for sym in parsed:
                    row = _c.execute(
                        "SELECT object FROM facts WHERE LOWER(subject)=?"
                        " AND predicate='signal_direction' ORDER BY confidence DESC LIMIT 1",
                        (sym.lower(),)
                    ).fetchone()
                    if row:
                        signal_dirs[sym] = row[0]
                _c.close()
            except Exception:
                pass

        atoms: List[RawAtom] = []
        latest_date = ''

        for sym, data in parsed.items():
            short_int  = data['short_interest']
            avg_vol    = data['avg_daily_vol']
            dtc        = data['days_to_cover']
            direction  = signal_dirs.get(sym)
            if data.get('settlement_date', '') > latest_date:
                latest_date = data['settlement_date']

            squeeze    = _squeeze_risk(dtc, short_int)
            vs_sig     = _vs_signal(short_int, avg_vol, direction)

            def _a(pred: str, val: str, _sym: str = sym) -> RawAtom:
                return RawAtom(
                    subject    = _sym,
                    predicate  = pred,
                    object     = val,
                    source     = _SOURCE_PFX,
                    confidence = _AUTHORITY,
                    upsert     = True,
                )

            atoms.append(_a('short_interest',     str(short_int)))
            if dtc is not None:
                atoms.append(_a('days_to_cover',  f'{dtc:.2f}'))
            atoms.append(_a('short_squeeze_risk', squeeze))
            atoms.append(_a('short_vs_signal',    vs_sig))

        _logger.info(
            '[finra_short] settlement=%s tickers=%d atoms=%d',
            latest_date, len(parsed), len(atoms),
        )
        return atoms
