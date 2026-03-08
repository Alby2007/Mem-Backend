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

import csv
import io
import logging
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

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

# FINRA CDN base — two feeds: NASDAQ and NYSE consolidated
_FINRA_BASE = 'https://cdn.finra.org/equity/regsho/biweekly'

# Days-to-cover thresholds for squeeze risk classification
_DTC_HIGH     = 5.0
_DTC_MODERATE = 2.5
_DTC_LOW      = 1.0


def _candidate_dates(n: int = 6) -> List[str]:
    """
    Generate candidate settlement date strings (yyyymmdd).
    FINRA publishes biweekly on ~1st and ~15th of each month (settlement dates).
    We target those anchors ±3 days for the last n months, sorted newest-first.
    """
    from datetime import date as _date
    seen: set = set()
    dates: List[str] = []
    today = datetime.now(timezone.utc).date()

    # Go back n months, checking the ~1st and ~15th anchors
    for month_offset in range(n):
        month = today.month - month_offset
        year  = today.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        for anchor_day in (15, 1):
            try:
                anchor = _date(year, month, anchor_day)
            except ValueError:
                continue
            # ±3 days around anchor, newest first
            for offset in range(-3, 4):
                candidate = anchor + timedelta(days=offset)
                if candidate > today:
                    continue
                s = candidate.strftime('%Y%m%d')
                if s not in seen:
                    seen.add(s)
                    dates.append(s)

    # Sort newest-first so we hit the most recent file first
    dates.sort(reverse=True)
    return dates


def _fetch_finra_file(date_str: str) -> Optional[str]:
    """
    Try to download the FINRA short interest file for a given date string.
    Returns the raw text content or None if the file doesn't exist (404).
    Tries NASDAQ feed first, then NYSE.
    """
    for prefix in ('FNSQ', 'FNYX'):
        url = f'{_FINRA_BASE}/{prefix}{date_str}.txt'
        try:
            req = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'Accept': 'text/plain,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://www.finra.org/',
                },
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            _logger.debug('[finra_short] HTTP %d for %s', e.code, url)
        except Exception as exc:
            _logger.debug('[finra_short] fetch error %s: %s', url, exc)
    return None


def _parse_finra_text(text: str, tickers: frozenset) -> Dict[str, dict]:
    """
    Parse FINRA short interest tab-delimited text.
    Returns dict keyed by ticker with keys: short_interest, avg_daily_vol, days_to_cover.
    """
    results: Dict[str, dict] = {}
    try:
        reader = csv.DictReader(io.StringIO(text), delimiter='|')
        for row in reader:
            symbol = (row.get('Symbol') or row.get('symbol') or '').strip().upper()
            if symbol not in tickers:
                continue
            try:
                short_int = int(row.get('ShortInterest') or row.get('ShortVolume') or 0)
                avg_vol   = int(row.get('AvgDailyShareVolume') or row.get('TotalVolume') or 0)
                dtc_raw   = row.get('DaysToConvert') or row.get('DaysToCover') or ''
                try:
                    dtc = float(dtc_raw) if dtc_raw.strip() else (
                        round(short_int / avg_vol, 2) if avg_vol > 0 else None
                    )
                except (ValueError, TypeError):
                    dtc = round(short_int / avg_vol, 2) if avg_vol > 0 else None

                results[symbol] = {
                    'short_interest':  short_int,
                    'avg_daily_vol':   avg_vol,
                    'days_to_cover':   dtc,
                }
            except (ValueError, TypeError, KeyError):
                continue
    except Exception as exc:
        _logger.debug('[finra_short] parse error: %s', exc)
    return results


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
        # Find most recent available FINRA file
        text: Optional[str] = None
        file_date: Optional[str] = None

        for date_str in _candidate_dates(n=4):
            if self._last_date and date_str <= self._last_date:
                _logger.debug('[finra_short] %s <= last fetched %s — no new file', date_str, self._last_date)
                break
            text = _fetch_finra_file(date_str)
            if text:
                file_date = date_str
                break
            time.sleep(0.5)

        if not text or not file_date:
            _logger.info('[finra_short] no recent FINRA file found — will retry next cycle')
            return []

        self._last_date = file_date
        parsed = _parse_finra_text(text, self._tickers)

        if not parsed:
            _logger.info('[finra_short] file %s parsed but no watched tickers found', file_date)
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

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        for sym, data in parsed.items():
            short_int  = data['short_interest']
            avg_vol    = data['avg_daily_vol']
            dtc        = data['days_to_cover']
            direction  = signal_dirs.get(sym)

            squeeze    = _squeeze_risk(dtc, short_int)
            vs_sig     = _vs_signal(short_int, avg_vol, direction)

            def _a(pred: str, val: str) -> RawAtom:
                return RawAtom(
                    subject    = sym,
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
            '[finra_short] file=%s tickers=%d atoms=%d',
            file_date, len(parsed), len(atoms),
        )
        return atoms
