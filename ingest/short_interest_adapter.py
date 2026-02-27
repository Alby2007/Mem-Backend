"""
ingest/short_interest_adapter.py — US Short Interest Adapter (FINRA)

Fetches biweekly consolidated short interest data from FINRA for US equities
and produces short squeeze potential atoms for the Trading KB.

SOURCE
======
  FINRA consolidated short interest files:
  https://cdn.finra.org/equity/regsho/biweekly/CNMSshvol{YYYYMMDD}.txt
  Tab-delimited. Updated biweekly (mid-month and end-of-month).
  No API key required.

  Settlement date index:
  https://cdn.finra.org/equity/regsho/biweekly/regsho-biweekly-index.html
  Scraped to find the most recent file date.

ATOMS PRODUCED
==============
  {TICKER} | short_interest_pct      | "18.4"   — short shares / avg_volume_30d
  {TICKER} | short_interest_shares   | "42000000"
  {TICKER} | days_to_cover           | "3.2"    — short_shares / avg_volume_30d
  {TICKER} | short_squeeze_potential | high | moderate | low | minimal
  {TICKER} | short_vs_signal         | tension | aligned | neutral
                                       tension = heavily short + KB signal is bullish
                                       aligned = heavily short + KB signal is bearish
                                       neutral = low short interest

SOURCE PREFIX
=============
  alt_data_finra_shorts  (authority 0.55, half-life 14d)

INTERVAL
========
  86400s (24h) — FINRA updates biweekly; daily check is cheap and ensures
  new files are picked up within 24h of publication.
"""

from __future__ import annotations

import io
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# FINRA Group short interest API — public, no key required
# Returns JSON with short interest per symbol for most recent settlement date
_FINRA_API_URL = (
    'https://api.finra.org/data/group/OTCMarket/name/consolidatedShortInterest'
    '?limit=5000&offset={offset}'
)
# FINRA also publishes via their public data portal (no auth)
_FINRA_PORTAL_URL = (
    'https://www.finra.org/finra-data/browse-catalog/equity-short-interest/data'
)
_TIMEOUT  = 30
_SOURCE   = 'alt_data_finra_shorts'

# Short squeeze potential thresholds (short_pct = short_shares / avg_volume_30d)
_SQUEEZE_HIGH     = 15.0   # >= 15% → high (classic squeeze territory)
_SQUEEZE_MODERATE =  8.0   # >=  8% → moderate
_SQUEEZE_LOW      =  3.0   # >=  3% → low
# Days-to-cover thresholds (short_shares / avg_volume_30d)
_DTC_HIGH         = 5.0    # >= 5 days → high squeeze via days-to-cover
_DTC_MODERATE     = 2.5    # >= 2.5 days → moderate

# Tickers to monitor
_DEFAULT_TICKERS = [
    # Portfolio holdings (highest short interest names)
    'COIN', 'HOOD', 'MSTR', 'PLTR', 'NVDA', 'ARKK', 'XYZ',
    # US mega-cap / high-conviction
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'MA', 'TSLA',
    'AMD', 'NFLX', 'CRM', 'SNOW', 'PYPL',
    # Macro proxies
    'SPY', 'QQQ', 'GLD', 'TLT', 'HYG',
]


def _fetch_finra_short_interest(tickers: List[str]) -> Dict[str, dict]:
    """
    Fetch short interest data from FINRA public API for specific tickers.
    Uses the FINRA equity short interest API (no auth required).
    Returns {ticker_upper: {'short_shares': int, 'settlement_date': str}}
    """
    ticker_set = {t.upper() for t in tickers}
    result: Dict[str, dict] = {}
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (compatible; TradingKB/1.0)',
    }

    for ticker in ticker_set:
        try:
            url = (
                f'https://api.finra.org/data/group/OTCMarket/name/consolidatedShortInterest'
                f'?limit=1&offset=0'
                f'&compareFilters=[{{"fieldName":"symbolCode","compareType":"EQUAL","fieldValue":"{ticker}"}}]'
                f'&sortFields=[{{"fieldName":"settlementDate","sortType":"DESC"}}]'
            )
            resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data:
                continue
            row = data[0] if isinstance(data, list) else data
            short_shares = int(row.get('shortInterestQty', 0) or 0)
            settle_date  = str(row.get('settlementDate', ''))
            if short_shares > 0:
                result[ticker] = {
                    'short_shares': short_shares,
                    'total_volume': 0,  # not provided by this endpoint
                    'date': settle_date,
                }
        except Exception as exc:
            _logger.debug('ShortInterestAdapter: API fetch failed for %s: %s', ticker, exc)
            continue

    return result


def _parse_finra_file(text: str, tickers: List[str]) -> Dict[str, dict]:
    """
    Parse FINRA consolidated short interest file (tab/pipe-delimited).
    Fallback: used only if file-based fetch is available.
    Returns {ticker_upper: {'short_shares': int, 'total_volume': int, 'date': str}}
    """
    ticker_set = {t.upper() for t in tickers}
    result: Dict[str, dict] = {}

    lines = text.splitlines()
    for line in lines[1:]:  # skip header
        parts = line.split('|')
        if len(parts) < 6:
            continue
        symbol = parts[1].strip().upper()
        if symbol not in ticker_set:
            continue
        try:
            short_vol = int(parts[2].strip())
            total_vol = int(parts[4].strip())
            date_str  = parts[5].strip()
        except (ValueError, IndexError):
            continue
        # Accumulate across markets (FINRA file has multiple market entries per symbol)
        if symbol not in result:
            result[symbol] = {'short_shares': 0, 'total_volume': 0, 'date': date_str}
        result[symbol]['short_shares'] += short_vol
        result[symbol]['total_volume'] += total_vol

    return result


def _get_avg_volume(ticker: str, db_path: Optional[str]) -> Optional[float]:
    """Read avg_volume_30d from the KB for this ticker."""
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='avg_volume_30d' "
            "ORDER BY confidence DESC LIMIT 1",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _get_kb_signal_direction(ticker: str, db_path: Optional[str]) -> Optional[str]:
    """Read signal_direction from the KB for this ticker."""
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='signal_direction' "
            "ORDER BY confidence DESC LIMIT 1",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        if row:
            return row[0].lower()
    except Exception:
        pass
    return None


def _classify_squeeze(short_pct: float, dtc: float) -> str:
    """Classify short squeeze potential from short_pct and days-to-cover."""
    if short_pct >= _SQUEEZE_HIGH or dtc >= _DTC_HIGH:
        return 'high'
    if short_pct >= _SQUEEZE_MODERATE or dtc >= _DTC_MODERATE:
        return 'moderate'
    if short_pct >= _SQUEEZE_LOW:
        return 'low'
    return 'minimal'


_BULLISH_DIRECTIONS = {'long', 'bullish', 'near_high', 'near_52w_high'}
_BEARISH_DIRECTIONS = {'short', 'bearish', 'near_low', 'near_52w_low'}


def _classify_short_vs_signal(squeeze: str, signal_direction: Optional[str]) -> str:
    """
    Cross-reference short squeeze potential with KB signal direction.
    tension = high short interest + bullish signal → potential squeeze catalyst
    aligned = high short interest + bearish signal → short thesis confirmed
    neutral = low short interest or no signal
    """
    if squeeze in ('high', 'moderate'):
        if signal_direction in _BULLISH_DIRECTIONS:
            return 'tension'
        if signal_direction in _BEARISH_DIRECTIONS:
            return 'aligned'
    return 'neutral'


class ShortInterestAdapter(BaseIngestAdapter):
    """
    Fetches FINRA biweekly consolidated short interest and produces
    short_interest_pct, days_to_cover, short_squeeze_potential atoms.
    """

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        db_path: Optional[str] = None,
    ):
        super().__init__(name='short_interest')
        self.tickers  = [t.upper() for t in (tickers or _DEFAULT_TICKERS)]
        self.db_path  = db_path

    def fetch(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []
        now = datetime.now(timezone.utc).isoformat()

        # Fetch via FINRA public API (per-ticker JSON, no file download needed)
        short_data = _fetch_finra_short_interest(self.tickers)
        if not short_data:
            _logger.info('ShortInterestAdapter: no short interest data returned from FINRA API')
            return atoms

        for ticker, data in short_data.items():
            ticker_l    = ticker.lower()
            short_shares = data['short_shares']
            total_volume = data['total_volume']
            file_date    = data['date']

            # Get avg_volume_30d from KB for DTC calculation
            avg_vol = _get_avg_volume(ticker, self.db_path) or total_volume or 1
            dtc     = short_shares / avg_vol if avg_vol > 0 else 0.0
            # short_pct as % of average daily volume (proxy for float %)
            short_pct = (short_shares / avg_vol * 100.0) if avg_vol > 0 else 0.0

            squeeze   = _classify_squeeze(short_pct, dtc)
            signal    = _get_kb_signal_direction(ticker, self.db_path)
            vs_signal = _classify_short_vs_signal(squeeze, signal)

            src  = f'{_SOURCE}_{ticker_l}'
            meta = {
                'as_of': now,
                'file_date': file_date,
                'short_shares': short_shares,
                'total_volume': total_volume,
                'avg_vol_30d': round(avg_vol),
            }
            conf = 0.55

            atoms += [
                RawAtom(subject=ticker_l, predicate='short_interest_shares',
                        object=str(short_shares), confidence=conf,
                        source=src, metadata=meta),
                RawAtom(subject=ticker_l, predicate='days_to_cover',
                        object=str(round(dtc, 1)), confidence=conf,
                        source=src, metadata=meta),
                RawAtom(subject=ticker_l, predicate='short_interest_pct',
                        object=str(round(short_pct, 1)), confidence=conf,
                        source=src, metadata=meta),
                RawAtom(subject=ticker_l, predicate='short_squeeze_potential',
                        object=squeeze, confidence=conf,
                        source=src, metadata=meta),
                RawAtom(subject=ticker_l, predicate='short_vs_signal',
                        object=vs_signal, confidence=conf,
                        source=src, metadata=meta),
            ]

            _logger.info(
                'ShortInterestAdapter: %s short_pct=%.1f%% dtc=%.1f squeeze=%s vs_signal=%s',
                ticker, short_pct, dtc, squeeze, vs_signal,
            )

        return atoms
