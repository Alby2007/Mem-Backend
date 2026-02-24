"""
ingest/edgar_adapter.py — SEC EDGAR Ingest Adapter (Trading KB)

Pulls recent SEC filings from the EDGAR full-text search API.
No API key required — SEC EDGAR is free and public.

Atoms produced:
  - {TICKER} | catalyst       | SEC filing: {form_type} — {description}
  - {TICKER} | risk_factor    | SEC 8-K: {event description}
  - {TICKER} | earnings_quality| SEC 10-Q/10-K filed on {date}

Source prefix: regulatory_filing_sec  (authority 0.95, half-life 1yr)
Interval: recommended 6h (SEC filings are not real-time)

EDGAR API rate limit: 10 requests/sec. We respect this with a per-request
sleep. The User-Agent header is required by SEC — set EDGAR_USER_AGENT
env var to 'YourName your@email.com' or the adapter uses a default.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# EDGAR EFTS (full-text search) API
_EFTS_BASE = 'https://efts.sec.gov/LATEST/search-index'
_SUBMISSIONS_BASE = 'https://data.sec.gov/submissions'

# Filing types we care about for trading atoms
_RELEVANT_FORMS = {
    '8-K':  'material_event',       # material events, earnings, M&A
    '10-Q': 'quarterly_report',     # quarterly financials
    '10-K': 'annual_report',        # annual financials
    '4':    'insider_transaction',   # insider buys/sells
    'SC 13D': 'activist_position',  # activist stake > 5%
    'SC 13G': 'passive_position',   # passive stake > 5%
}

# Default tickers to monitor
_DEFAULT_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
    'JPM', 'V', 'UNH', 'XOM', 'JNJ', 'WMT',
]

# CIK lookup cache (ticker → CIK)
_CIK_CACHE: Dict[str, str] = {}


def _get_headers() -> dict:
    """SEC requires a User-Agent with contact info."""
    agent = os.environ.get(
        'EDGAR_USER_AGENT',
        'TradingGalaxyKB admin@tradinggalaxy.dev',
    )
    return {
        'User-Agent': agent,
        'Accept': 'application/json',
    }


def _ticker_to_cik(ticker: str) -> Optional[str]:
    """Resolve a ticker symbol to a CIK number using SEC's company tickers JSON."""
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]

    try:
        resp = requests.get(
            'https://www.sec.gov/files/company_tickers.json',
            headers=_get_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        # Build reverse lookup: ticker → zero-padded CIK
        for entry in data.values():
            t = entry.get('ticker', '').upper()
            cik = str(entry.get('cik_str', '')).zfill(10)
            _CIK_CACHE[t] = cik

        return _CIK_CACHE.get(ticker)
    except Exception as e:
        _logger.warning('Failed to resolve CIK for %s: %s', ticker, e)
        return None


def _fetch_recent_filings(cik: str, max_filings: int = 10) -> List[dict]:
    """Fetch recent filings for a CIK from the submissions API."""
    try:
        url = f'{_SUBMISSIONS_BASE}/CIK{cik}.json'
        resp = requests.get(url, headers=_get_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()

        recent = data.get('filings', {}).get('recent', {})
        if not recent:
            return []

        forms = recent.get('form', [])
        dates = recent.get('filingDate', [])
        descriptions = recent.get('primaryDocDescription', [])
        accessions = recent.get('accessionNumber', [])

        filings = []
        for i in range(min(len(forms), max_filings)):
            form_type = forms[i] if i < len(forms) else ''
            if form_type not in _RELEVANT_FORMS:
                continue
            filings.append({
                'form': form_type,
                'date': dates[i] if i < len(dates) else '',
                'description': descriptions[i] if i < len(descriptions) else '',
                'accession': accessions[i] if i < len(accessions) else '',
            })

        return filings

    except Exception as e:
        _logger.warning('Failed to fetch filings for CIK %s: %s', cik, e)
        return []


class EDGARAdapter(BaseIngestAdapter):
    """
    SEC EDGAR ingest adapter.

    Pulls recent filings and converts to trading atoms.
    No API key required.
    """

    def __init__(self, tickers: Optional[List[str]] = None):
        super().__init__(name='edgar')
        self.tickers = tickers or _DEFAULT_TICKERS

    def fetch(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        source = 'regulatory_filing_sec'

        for symbol in self.tickers:
            try:
                cik = _ticker_to_cik(symbol)
                if not cik:
                    self._logger.debug('No CIK found for %s', symbol)
                    continue

                filings = _fetch_recent_filings(cik)
                for filing in filings:
                    atoms.extend(
                        self._filing_to_atoms(symbol, filing, source, now_iso)
                    )

                # Respect SEC rate limit (10 req/sec)
                time.sleep(0.15)

            except Exception as e:
                self._logger.warning('Failed to process %s: %s', symbol, e)

        return atoms

    def _filing_to_atoms(
        self,
        symbol: str,
        filing: dict,
        source: str,
        now_iso: str,
    ) -> List[RawAtom]:
        """Convert a single filing into one or more atoms."""
        atoms: List[RawAtom] = []
        form = filing['form']
        date = filing.get('date', '')
        desc = filing.get('description', '') or f'{form} filing'
        category = _RELEVANT_FORMS.get(form, 'filing')

        meta = {
            'form_type': form,
            'filing_date': date,
            'accession': filing.get('accession', ''),
            'fetched_at': now_iso,
        }

        # ── 8-K: material events → catalyst ──────────────────────────────
        if form == '8-K':
            atoms.append(RawAtom(
                subject=symbol,
                predicate='catalyst',
                object=f'SEC 8-K ({date}): {desc[:200]}',
                confidence=0.85,
                source=source,
                metadata=meta,
            ))

        # ── 10-Q / 10-K: financial reports → earnings quality ────────────
        elif form in ('10-Q', '10-K'):
            label = 'quarterly' if form == '10-Q' else 'annual'
            atoms.append(RawAtom(
                subject=symbol,
                predicate='earnings_quality',
                object=f'{label}_report_filed: {date}',
                confidence=0.90,
                source=source,
                metadata=meta,
            ))

        # ── Form 4: insider transactions → risk factor ───────────────────
        elif form == '4':
            atoms.append(RawAtom(
                subject=symbol,
                predicate='risk_factor',
                object=f'insider_transaction ({date}): {desc[:200]}',
                confidence=0.80,
                source=source,
                metadata=meta,
            ))

        # ── SC 13D/13G: activist / large stake → catalyst ───────────────
        elif form in ('SC 13D', 'SC 13G'):
            stake_type = 'activist' if form == 'SC 13D' else 'passive_large'
            atoms.append(RawAtom(
                subject=symbol,
                predicate='catalyst',
                object=f'{stake_type}_stake_disclosure ({date}): {desc[:200]}',
                confidence=0.85,
                source=source,
                metadata=meta,
            ))

        return atoms
