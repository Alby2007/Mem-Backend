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
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO',
    'JPM', 'V', 'MA', 'BAC', 'GS', 'MS',
    'UNH', 'JNJ', 'LLY', 'ABBV', 'PFE', 'MRK',
    'XOM', 'CVX', 'COP',
    'WMT', 'COST', 'MCD',
    'CAT', 'HON', 'RTX',
    'DIS', 'NFLX',
    'AMD', 'INTC', 'QCOM', 'CRM', 'ADBE', 'NOW',
    'BLK', 'SCHW', 'AXP',
    'NEE', 'AMT',
]

# Max parallel workers — stay well under SEC 10 req/s limit
_EDGAR_WORKERS = 6
_EDGAR_REQ_DELAY = 0.12  # seconds between requests per thread

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

    def __init__(self, tickers: Optional[List[str]] = None, db_path: Optional[str] = None):
        super().__init__(name='edgar')
        self.tickers = tickers or _DEFAULT_TICKERS
        self._db_path: Optional[str] = db_path

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'regulatory_filing_sec'

        # ── Pre-populate the CIK cache in one request (fast) ──────────────
        _ticker_to_cik(self.tickers[0])  # side-effect: fills _CIK_CACHE for all tickers

        # ── Parallel filing fetch ─────────────────────────────────────────
        all_atoms: List[RawAtom] = []
        all_queue: List[tuple] = []
        with ThreadPoolExecutor(
            max_workers=_EDGAR_WORKERS, thread_name_prefix='edgar'
        ) as ex:
            futures = {
                ex.submit(self._fetch_symbol, sym, source, now_iso): sym
                for sym in self.tickers
            }
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    sym_atoms, sym_queue = future.result(timeout=20)
                    all_atoms.extend(sym_atoms)
                    all_queue.extend(sym_queue)
                except Exception as e:
                    self._logger.warning('Failed to process %s: %s', sym, e)

        # extraction_queue writes disabled — dead storage, never consumed by LLM adapter

        return all_atoms

    def _fetch_symbol(
        self, symbol: str, source: str, now_iso: str
    ) -> Tuple[List[RawAtom], List[tuple]]:
        """Fetch filings for one ticker and convert to atoms.

        Returns (atoms, queue_rows) tuple.
        """
        atoms: List[RawAtom] = []
        queue_rows: List[tuple] = []
        cik = _ticker_to_cik(symbol)
        if not cik:
            self._logger.debug('No CIK found for %s', symbol)
            return atoms, queue_rows

        time.sleep(_EDGAR_REQ_DELAY)  # per-thread rate limit respect
        filings = _fetch_recent_filings(cik)
        for filing in filings:
            filing_atoms, filing_queue = self._filing_to_atoms(symbol, filing, source, now_iso)
            atoms.extend(filing_atoms)
            queue_rows.extend(filing_queue)
        return atoms, queue_rows

    def _filing_to_atoms(
        self,
        symbol: str,
        filing: dict,
        source: str,
        now_iso: str,
    ) -> Tuple[List[RawAtom], List[tuple]]:
        """Convert a single filing into one or more atoms.

        Returns (atoms, queue_rows) tuple.
        """
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

        # Queue the filing description for LLM extraction
        filing_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{filing.get('accession','').replace('-','')}/{filing.get('accession','')}-index.htm"
        )
        raw_text = f"{symbol} {form} filed {date}: {desc}"[:800]
        queue_rows = [(raw_text, filing_url, source, now_iso)]

        return atoms, queue_rows
