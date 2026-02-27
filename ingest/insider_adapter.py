"""
ingest/insider_adapter.py — Form 4 Insider Transaction Adapter

Fetches Form 4 (insider buy/sell) filings from SEC EDGAR for portfolio
holdings and produces conviction-weighted insider direction atoms.

SOURCE
======
  https://data.sec.gov/submissions/CIK{cik}.json
  SEC requires User-Agent header. No API key needed.
  Form 4 = Statement of Changes in Beneficial Ownership
  Transaction codes: P=Purchase, S=Sale, A=Award/Grant, D=Disposition

ATOMS PRODUCED
==============
  {TICKER} | insider_direction   | buy | sell | mixed | none
  {TICKER} | insider_value_usd   | "4200000"  — rolling 30d net $ value
  {TICKER} | insider_role        | ceo | cfo | director | officer | major_holder
  {TICKER} | insider_conviction  | high | moderate | low | none
                                   high     >= $500k net buy in 30d
                                   moderate >= $100k net buy in 30d
                                   low      >= $10k  net buy in 30d
                                   none     < $10k or net sell

SOURCE PREFIX
=============
  regulatory_filing_sec_form4  (authority 0.90, half-life 30d)
  Insider purchases are high-authority directional signals.
  Sales are weaker (options exercise, diversification) — confidence 0.70.

INTERVAL
========
  3600s (1h) — Form 4s must be filed within 2 business days of transaction.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_SEC_SUBMISSIONS_BASE = 'https://data.sec.gov/submissions'
_SEC_CIK_SEARCH       = 'https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt={start}&enddt={end}&forms=4'
_TIMEOUT   = 20
_SOURCE_BUY  = 'regulatory_filing_sec_form4'
_SOURCE_SELL = 'regulatory_filing_sec_form4_sale'

_USER_AGENT = os.environ.get(
    'EDGAR_USER_AGENT',
    'TradingGalaxyKB admin@tradinggalaxy.dev',
)

# Rolling window for summing insider transactions
_LOOKBACK_DAYS = 30

# $ value thresholds for conviction classification
_HIGH_THRESHOLD     = 500_000
_MODERATE_THRESHOLD = 100_000
_LOW_THRESHOLD      =  10_000

# Transaction codes: open-market purchases only for strong signal
_BUY_CODES  = {'P'}        # P = open-market purchase (strongest signal)
_SELL_CODES = {'S', 'D'}   # S = open-market sale, D = disposition

# Role classification from SEC relationship flags
_ROLE_MAP = {
    'isOfficer': 'officer',
    'isDirector': 'director',
    'isTenPercentOwner': 'major_holder',
}

# Tickers to monitor — portfolio holdings + EDGAR default list
_DEFAULT_TICKERS = [
    # Portfolio holdings
    'COIN', 'HOOD', 'MSTR', 'PLTR', 'NVDA', 'ARKK', 'XYZ',
    # US mega-cap
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'MA',
    'TSLA', 'AVGO', 'JPM', 'V', 'BAC', 'GS',
    # High-conviction watchlist
    'AMD', 'CRM', 'SNOW', 'PYPL', 'NFLX',
]

# CIK lookup cache (populated from SEC EDGAR ticker→CIK map)
_CIK_CACHE: Dict[str, str] = {}

# SEC full ticker-to-CIK map URL
_TICKERS_JSON_URL = 'https://www.sec.gov/files/company_tickers.json'


def _load_cik_map() -> Dict[str, str]:
    """Download SEC company_tickers.json and return {ticker: cik_padded}."""
    try:
        resp = requests.get(
            _TICKERS_JSON_URL,
            headers={'User-Agent': _USER_AGENT},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        result: Dict[str, str] = {}
        for entry in data.values():
            ticker = entry.get('ticker', '').upper()
            cik    = str(entry.get('cik_str', '')).zfill(10)
            if ticker and cik:
                result[ticker] = cik
        return result
    except Exception as exc:
        _logger.warning('InsiderAdapter: CIK map load failed: %s', exc)
        return {}


def _get_cik(ticker: str) -> Optional[str]:
    """Return zero-padded CIK for ticker, loading map if needed."""
    global _CIK_CACHE
    if not _CIK_CACHE:
        _CIK_CACHE = _load_cik_map()
    return _CIK_CACHE.get(ticker.upper())


def _fetch_form4_filings(cik: str, lookback_days: int) -> List[dict]:
    """
    Fetch recent Form 4 filings for a CIK from the submissions endpoint.
    Returns list of filing dicts with date, transactionCode, value, role.
    """
    url = f'{_SEC_SUBMISSIONS_BASE}/CIK{cik}.json'
    try:
        resp = requests.get(
            url,
            headers={'User-Agent': _USER_AGENT},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        _logger.debug('InsiderAdapter: submissions fetch failed for CIK %s: %s', cik, exc)
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    filings = []

    recent = data.get('filings', {}).get('recent', {})
    forms       = recent.get('form', [])
    dates       = recent.get('filingDate', [])
    accessions  = recent.get('accessionNumber', [])

    for i, form in enumerate(forms):
        if form != '4':
            continue
        date_str = dates[i] if i < len(dates) else ''
        try:
            filing_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if filing_dt < cutoff:
            # Filings are roughly newest-first; once we go past lookback, break
            break
        filings.append({
            'date': date_str,
            'accession': accessions[i] if i < len(accessions) else '',
        })

    return filings


def _parse_form4_document(cik: str, accession: str) -> List[dict]:
    """
    Fetch the primary Form 4 XML document and extract transactions.
    Returns list of {code, value_usd, role} dicts.
    """
    acc_clean = accession.replace('-', '')
    base_url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/'
    idx_url  = base_url + f'{accession}-index.json'

    try:
        resp = requests.get(
            idx_url,
            headers={'User-Agent': _USER_AGENT},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        idx = resp.json()
        # Find the primary XML document
        xml_file = None
        for f in idx.get('directory', {}).get('item', []):
            if f.get('type') == '4' and f.get('name', '').endswith('.xml'):
                xml_file = f['name']
                break
        if not xml_file:
            return []
        xml_url = base_url + xml_file
    except Exception:
        return []

    try:
        import xml.etree.ElementTree as ET
        resp2 = requests.get(
            xml_url,
            headers={'User-Agent': _USER_AGENT},
            timeout=_TIMEOUT,
        )
        resp2.raise_for_status()
        root = ET.fromstring(resp2.text)
    except Exception:
        return []

    transactions = []

    # Determine role
    role = 'officer'
    rpt = root.find('.//reportingOwner/reportingOwnerRelationship')
    if rpt is not None:
        if rpt.findtext('isDirector') == '1':
            role = 'director'
        elif rpt.findtext('isTenPercentOwner') == '1':
            role = 'major_holder'
        elif rpt.findtext('isOfficer') == '1':
            title = (rpt.findtext('officerTitle') or '').lower()
            if 'chief executive' in title or ' ceo' in title:
                role = 'ceo'
            elif 'chief financial' in title or ' cfo' in title:
                role = 'cfo'
            else:
                role = 'officer'

    # Parse non-derivative transactions
    for txn in root.findall('.//nonDerivativeTransaction'):
        code_el  = txn.find('transactionCoding/transactionCode')
        shares_el = txn.find('transactionAmounts/transactionShares/value')
        price_el  = txn.find('transactionAmounts/transactionPricePerShare/value')
        if code_el is None:
            continue
        code = (code_el.text or '').strip().upper()
        if code not in _BUY_CODES and code not in _SELL_CODES:
            continue
        try:
            shares = float(shares_el.text) if shares_el is not None and shares_el.text else 0.0
            price  = float(price_el.text)  if price_el  is not None and price_el.text  else 0.0
            value  = shares * price
        except ValueError:
            continue
        transactions.append({'code': code, 'value_usd': value, 'role': role})

    return transactions


def _classify_conviction(net_buy_usd: float) -> str:
    if net_buy_usd >= _HIGH_THRESHOLD:
        return 'high'
    if net_buy_usd >= _MODERATE_THRESHOLD:
        return 'moderate'
    if net_buy_usd >= _LOW_THRESHOLD:
        return 'low'
    return 'none'


def _classify_role_priority(roles: List[str]) -> str:
    """Return highest-authority role seen across all transactions."""
    priority = ['ceo', 'cfo', 'major_holder', 'director', 'officer']
    for p in priority:
        if p in roles:
            return p
    return roles[0] if roles else 'officer'


class InsiderAdapter(BaseIngestAdapter):
    """
    Fetches Form 4 insider transactions from SEC EDGAR and produces
    insider_direction, insider_value_usd, insider_role, insider_conviction atoms.
    """

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        db_path: Optional[str] = None,
        lookback_days: int = _LOOKBACK_DAYS,
    ):
        super().__init__(name='insider_transactions')
        self.tickers       = tickers or _DEFAULT_TICKERS
        self.db_path       = db_path
        self.lookback_days = lookback_days

    def fetch(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []
        now   = datetime.now(timezone.utc).isoformat()

        for ticker in self.tickers:
            cik = _get_cik(ticker)
            if not cik:
                _logger.debug('InsiderAdapter: no CIK for %s', ticker)
                continue

            filings = _fetch_form4_filings(cik, self.lookback_days)
            if not filings:
                continue

            buy_usd  = 0.0
            sell_usd = 0.0
            roles: List[str] = []

            for filing in filings[:10]:  # cap at 10 filings per ticker per run
                txns = _parse_form4_document(cik, filing['accession'])
                for txn in txns:
                    if txn['code'] in _BUY_CODES:
                        buy_usd += txn['value_usd']
                    else:
                        sell_usd += txn['value_usd']
                    roles.append(txn['role'])
                time.sleep(0.15)  # respect SEC rate limits

            if not roles and buy_usd == 0 and sell_usd == 0:
                continue

            net_buy = buy_usd - sell_usd
            ticker_l = ticker.lower()
            src_buy  = f'{_SOURCE_BUY}_{ticker_l}'
            src_sell = f'{_SOURCE_SELL}_{ticker_l}'
            meta     = {
                'as_of': now,
                'lookback_days': self.lookback_days,
                'buy_usd': round(buy_usd),
                'sell_usd': round(sell_usd),
            }

            # Direction
            if buy_usd > 0 and sell_usd == 0:
                direction = 'buy'
            elif sell_usd > 0 and buy_usd == 0:
                direction = 'sell'
            elif buy_usd > 0 and sell_usd > 0:
                direction = 'mixed'
            else:
                direction = 'none'

            conviction = _classify_conviction(max(net_buy, 0.0))
            role       = _classify_role_priority(roles) if roles else 'officer'
            conf_buy   = 0.90
            conf_sell  = 0.70  # sales are weaker signal

            conf = conf_buy if direction in ('buy', 'mixed') else conf_sell

            atoms.append(RawAtom(
                subject=ticker_l, predicate='insider_direction', object=direction,
                confidence=conf, source=src_buy, metadata=meta,
            ))
            atoms.append(RawAtom(
                subject=ticker_l, predicate='insider_value_usd',
                object=str(round(abs(net_buy))),
                confidence=conf, source=src_buy, metadata=meta,
            ))
            atoms.append(RawAtom(
                subject=ticker_l, predicate='insider_role', object=role,
                confidence=conf, source=src_buy, metadata=meta,
            ))
            atoms.append(RawAtom(
                subject=ticker_l, predicate='insider_conviction', object=conviction,
                confidence=conf, source=src_buy, metadata=meta,
            ))

            _logger.info(
                'InsiderAdapter: %s → direction=%s conviction=%s net=$%s',
                ticker, direction, conviction, round(net_buy),
            )

        return atoms
