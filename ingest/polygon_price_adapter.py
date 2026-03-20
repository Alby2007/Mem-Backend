"""
ingest/polygon_price_adapter.py — Polygon Price & Fundamentals Adapter

Replaces yfinance for US daily last_price atoms and adds fundamentals, news,
dividends/splits, and ticker details from the already-paid Polygon Stocks Starter
plan — all in a single adapter with internal timing guards.

WHAT THIS ADAPTER DOES
======================
Pass 1 — Grouped Daily (every cycle, 1 API call):
  GET /v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true
  → last_price atoms for all US watchlist tickers in one call.
  Replaces ~25+ individual yfinance fast_info calls for US equities.

Pass 2 — Ticker Details (20h guard):
  GET /v3/reference/tickers/{ticker}
  → sector, market_cap_tier atoms. More reliable than yfinance .info() 404s.

Pass 3 — Financials (20h guard):
  GET /vX/reference/financials?ticker={ticker}&timeframe=quarterly&limit=1
  → revenue, net_income, eps, free_cash_flow, debt_to_equity atoms.
  Fundamental data yfinance doesn't reliably provide.

Pass 4 — News (2h guard):
  GET /v2/reference/news?ticker={ticker}&limit=10&order=desc
  → rows inserted into extraction_queue (same table as RSSAdapter).
  LLMExtractionAdapter drains queue unchanged — zero pipeline changes.

Pass 5 — Dividends & Splits (24h guard):
  GET /v3/reference/dividends?ticker={ticker}&limit=5
  → ex_dividend_date, dividend_yield, split_ratio atoms.

RATE LIMITING
=============
Polygon Stocks Starter = 5 requests/minute = 12s between per-ticker calls.
Pass 1 is a single call — no sleep needed.
Passes 2-5 use time.sleep(12) between per-ticker calls.

REQUIRES
========
  POLYGON_API_KEY env var.
  Skips gracefully if not set.

TICKER SCOPE
============
US watchlist tickers only for passes 2-5 (not all 11k).
Pass 1 fetches all US tickers but only emits atoms for watchlist members.
Non-US tickers (.L, FX pairs, indices) are excluded — covered by yfinance.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_POLYGON_BASE = 'https://api.polygon.io'

# ── US watchlist tickers handled by this adapter ──────────────────────────────
# Dynamically loaded from universe_tickers on init.
# These are removed from yfinance's _bulk_download_prices() and
# _parallel_info_fetch() to avoid duplicate atoms.
# _cache_ohlcv_candles() in yfinance still runs for these (OHLCV history).

# Known ETF tickers — excluded from financials (Pass 3)
_ETF_TICKERS: Set[str] = {
    'SPY', 'QQQ', 'DIA', 'IWM', 'HYG', 'TLT', 'GLD', 'GDX', 'XBI', 'SMH',
    'ARKK', 'EEM', 'EFA', 'VWO', 'IBIT', 'ETHA', 'GBTC',
    'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLC', 'XLY', 'XLP', 'XLU', 'XLRE', 'XLB',
}


def _load_us_tickers(db_path: Optional[str] = None) -> Set[str]:
    """Load all US equity tickers from universe_tickers.
    US tickers = no '.' suffix, no '^' prefix, no '=' in name."""
    if not db_path:
        try:
            import extensions as ext
            db_path = getattr(ext, 'DB_PATH', None)
        except Exception:
            pass
    if not db_path:
        return set()
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            "SELECT ticker FROM universe_tickers "
            "WHERE ticker NOT LIKE '%.%' AND ticker NOT LIKE '^%' AND ticker NOT LIKE '%=%'"
        ).fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception as exc:
        _logger.warning('[polygon_price] failed to load US tickers: %s', exc)
        return set()


# Module-level set for yfinance_adapter to import
# Populated lazily on first access
_us_polygon_cache: Optional[Set[str]] = None


def get_us_polygon_tickers(db_path: Optional[str] = None) -> Set[str]:
    """Get the set of US tickers handled by Polygon. Cached after first call."""
    global _us_polygon_cache
    if _us_polygon_cache is None:
        _us_polygon_cache = _load_us_tickers(db_path)
    return _us_polygon_cache


# Backwards compat: yfinance imports this name
US_POLYGON_TICKERS = _load_us_tickers()

# Sleep between per-ticker calls to stay within 5 req/min on Starter plan
_RATE_SLEEP = 12.0


def _api_key() -> Optional[str]:
    return os.environ.get('POLYGON_API_KEY', '').strip() or None


def _get(session, url: str, params: dict, timeout: int = 15) -> Optional[dict]:
    """Perform a GET request and return parsed JSON or None on error."""
    try:
        resp = session.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            _logger.warning('[polygon_price] rate limited (429) on %s — sleeping 60s', url)
            time.sleep(60)
            return None
        if resp.status_code != 200:
            _logger.debug('[polygon_price] HTTP %d on %s', resp.status_code, url)
            return None
        return resp.json()
    except Exception as exc:
        _logger.debug('[polygon_price] request failed for %s: %s', url, exc)
        return None


class PolygonPriceAdapter(BaseIngestAdapter):
    """
    Polygon Stocks Starter adapter: US daily prices, fundamentals, news,
    dividends/splits and ticker metadata.

    Single registration at interval_sec=1800.
    Internal _should_run() guards gate the slower passes.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        tickers: Optional[Set[str]] = None,
    ):
        super().__init__(name='polygon_price')
        self._db_path = db_path
        self._tickers = tickers if tickers is not None else _load_us_tickers(db_path)
        self._equity_tickers = self._tickers - _ETF_TICKERS  # Equities only for financials
        _logger.info('[polygon_price] loaded %d US tickers (%d equities, %d ETFs)',
                     len(self._tickers), len(self._equity_tickers),
                     len(self._tickers) - len(self._equity_tickers))
        # In-memory last-run timestamps for each slow pass.
        # Staggered so only one slow pass fires per 1800s cycle:
        #   cycle 1 (0h):   details runs (last=None)
        #   cycle 2 (0.5h): financials runs (last=0.5h ago, guard=20h → runs)
        #   cycle 3 (1h):   news runs (last=1h ago, guard=2h → skips until cycle 4)
        #   cycle 4 (1.5h): news fires (last=1.5h ago > 2h? no — fires on cycle 5 at 2h)
        # In practice: details fires on cycle 1, financials on cycle 2,
        # news on cycle 3 (but guard is 2h so actually cycle 5), dividends cycle 4.
        # Pre-setting to 1h/2h/3h ago ensures they don't all pile up at restart.
        _now = datetime.now(timezone.utc)
        self._last_run: Dict[str, Optional[datetime]] = {
            'details':    None,                                  # fires on cycle 1
            'financials': _now - timedelta(hours=19),            # fires after 1h (cycle 2+)
            'news':       _now - timedelta(hours=1),             # fires after 1h (cycle 2)
            'dividends':  _now - timedelta(hours=23),            # fires after 1h (cycle 2)
        }

    def _should_run(self, pass_name: str, hours: float) -> bool:
        last = self._last_run.get(pass_name)
        if last is None:
            return True
        return (datetime.now(timezone.utc) - last).total_seconds() >= hours * 3600

    def _mark_run(self, pass_name: str) -> None:
        self._last_run[pass_name] = datetime.now(timezone.utc)

    def fetch(self) -> List[RawAtom]:
        key = _api_key()
        if not key:
            _logger.info('[polygon_price] POLYGON_API_KEY not set — skipping')
            return []

        try:
            import requests
        except ImportError:
            _logger.warning('[polygon_price] requests not installed — skipping')
            return []

        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})

        atoms: List[RawAtom] = []

        # ── Pass 1: Grouped Daily ─────────────────────────────────────────────
        atoms.extend(self._grouped_daily(session, key))

        # ── Pass 2: Ticker Details (20h guard) ────────────────────────────────
        if self._should_run('details', hours=20):
            atoms.extend(self._ticker_details(session, key))
            self._mark_run('details')

        # ── Pass 3: Financials (20h guard) ────────────────────────────────────
        if self._should_run('financials', hours=20):
            atoms.extend(self._financials(session, key))
            self._mark_run('financials')

        # ── Pass 4: News (2h guard) ───────────────────────────────────────────
        if self._should_run('news', hours=2):
            self._news(session, key)
            self._mark_run('news')

        # ── Pass 5: Dividends & Splits (24h guard) ────────────────────────────
        if self._should_run('dividends', hours=24):
            atoms.extend(self._dividends_splits(session, key))
            self._mark_run('dividends')

        return atoms

    # ── Pass 1: Grouped Daily ─────────────────────────────────────────────────

    def _grouped_daily(self, session, key: str) -> List[RawAtom]:
        """One call → last_price for all ~11,977 US tickers. Filter to watchlist."""
        # Use previous trading day (Polygon may not have today yet before market close)
        today = datetime.now(timezone.utc).date()
        # Try today first, fall back to yesterday
        for days_back in range(0, 4):
            date_str = (today - timedelta(days=days_back)).isoformat()
            url = f'{_POLYGON_BASE}/v2/aggs/grouped/locale/us/market/stocks/{date_str}'
            data = _get(session, url, {'adjusted': 'true', 'include_otc': 'false', 'apiKey': key})
            if data and data.get('resultsCount', 0) > 0:
                break
        else:
            _logger.warning('[polygon_price] grouped daily: no results for last 4 days')
            return []

        results = data.get('results') or []
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        matched = 0

        for r in results:
            sym = (r.get('T') or '').upper()
            if sym not in self._tickers:
                continue
            close = r.get('c')
            if not close or float(close) <= 0:
                continue
            atoms.append(RawAtom(
                subject=sym,
                predicate='last_price',
                object=str(round(float(close), 4)),
                confidence=0.95,
                source=f'polygon_daily_{sym.lower()}',
                metadata={
                    'as_of': date_str,
                    'open': r.get('o'),
                    'high': r.get('h'),
                    'low': r.get('l'),
                    'volume': r.get('v'),
                    'vwap': r.get('vw'),
                    'via': 'polygon_grouped_daily',
                },
                upsert=True,
            ))
            matched += 1

        _logger.info(
            '[polygon_price] grouped daily (%s): %d results, %d watchlist matches',
            date_str, len(results), matched,
        )
        return atoms

    # ── Pass 2: Ticker Details ────────────────────────────────────────────────

    def _ticker_details(self, session, key: str) -> List[RawAtom]:
        """Fetch sector, SIC, market cap from /v3/reference/tickers/{ticker}."""
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        succeeded = 0

        for sym in sorted(self._tickers):
            url = f'{_POLYGON_BASE}/v3/reference/tickers/{sym}'
            data = _get(session, url, {'apiKey': key})
            if data:
                result = data.get('results') or {}
                src = f'polygon_ref_{sym.lower()}'

                sic_desc = result.get('sic_description') or result.get('standard_industrial_classification', {}).get('sic_description', '')
                sector_raw = (
                    result.get('sector')
                    or sic_desc
                    or result.get('type', '')
                )
                if sector_raw:
                    sector_norm = sector_raw.lower().strip().replace(' ', '_').replace('-', '_').replace('/', '_')
                    atoms.append(RawAtom(
                        subject=sym, predicate='sector', object=sector_norm,
                        confidence=0.92, source=src,
                        metadata={'sector_raw': sector_raw, 'sic': result.get('sic_code'), 'as_of': now_iso},
                        upsert=True,
                    ))

                market_cap = result.get('market_cap')
                if market_cap:
                    atoms.append(RawAtom(
                        subject=sym, predicate='market_cap_tier',
                        object=_market_cap_tier(float(market_cap)),
                        confidence=0.92, source=src,
                        metadata={'market_cap_raw': market_cap, 'as_of': now_iso},
                        upsert=True,
                    ))

                succeeded += 1

            time.sleep(_RATE_SLEEP)

        _logger.info('[polygon_price] ticker details: %d/%d succeeded', succeeded, len(self._tickers))
        return atoms

    # ── Pass 3: Financials ────────────────────────────────────────────────────

    def _financials(self, session, key: str) -> List[RawAtom]:
        """Fetch quarterly fundamentals for US equity tickers."""
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        equity_tickers = self._equity_tickers  # already excludes ETFs via __init__
        succeeded = 0

        for sym in sorted(equity_tickers):
            url = f'{_POLYGON_BASE}/vX/reference/financials'
            data = _get(session, url, {
                'ticker': sym,
                'timeframe': 'quarterly',
                'limit': 1,
                'sort': 'period_of_report_date',
                'order': 'desc',
                'apiKey': key,
            })
            if data:
                results = data.get('results') or []
                if results:
                    fin = results[0]
                    src = f'polygon_financials_{sym.lower()}'
                    period = fin.get('fiscal_period', '') + fin.get('fiscal_year', '')
                    meta = {'period': period, 'as_of': now_iso, 'via': 'polygon_financials'}

                    ic = fin.get('financials', {}).get('income_statement', {})
                    bs = fin.get('financials', {}).get('balance_sheet', {})
                    cf = fin.get('financials', {}).get('cash_flow_statement', {})

                    def _val(section: dict, *keys) -> Optional[float]:
                        for k in keys:
                            v = section.get(k, {})
                            if isinstance(v, dict):
                                val = v.get('value')
                                if val is not None:
                                    return float(val)
                        return None

                    revenue = _val(ic, 'revenues', 'net_revenues')
                    if revenue is not None:
                        atoms.append(RawAtom(
                            subject=sym, predicate='revenue',
                            object=str(round(revenue, 0)),
                            confidence=0.95, source=src, metadata=meta, upsert=True,
                        ))

                    net_income = _val(ic, 'net_income_loss', 'net_income')
                    if net_income is not None:
                        atoms.append(RawAtom(
                            subject=sym, predicate='net_income',
                            object=str(round(net_income, 0)),
                            confidence=0.95, source=src, metadata=meta, upsert=True,
                        ))

                    eps = _val(ic, 'basic_earnings_per_share', 'diluted_earnings_per_share')
                    if eps is not None:
                        atoms.append(RawAtom(
                            subject=sym, predicate='eps',
                            object=str(round(eps, 4)),
                            confidence=0.95, source=src, metadata=meta, upsert=True,
                        ))

                    fcf = _val(cf, 'net_cash_flow_from_operating_activities_continuing')
                    capex = _val(cf, 'payments_for_property_plant_and_equipment')
                    if fcf is not None and capex is not None:
                        free_cf = fcf - abs(capex)
                        atoms.append(RawAtom(
                            subject=sym, predicate='free_cash_flow',
                            object=str(round(free_cf, 0)),
                            confidence=0.90, source=src, metadata=meta, upsert=True,
                        ))
                    elif fcf is not None:
                        atoms.append(RawAtom(
                            subject=sym, predicate='free_cash_flow',
                            object=str(round(fcf, 0)),
                            confidence=0.85, source=src, metadata=meta, upsert=True,
                        ))

                    total_debt = _val(bs, 'long_term_debt', 'long_term_debt_and_capital_lease_obligations')
                    equity = _val(bs, 'equity', 'stockholders_equity', 'equity_attributable_to_parent')
                    if total_debt is not None and equity and equity != 0:
                        dte = round(total_debt / abs(equity), 3)
                        atoms.append(RawAtom(
                            subject=sym, predicate='debt_to_equity',
                            object=str(dte),
                            confidence=0.90, source=src, metadata=meta, upsert=True,
                        ))

                    succeeded += 1

            time.sleep(_RATE_SLEEP)

        _logger.info('[polygon_price] financials: %d/%d succeeded', succeeded, len(equity_tickers))
        return atoms

    # ── Pass 4: News ──────────────────────────────────────────────────────────

    def _news(self, session, key: str) -> None:
        """Fetch ticker-tagged news and insert into extraction_queue for LLM processing.

        DISABLED — extraction_queue was append-only dead storage never consumed
        by LLM extraction adapter. Removing writes eliminates ~16k rows/hour of
        SQLite WAL contention.
        """
        return

    # ── Pass 5: Dividends & Splits ────────────────────────────────────────────

    def _dividends_splits(self, session, key: str) -> List[RawAtom]:
        """Fetch ex-dividend dates, dividend yields, and split ratios."""
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        succeeded = 0

        for sym in sorted(self._tickers):
            src = f'polygon_corp_{sym.lower()}'

            # Dividends
            div_url = f'{_POLYGON_BASE}/v3/reference/dividends'
            div_data = _get(session, div_url, {
                'ticker': sym,
                'limit': 5,
                'order': 'desc',
                'apiKey': key,
            })
            if div_data:
                results = div_data.get('results') or []
                if results:
                    latest = results[0]
                    ex_date = latest.get('ex_dividend_date') or latest.get('ex_date')
                    cash_amount = latest.get('cash_amount')
                    frequency = latest.get('frequency')  # 4 = quarterly, 12 = monthly, etc.

                    if ex_date:
                        atoms.append(RawAtom(
                            subject=sym, predicate='ex_dividend_date',
                            object=str(ex_date),
                            confidence=0.95, source=src,
                            metadata={'cash_amount': cash_amount, 'frequency': frequency, 'as_of': now_iso},
                            upsert=True,
                        ))

                    # Annualised yield estimate: cash_amount * frequency / last_price
                    if cash_amount and frequency:
                        try:
                            annual_div = float(cash_amount) * int(frequency)
                            # Try to get last_price from atoms already emitted this cycle
                            # (Will be approximate; KB has more accurate value)
                            atoms.append(RawAtom(
                                subject=sym, predicate='annual_dividend',
                                object=str(round(annual_div, 4)),
                                confidence=0.90, source=src,
                                metadata={'cash_per_period': cash_amount, 'frequency': frequency, 'as_of': now_iso},
                                upsert=True,
                            ))
                        except (TypeError, ValueError):
                            pass

                succeeded += 1

            time.sleep(_RATE_SLEEP)

            # Splits — separate endpoint call
            split_url = f'{_POLYGON_BASE}/v3/reference/splits'
            split_data = _get(session, split_url, {
                'ticker': sym,
                'limit': 3,
                'order': 'desc',
                'apiKey': key,
            })
            if split_data:
                results = split_data.get('results') or []
                if results:
                    latest = results[0]
                    split_from = latest.get('split_from')
                    split_to = latest.get('split_to')
                    ex_split_date = latest.get('execution_date')
                    if split_from and split_to:
                        ratio = f'{split_to}:{split_from}'
                        atoms.append(RawAtom(
                            subject=sym, predicate='split_ratio',
                            object=ratio,
                            confidence=0.95, source=src,
                            metadata={'execution_date': ex_split_date, 'as_of': now_iso},
                            upsert=True,
                        ))

            time.sleep(_RATE_SLEEP)

        _logger.info('[polygon_price] dividends/splits: %d/%d tickers processed', succeeded, len(self._tickers))
        return atoms


# ── Helpers ───────────────────────────────────────────────────────────────────

def _market_cap_tier(market_cap: float) -> str:
    if market_cap >= 200e9:
        return 'mega_cap'
    if market_cap >= 10e9:
        return 'large_cap'
    if market_cap >= 2e9:
        return 'mid_cap'
    if market_cap >= 300e6:
        return 'small_cap'
    return 'micro_cap'
