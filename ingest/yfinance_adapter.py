"""
ingest/yfinance_adapter.py — Yahoo Finance Ingest Adapter (Trading KB)

Pulls price data, fundamentals, analyst targets, and earnings dates from
Yahoo Finance via the yfinance library. No API key required.

Atoms produced:
  - {TICKER} | last_price        | {price}
  - {TICKER} | price_target      | {mean analyst target}
  - {TICKER} | signal_direction  | {long/short/neutral}  (derived from price vs target, equities only)
  - {TICKER} | price_regime      | {near_high/mid_range/near_low}  (ETFs only — 52w position)
  - {TICKER} | sector            | {sector}
  - {TICKER} | market_cap_tier   | {mega/large/mid/small/micro}
  - {TICKER} | earnings_quality  | {next earnings date}
  - {TICKER} | volatility_regime | {high/medium/low}  (from beta)

Source prefix: exchange_feed_yahoo  (authority 1.0, fast decay)
               broker_research_yahoo_consensus (for analyst targets)

Performance:
  - Fast path: yf.download() bulk price fetch for last_price (all tickers in 1 call)
  - Parallel:  ThreadPoolExecutor for per-ticker info() calls (fundamentals/targets)
  - upsert=True on all time-series atoms so repeat runs update rather than append

Interval: recommended 5 min for price, 30 min for fundamentals.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

try:
    from ingest.dynamic_watchlist import DynamicWatchlistManager
    _HAS_DYNAMIC_WATCHLIST = True
except ImportError:
    _HAS_DYNAMIC_WATCHLIST = False
    DynamicWatchlistManager = None  # type: ignore

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    yf = None  # type: ignore

_logger = logging.getLogger(__name__)

# Default watchlist — override via constructor
_DEFAULT_TICKERS = [
    # FTSE 100 heavyweights
    'SHEL.L', 'AZN.L', 'HSBA.L', 'ULVR.L', 'BP.L',
    'GSK.L', 'RIO.L', 'BATS.L', 'VOD.L', 'LLOY.L',
    'BARC.L', 'NWG.L', 'LSEG.L', 'REL.L', 'NG.L',
    # Defence / industrials
    'BA.L', 'QQ.L', 'RR.L',
    # Consumer / retail
    'TSCO.L', 'MKS.L', 'PSON.L',
    # Housebuilders
    'PSN.L',
    # FTSE indices as macro proxies
    '^FTSE',    # FTSE 100
    '^FTMC',    # FTSE 250
    # UK FX — critical macro signal
    'GBPUSD=X',
    'EURGBP=X',
    # US rate proxy (liquid, used as gilt/rate regime anchor)
    'TLT',
    # Global macro proxies — London is globally connected
    'GLD',
    '^GSPC',    # S&P 500 correlation
    '^VIX',
    # US macro confirmation proxies (required by signal_enrichment_adapter)
    'SPY', 'HYG', 'TLT',
    # User portfolio holdings — ensure live price + signal enrichment every cycle
    'ARKK', 'COIN', 'HOOD', 'MSTR', 'PLTR', 'NVDA',
    'XYZ',      # Block Inc. (SQ rebranded)
    # High-conviction watchlist — US mega-cap with strong KB coverage
    'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT', 'MA',
]

# Parallel workers for per-ticker info() calls
_MAX_WORKERS = 12

# ETF quoteType values that don't carry equity fundamentals
_ETF_QUOTE_TYPES = {'ETF', 'MUTUALFUND', 'INDEX', 'FUTURE', 'CRYPTOCURRENCY'}

# Fallback category labels for well-known ETFs when yfinance returns empty category
_ETF_CATEGORY_FALLBACK: dict = {
    'XLF': 'financials_sector',
    'XLE': 'energy_sector',
    'XLK': 'technology_sector',
    'XLV': 'healthcare_sector',
    'XLI': 'industrials_sector',
    'XLC': 'communication_services_sector',
    'XLY': 'consumer_discretionary_sector',
    'XLP': 'consumer_staples_sector',
    'XLU': 'utilities_sector',
    'XLRE': 'real_estate_sector',
    'XLB': 'materials_sector',
    'SPY': 'broad_market_us_large_cap',
    'QQQ': 'broad_market_nasdaq100',
    'IWM': 'broad_market_us_small_cap',
    'DIA': 'broad_market_dow30',
    'VTI': 'broad_market_us_total',
    'TLT': 'long_government_bonds',
    'HYG': 'high_yield_credit',
    'LQD': 'investment_grade_credit',
    'GLD': 'gold_commodity_inflation_hedge',
    'SLV': 'silver_commodity_inflation_hedge',
    'UUP': 'us_dollar_index',
    'USO': 'crude_oil_commodity',
}

# Predicates that are time-series and should upsert (update) on repeat ingest
_UPSERT_PREDICATES = {'last_price', 'price_target', 'signal_direction', 'volatility_regime'}


def _market_cap_tier(market_cap: Optional[float]) -> str:
    """Classify market cap into standard tiers."""
    if not market_cap:
        return 'unknown'
    if market_cap >= 200e9:
        return 'mega_cap'
    if market_cap >= 10e9:
        return 'large_cap'
    if market_cap >= 2e9:
        return 'mid_cap'
    if market_cap >= 300e6:
        return 'small_cap'
    return 'micro_cap'


def _volatility_regime(beta: Optional[float]) -> str:
    """Classify volatility regime from beta."""
    if beta is None:
        return 'unknown'
    if beta > 1.5:
        return 'high_volatility'
    if beta > 0.8:
        return 'medium_volatility'
    return 'low_volatility'


def _direction_from_target(current_price: float, target_price: float) -> str:
    """Derive directional signal from current price vs analyst consensus target."""
    if target_price <= 0 or current_price <= 0:
        return 'neutral'
    pct = (target_price - current_price) / current_price
    if pct > 0.10:
        return 'long'
    if pct < -0.10:
        return 'short'
    return 'neutral'


class YFinanceAdapter(BaseIngestAdapter):
    """
    Yahoo Finance ingest adapter — parallel fetch version.

    Fast path: yf.download() fetches all last_price atoms in a single
    bulk HTTP call (~2s for 60 tickers).

    Parallel path: ThreadPoolExecutor with _MAX_WORKERS concurrent threads
    for per-ticker info() calls (fundamentals, analyst targets, beta).

    Time-series atoms (last_price, price_target, signal_direction,
    volatility_regime) are marked upsert=True so repeat runs overwrite
    the existing row from the same source rather than appending new rows.
    """

    def __init__(self, tickers: Optional[List[str]] = None, db_path: Optional[str] = None):
        super().__init__(name='yfinance')
        # If no explicit tickers, use DynamicWatchlistManager (falls back to _DEFAULT_TICKERS)
        if tickers is None:
            if _HAS_DYNAMIC_WATCHLIST and db_path:
                tickers = DynamicWatchlistManager.get_active_tickers(db_path)
            else:
                tickers = _DEFAULT_TICKERS
        # Deduplicate while preserving order
        seen: set = set()
        self.tickers: List[str] = []
        for t in tickers:
            if t not in seen:
                seen.add(t)
                self.tickers.append(t)

    def fetch(self) -> List[RawAtom]:
        if not HAS_YFINANCE:
            self._logger.error('yfinance not installed — pip install yfinance')
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        # ── 1. Bulk last_price via yf.download() ──────────────────────────
        bulk_prices = self._bulk_download_prices(now_iso)
        atoms.extend(bulk_prices)

        # ── 2. Parallel per-ticker info() for fundamentals + targets ──────
        info_atoms = self._parallel_info_fetch(now_iso, bulk_prices)
        atoms.extend(info_atoms)

        self._logger.info(
            'fetch complete: %d tickers, %d total atoms (%d price, %d info)',
            len(self.tickers), len(atoms), len(bulk_prices), len(info_atoms),
        )
        return atoms

    # ── Fast bulk price path ───────────────────────────────────────────────

    def _bulk_download_prices(self, now_iso: str) -> List[RawAtom]:
        """
        Use yf.download() to fetch the latest close price for ALL tickers in
        a single HTTP round-trip. Falls back to empty list on failure.
        """
        atoms: List[RawAtom] = []
        try:
            import pandas as pd
            data = yf.download(
                tickers=self.tickers,
                period='2d',
                interval='1d',
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if data is None or data.empty:
                return atoms

            for symbol in self.tickers:
                src = f'exchange_feed_yahoo_{symbol.lower().replace("-", "_")}'
                try:
                    if len(self.tickers) == 1:
                        # Single-ticker download has flat columns
                        col_data = data['Close']
                    else:
                        col_data = data[symbol]['Close']

                    series = col_data.dropna()
                    if series.empty:
                        continue
                    price = float(series.iloc[-1])
                    atoms.append(RawAtom(
                        subject=symbol,
                        predicate='last_price',
                        object=str(round(price, 2)),
                        confidence=0.95,
                        source=src,
                        metadata={'as_of': now_iso, 'via': 'bulk_download'},
                        upsert=True,
                    ))
                except Exception as e:
                    self._logger.debug('bulk price miss for %s: %s', symbol, e)

        except Exception as e:
            self._logger.warning('bulk_download_prices failed: %s', e)

        return atoms

    # ── Parallel info() path ───────────────────────────────────────────────

    def _parallel_info_fetch(
        self, now_iso: str, bulk_atoms: List[RawAtom]
    ) -> List[RawAtom]:
        """
        Fetch per-ticker .info() in parallel using ThreadPoolExecutor.
        Skips last_price (already covered by bulk download).
        """
        # Build a quick lookup of prices already fetched via bulk
        bulk_prices: Dict[str, float] = {}
        for a in bulk_atoms:
            if a.predicate == 'last_price':
                try:
                    bulk_prices[a.subject.upper()] = float(a.object)
                except ValueError:
                    pass

        all_atoms: List[RawAtom] = []
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix='yf-info') as ex:
            futures = {
                ex.submit(self._fetch_info_atoms, sym, now_iso, bulk_prices): sym
                for sym in self.tickers
            }
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    result = future.result(timeout=30)
                    all_atoms.extend(result)
                except Exception as e:
                    self._logger.warning('info fetch failed for %s: %s', sym, e)

        return all_atoms

    def _fetch_info_atoms(
        self, symbol: str, now_iso: str, bulk_prices: Dict[str, float]
    ) -> List[RawAtom]:
        """
        Fetch .info() for one ticker and return non-price atoms.
        Retries once on rate-limit / timeout.
        """
        import time
        for attempt in range(2):
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info or {}
                if not info:
                    return []
                return self._info_to_atoms(symbol, info, now_iso, bulk_prices)
            except Exception as e:
                err = str(e).lower()
                if attempt == 0 and any(k in err for k in ('429', 'rate', 'too many', 'timeout')):
                    time.sleep(3.0)
                    continue
                self._logger.debug('info() failed for %s (attempt %d): %s', symbol, attempt + 1, e)
                return []
        return []

    def _info_to_atoms(
        self,
        symbol: str,
        info: dict,
        now_iso: str,
        bulk_prices: Dict[str, float],
    ) -> List[RawAtom]:
        """Convert a .info() dict to atoms (excluding last_price which came from bulk)."""
        atoms: List[RawAtom] = []
        src = f'exchange_feed_yahoo_{symbol.lower().replace("-", "_")}'
        quote_type = info.get('quoteType', 'EQUITY').upper()
        is_etf = quote_type in _ETF_QUOTE_TYPES

        # Use bulk price if available, fall back to info fields
        current_price = bulk_prices.get(symbol.upper()) or (
            info.get('navPrice')
            or info.get('currentPrice')
            or info.get('regularMarketPrice')
        )

        # If bulk download missed this ticker, emit last_price from info
        if current_price and symbol.upper() not in bulk_prices:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='last_price',
                object=str(round(float(current_price), 2)),
                confidence=0.90,
                source=src,
                metadata={'as_of': now_iso, 'via': 'info_fallback', 'quote_type': quote_type},
                upsert=True,
            ))

        # Store currency so LLM never invents a wrong symbol
        _currency = info.get('currency') or info.get('financialCurrency')
        if _currency:
            atoms.append(RawAtom(
                subject=symbol, predicate='currency', object=_currency.upper(),
                confidence=0.99, source=src,
                metadata={'as_of': now_iso},
                upsert=True,
            ))

        if is_etf:
            return self._etf_atoms(symbol, info, src, now_iso, atoms, current_price)

        # ── Analyst consensus target ──────────────────────────────────────
        consensus_src = f'broker_research_yahoo_consensus_{symbol.lower()}'
        target_price = info.get('targetMeanPrice')
        if target_price:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='price_target',
                object=str(round(float(target_price), 2)),
                confidence=0.75,
                source=consensus_src,
                metadata={
                    'as_of': now_iso,
                    'target_high': info.get('targetHighPrice'),
                    'target_low': info.get('targetLowPrice'),
                    'num_analysts': info.get('numberOfAnalystOpinions'),
                },
                upsert=True,
            ))
            if current_price and float(current_price) > 0:
                direction = _direction_from_target(float(current_price), float(target_price))
                pct_upside = round((float(target_price) - float(current_price)) / float(current_price) * 100, 1)
                atoms.append(RawAtom(
                    subject=symbol,
                    predicate='signal_direction',
                    object=direction,
                    confidence=0.65,
                    source=consensus_src,
                    metadata={'derived_from': 'price_vs_consensus_target', 'pct_upside': pct_upside, 'as_of': now_iso},
                    upsert=True,
                ))

        # ── Sector (static — no upsert needed) ───────────────────────────
        sector = info.get('sector')
        if sector:
            atoms.append(RawAtom(
                subject=symbol, predicate='sector', object=sector,
                confidence=0.95, source=src,
                metadata={'industry': info.get('industry')},
            ))

        # ── Market cap tier (static) ──────────────────────────────────────
        market_cap = info.get('marketCap')
        if market_cap:
            atoms.append(RawAtom(
                subject=symbol, predicate='market_cap_tier',
                object=_market_cap_tier(market_cap),
                confidence=0.95, source=src,
                metadata={'market_cap_raw': market_cap},
            ))

        # ── Volatility regime (beta) ──────────────────────────────────────
        beta = info.get('beta')
        if beta is not None:
            regime = _volatility_regime(float(beta))
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol, predicate='volatility_regime', object=regime,
                    confidence=0.80, source=src, metadata={'beta': beta},
                    upsert=True,
                ))

        # ── Next earnings date (upsert — date changes each quarter) ───────
        try:
            ticker = yf.Ticker(symbol)
            cal = ticker.calendar
            if cal is not None and isinstance(cal, dict):
                earnings_date = cal.get('Earnings Date')
                if isinstance(earnings_date, list) and earnings_date:
                    earnings_date = str(earnings_date[0])
                elif earnings_date:
                    earnings_date = str(earnings_date)
                else:
                    earnings_date = None
                if earnings_date:
                    atoms.append(RawAtom(
                        subject=symbol,
                        predicate='earnings_quality',
                        object=f'next_earnings: {earnings_date}',
                        confidence=0.85,
                        source=f'earnings_{symbol.lower()}_upcoming',
                        metadata={'earnings_date': earnings_date},
                        upsert=True,
                    ))
        except Exception:
            pass

        return atoms

    def _etf_atoms(
        self, symbol: str, info: dict, src: str, now_iso: str,
        atoms: List[RawAtom], current_price
    ) -> List[RawAtom]:
        """ETF-specific atoms: category, AUM tier, beta-derived volatility, momentum."""
        category = (
            info.get('category')
            or info.get('fundFamily')
            or _ETF_CATEGORY_FALLBACK.get(symbol)
        )
        if category:
            atoms.append(RawAtom(
                subject=symbol, predicate='sector', object=f'etf:{category}',
                confidence=0.90, source=src,
                metadata={'etf_category': category},
            ))

        aum = info.get('totalAssets')
        if aum:
            atoms.append(RawAtom(
                subject=symbol, predicate='market_cap_tier',
                object=_market_cap_tier(aum),
                confidence=0.90, source=src,
                metadata={'aum_raw': aum, 'metric': 'total_assets'},
            ))

        beta = info.get('beta3Year') or info.get('beta')
        if beta is not None:
            regime = _volatility_regime(float(beta))
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol, predicate='volatility_regime', object=regime,
                    confidence=0.75, source=src,
                    metadata={'beta': beta, 'etf': True},
                    upsert=True,
                ))

        high_52 = info.get('fiftyTwoWeekHigh')
        low_52  = info.get('fiftyTwoWeekLow')
        price   = current_price or info.get('regularMarketPrice') or info.get('navPrice')
        if high_52 and low_52 and price:
            pct_from_high = round((float(price) - float(high_52)) / float(high_52) * 100, 1)
            regime = 'near_high' if pct_from_high > -5 else ('near_low' if pct_from_high < -20 else 'mid_range')
            atoms.append(RawAtom(
                subject=symbol, predicate='price_regime', object=regime,
                confidence=0.60, source=src,
                metadata={'pct_from_52w_high': pct_from_high, 'as_of': now_iso},
                upsert=True,
            ))

        return atoms
