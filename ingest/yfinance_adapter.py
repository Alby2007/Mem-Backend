"""
ingest/yfinance_adapter.py — Yahoo Finance Ingest Adapter (Trading KB)

Pulls price data, fundamentals, analyst targets, and earnings dates from
Yahoo Finance via the yfinance library. No API key required.

Atoms produced:
  - {TICKER} | last_price        | {price}
  - {TICKER} | price_target      | {mean analyst target}
  - {TICKER} | signal_direction  | {long/short/neutral}  (derived from price vs target)
  - {TICKER} | sector            | {sector}
  - {TICKER} | market_cap_tier   | {mega/large/mid/small/micro}
  - {TICKER} | earnings_quality  | {next earnings date}
  - {TICKER} | volatility_regime | {high/medium/low}  (from beta)

Source prefix: exchange_feed_yahoo  (authority 1.0, fast decay)
               broker_research_yahoo_consensus (for analyst targets)

Interval: recommended 15 min for price, daily for fundamentals.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    yf = None  # type: ignore

_logger = logging.getLogger(__name__)

# Default watchlist — override via constructor
# Covers all 11 S&P sectors + major ETFs + macro proxies
_DEFAULT_TICKERS = [
    # Mega-cap tech
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO',
    # Financials
    'JPM', 'V', 'MA', 'BAC', 'GS', 'MS', 'BRK-B',
    # Healthcare
    'UNH', 'JNJ', 'LLY', 'ABBV', 'PFE',
    # Energy
    'XOM', 'CVX', 'COP',
    # Consumer
    'WMT', 'PG', 'KO', 'MCD', 'COST',
    # Industrials
    'CAT', 'HON', 'RTX',
    # Comms
    'DIS', 'NFLX', 'CMCSA',
    # Broad market ETFs
    'SPY', 'QQQ', 'IWM', 'DIA', 'VTI',
    # Sector ETFs
    'XLF', 'XLE', 'XLK', 'XLV', 'XLI',
    # Macro proxies
    'GLD', 'SLV', 'TLT', 'HYG', 'UUP',
]

# Batch size for yfinance calls — keeps requests small to avoid rate limits
_BATCH_SIZE = 10
_BATCH_DELAY_SEC = 1.5  # seconds between batches

# ETF quoteType values that don't carry equity fundamentals
_ETF_QUOTE_TYPES = {'ETF', 'MUTUALFUND', 'INDEX', 'FUTURE', 'CRYPTOCURRENCY'}

# Fallback category labels for well-known ETFs when yfinance returns empty category
_ETF_CATEGORY_FALLBACK: dict = {
    # Sector ETFs
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
    # Broad market
    'SPY': 'broad_market_us_large_cap',
    'QQQ': 'broad_market_nasdaq100',
    'IWM': 'broad_market_us_small_cap',
    'DIA': 'broad_market_dow30',
    'VTI': 'broad_market_us_total',
    # Rates & credit
    'TLT': 'long_government_bonds',
    'HYG': 'high_yield_credit',
    'LQD': 'investment_grade_credit',
    # Macro proxies
    'GLD': 'gold_commodity_inflation_hedge',
    'SLV': 'silver_commodity_inflation_hedge',
    'UUP': 'us_dollar_index',
    'USO': 'crude_oil_commodity',
}


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
    Yahoo Finance ingest adapter.

    Pulls price, fundamentals, analyst targets for a watchlist of tickers.
    No API key required.
    """

    def __init__(self, tickers: Optional[List[str]] = None):
        super().__init__(name='yfinance')
        self.tickers = tickers or _DEFAULT_TICKERS

    def fetch(self) -> List[RawAtom]:
        if not HAS_YFINANCE:
            self._logger.error(
                'yfinance not installed — pip install yfinance'
            )
            return []

        import time
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()

        # Process in batches to avoid yfinance rate limits
        for i in range(0, len(self.tickers), _BATCH_SIZE):
            batch = self.tickers[i:i + _BATCH_SIZE]
            for symbol in batch:
                atoms.extend(self._fetch_with_backoff(symbol, now_iso))
            if i + _BATCH_SIZE < len(self.tickers):
                time.sleep(_BATCH_DELAY_SEC)

        return atoms

    def _fetch_with_backoff(self, symbol: str, now_iso: str) -> List[RawAtom]:
        """Fetch a ticker with exponential backoff on transient errors."""
        import time
        delay = 2.0
        for attempt in range(3):
            try:
                return self._fetch_ticker(symbol, now_iso)
            except Exception as e:
                err = str(e).lower()
                if any(k in err for k in ('429', 'rate', 'too many', 'timeout', 'timed out')):
                    self._logger.warning(
                        'Rate limit on %s attempt %d, backing off %.0fs', symbol, attempt + 1, delay
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    self._logger.warning('Failed to fetch %s: %s', symbol, e)
                    return []
        self._logger.warning('Giving up on %s after 3 attempts', symbol)
        return []

    def _fetch_ticker(self, symbol: str, now_iso: str) -> List[RawAtom]:
        """Fetch atoms for a single ticker."""
        atoms: List[RawAtom] = []
        ticker = yf.Ticker(symbol)

        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        if not info:
            return atoms

        quote_type = info.get('quoteType', 'EQUITY').upper()
        is_etf = quote_type in _ETF_QUOTE_TYPES
        src = f'exchange_feed_yahoo_{symbol.lower().replace("-", "_")}'

        # ── Price atom (all instruments) ──────────────────────────────────
        current_price = (
            info.get('navPrice')
            or info.get('currentPrice')
            or info.get('regularMarketPrice')
        )
        if current_price:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='last_price',
                object=str(round(current_price, 2)),
                confidence=0.95,
                source=src,
                metadata={'as_of': now_iso, 'currency': info.get('currency', 'USD'), 'quote_type': quote_type},
            ))

        if is_etf:
            return self._etf_atoms(symbol, info, src, now_iso, atoms)

        # ── Analyst consensus target ──────────────────────────────────────
        target_price = info.get('targetMeanPrice')
        consensus_src = f'broker_research_yahoo_consensus_{symbol.lower()}'
        if target_price:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='price_target',
                object=str(round(target_price, 2)),
                confidence=0.75,
                source=consensus_src,
                metadata={
                    'as_of': now_iso,
                    'target_high': info.get('targetHighPrice'),
                    'target_low': info.get('targetLowPrice'),
                    'num_analysts': info.get('numberOfAnalystOpinions'),
                },
            ))
            if current_price:
                direction = _direction_from_target(current_price, target_price)
                pct_upside = round((target_price - current_price) / current_price * 100, 1)
                atoms.append(RawAtom(
                    subject=symbol,
                    predicate='signal_direction',
                    object=direction,
                    confidence=0.65,
                    source=consensus_src,
                    metadata={'derived_from': 'price_vs_consensus_target', 'pct_upside': pct_upside, 'as_of': now_iso},
                ))

        # ── Sector ────────────────────────────────────────────────────────
        sector = info.get('sector')
        if sector:
            atoms.append(RawAtom(
                subject=symbol, predicate='sector', object=sector,
                confidence=0.95, source=src,
                metadata={'industry': info.get('industry')},
            ))

        # ── Market cap tier ───────────────────────────────────────────────
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
            regime = _volatility_regime(beta)
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol, predicate='volatility_regime', object=regime,
                    confidence=0.80, source=src, metadata={'beta': beta},
                ))

        # ── Next earnings date ────────────────────────────────────────────
        try:
            cal = ticker.calendar
            if cal is not None:
                # yfinance returns calendar as dict or DataFrame depending on version
                if isinstance(cal, dict):
                    earnings_date = cal.get('Earnings Date')
                    if isinstance(earnings_date, list) and earnings_date:
                        earnings_date = str(earnings_date[0])
                    elif earnings_date:
                        earnings_date = str(earnings_date)
                    else:
                        earnings_date = None
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
                    ))
        except Exception:
            pass

        return atoms

    def _etf_atoms(
        self, symbol: str, info: dict, src: str, now_iso: str, atoms: List[RawAtom]
    ) -> List[RawAtom]:
        """ETF-specific atoms: category, AUM tier, beta-derived volatility."""
        # Category → sector proxy (fallback to hardcoded map for well-known ETFs)
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

        # AUM → market cap tier equivalent
        aum = info.get('totalAssets')
        if aum:
            atoms.append(RawAtom(
                subject=symbol, predicate='market_cap_tier',
                object=_market_cap_tier(aum),
                confidence=0.90, source=src,
                metadata={'aum_raw': aum, 'metric': 'total_assets'},
            ))

        # Beta → volatility regime (3-year beta preferred for ETFs)
        beta = info.get('beta3Year') or info.get('beta')
        if beta is not None:
            regime = _volatility_regime(beta)
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol, predicate='volatility_regime', object=regime,
                    confidence=0.75, source=src,
                    metadata={'beta': beta, 'etf': True},
                ))

        # 52-week performance → momentum signal
        high_52 = info.get('fiftyTwoWeekHigh')
        low_52 = info.get('fiftyTwoWeekLow')
        price = info.get('regularMarketPrice') or info.get('navPrice')
        if high_52 and low_52 and price:
            pct_from_high = round((price - high_52) / high_52 * 100, 1)
            momentum = 'near_high' if pct_from_high > -5 else ('near_low' if pct_from_high < -20 else 'mid_range')
            atoms.append(RawAtom(
                subject=symbol, predicate='signal_direction', object=momentum,
                confidence=0.60, source=src,
                metadata={'pct_from_52w_high': pct_from_high, 'as_of': now_iso},
            ))

        return atoms
