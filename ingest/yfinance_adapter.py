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
_BATCH_DELAY_SEC = 1.0  # seconds between batches


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
                try:
                    atoms.extend(self._fetch_ticker(symbol, now_iso))
                except Exception as e:
                    self._logger.warning('Failed to fetch %s: %s', symbol, e)
            if i + _BATCH_SIZE < len(self.tickers):
                time.sleep(_BATCH_DELAY_SEC)

        return atoms

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

        # ── Price atom ────────────────────────────────────────────────────
        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
        if current_price:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='last_price',
                object=str(round(current_price, 2)),
                confidence=0.95,
                source=f'exchange_feed_yahoo_{symbol.lower()}',
                metadata={'as_of': now_iso, 'currency': info.get('currency', 'USD')},
            ))

        # ── Analyst consensus target ──────────────────────────────────────
        target_price = info.get('targetMeanPrice')
        if target_price:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='price_target',
                object=str(round(target_price, 2)),
                confidence=0.75,
                source=f'broker_research_yahoo_consensus_{symbol.lower()}',
                metadata={
                    'as_of': now_iso,
                    'target_high': info.get('targetHighPrice'),
                    'target_low': info.get('targetLowPrice'),
                    'num_analysts': info.get('numberOfAnalystOpinions'),
                },
            ))

            # ── Derived directional signal ────────────────────────────────
            if current_price:
                direction = _direction_from_target(current_price, target_price)
                pct_upside = round(
                    (target_price - current_price) / current_price * 100, 1
                )
                atoms.append(RawAtom(
                    subject=symbol,
                    predicate='signal_direction',
                    object=direction,
                    confidence=0.65,
                    source=f'broker_research_yahoo_consensus_{symbol.lower()}',
                    metadata={
                        'derived_from': 'price_vs_consensus_target',
                        'pct_upside': pct_upside,
                        'as_of': now_iso,
                    },
                ))

        # ── Sector ────────────────────────────────────────────────────────
        sector = info.get('sector')
        if sector:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='sector',
                object=sector,
                confidence=0.95,
                source=f'exchange_feed_yahoo_{symbol.lower()}',
                metadata={'industry': info.get('industry')},
            ))

        # ── Market cap tier ───────────────────────────────────────────────
        market_cap = info.get('marketCap')
        if market_cap:
            atoms.append(RawAtom(
                subject=symbol,
                predicate='market_cap_tier',
                object=_market_cap_tier(market_cap),
                confidence=0.95,
                source=f'exchange_feed_yahoo_{symbol.lower()}',
                metadata={'market_cap_raw': market_cap},
            ))

        # ── Volatility regime (beta) ──────────────────────────────────────
        beta = info.get('beta')
        if beta is not None:
            regime = _volatility_regime(beta)
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol,
                    predicate='volatility_regime',
                    object=regime,
                    confidence=0.80,
                    source=f'exchange_feed_yahoo_{symbol.lower()}',
                    metadata={'beta': beta},
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
