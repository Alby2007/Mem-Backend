"""
ingest/alpha_vantage_adapter.py — Alpha Vantage News Sentiment Adapter

Fetches AI-scored news sentiment per ticker from Alpha Vantage's
NEWS_SENTIMENT endpoint. Acts as a second opinion on every ticker
alongside the LLM extraction pipeline.

Free API key at: https://www.alphavantage.co/support/#api-key
Requires: ALPHA_VANTAGE_API_KEY env var

Rate limits:
  Free tier: 25 requests/day, 5/min
  Adapter batches watchlist tickers (up to 20/day) with 15s inter-request
  delay to stay within limits. Runs once per 24h.

Atoms produced per ticker:
  - {ticker} | av_sentiment_score  | {float -1..1}
  - {ticker} | av_sentiment_label  | Bullish | Somewhat-Bullish | Neutral |
                                     Somewhat-Bearish | Bearish
  - {ticker} | av_news_count_24h   | {int}

Source: financial_data_alpha_vantage  (authority 0.68, half-life 1d)
Schedule: 86400s (daily)
"""

from __future__ import annotations

import json as _json
import logging
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_AV_BASE = 'https://www.alphavantage.co/query'

# Max tickers per day (free tier: 25 req/day, keep 5 in reserve for retries)
_MAX_TICKERS_PER_RUN = 20
# Seconds between requests to respect 5/min limit
_REQUEST_DELAY_SEC = 13

# Sentiment score → label mapping (Alpha Vantage's own scale)
# score < -0.35 = Bearish, -0.35..-0.15 = Somewhat-Bearish, -0.15..0.15 = Neutral,
# 0.15..0.35 = Somewhat-Bullish, > 0.35 = Bullish
def _score_to_label(score: float) -> str:
    if score >= 0.35:
        return 'Bullish'
    if score >= 0.15:
        return 'Somewhat-Bullish'
    if score >= -0.15:
        return 'Neutral'
    if score >= -0.35:
        return 'Somewhat-Bearish'
    return 'Bearish'


def _fetch_sentiment(ticker: str, api_key: str) -> Optional[Tuple[float, int]]:
    """
    Fetch sentiment for one ticker. Returns (avg_score, article_count) or None.
    """
    params = {
        'function': 'NEWS_SENTIMENT',
        'tickers':  ticker.upper(),
        'limit':    50,
        'apikey':   api_key,
    }
    url = _AV_BASE + '?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingGalaxyKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))

        # Check for API limit message
        if 'Information' in data or 'Note' in data:
            msg = data.get('Information') or data.get('Note', '')
            _logger.warning('AV API limit hit for %s: %s', ticker, msg[:120])
            return None

        feed = data.get('feed', [])
        if not feed:
            return None

        # Average ticker-specific sentiment scores from each article
        ticker_upper = ticker.upper()
        scores: List[float] = []
        for article in feed:
            for ts in article.get('ticker_sentiment', []):
                if ts.get('ticker', '').upper() == ticker_upper:
                    try:
                        scores.append(float(ts['ticker_sentiment_score']))
                    except (KeyError, ValueError):
                        pass

        if not scores:
            return None

        avg_score = sum(scores) / len(scores)
        return (round(avg_score, 4), len(feed))

    except Exception as exc:
        _logger.warning('AV sentiment fetch failed for %s: %s', ticker, exc)
        return None


def _get_watchlist_tickers(db_path: str, limit: int) -> List[str]:
    """Pull tracked tickers from the KB facts table."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            """SELECT DISTINCT subject FROM facts
               WHERE predicate = 'last_price' AND subject NOT LIKE '%:%'
               ORDER BY created_at DESC LIMIT ?""",
            (limit * 3,),   # over-fetch, then dedupe
        ).fetchall()
        conn.close()
        seen: Dict[str, bool] = {}
        result: List[str] = []
        for (s,) in rows:
            t = s.upper().strip()
            if t and t not in seen:
                seen[t] = True
                result.append(t)
            if len(result) >= limit:
                break
        return result
    except Exception as exc:
        _logger.warning('AV: could not load watchlist tickers: %s', exc)
        return []


class AlphaVantageAdapter(BaseIngestAdapter):
    """
    Alpha Vantage NEWS_SENTIMENT adapter.

    Scores per-ticker news sentiment daily using AV's AI model.
    Provides a calibrated second opinion alongside LLM extraction.
    Skips gracefully if ALPHA_VANTAGE_API_KEY is not set.
    """

    def __init__(self, db_path: Optional[str] = None, api_key: Optional[str] = None):
        super().__init__(name='av_sentiment')
        self._api_key = api_key or os.environ.get('ALPHA_VANTAGE_API_KEY', '')
        self._db_path = db_path or os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')

    def fetch(self) -> List[RawAtom]:
        if not self._api_key:
            self._logger.warning(
                'ALPHA_VANTAGE_API_KEY not set — skipping AV sentiment adapter. '
                'Get a free key at https://www.alphavantage.co/support/#api-key'
            )
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'financial_data_alpha_vantage'
        meta_base = {'fetched_at': now_iso, 'source_url': _AV_BASE}

        tickers = _get_watchlist_tickers(self._db_path, _MAX_TICKERS_PER_RUN)
        if not tickers:
            self._logger.info('AV: no tickers in watchlist to score')
            return []

        atoms: List[RawAtom] = []
        successes = 0

        for i, ticker in enumerate(tickers):
            if i > 0:
                time.sleep(_REQUEST_DELAY_SEC)

            result = _fetch_sentiment(ticker, self._api_key)
            if result is None:
                # Could be rate limit — stop early rather than burning remaining quota
                if i > 0:
                    self._logger.info('AV: stopping early at ticker %d/%d', i + 1, len(tickers))
                    break
                continue

            avg_score, article_count = result
            label = _score_to_label(avg_score)
            ticker_lower = ticker.lower()

            atoms.extend([
                RawAtom(
                    subject=ticker_lower,
                    predicate='av_sentiment_score',
                    object=str(avg_score),
                    confidence=0.68,
                    source=source,
                    metadata={**meta_base, 'ticker': ticker, 'article_count': article_count},
                    upsert=True,
                ),
                RawAtom(
                    subject=ticker_lower,
                    predicate='av_sentiment_label',
                    object=label,
                    confidence=0.68,
                    source=source,
                    metadata={**meta_base, 'ticker': ticker, 'score': avg_score},
                    upsert=True,
                ),
                RawAtom(
                    subject=ticker_lower,
                    predicate='av_news_count_24h',
                    object=str(article_count),
                    confidence=0.70,
                    source=source,
                    metadata={**meta_base, 'ticker': ticker},
                    upsert=True,
                ),
            ])
            successes += 1

        self._logger.info(
            'AV sentiment adapter: %d tickers scored, %d atoms produced',
            successes, len(atoms),
        )
        return atoms
