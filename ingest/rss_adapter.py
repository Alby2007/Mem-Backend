"""
ingest/rss_adapter.py — RSS News Ingest Adapter (Trading KB)

Pulls financial news headlines from free RSS feeds (Reuters, BBC Business,
CNBC, MarketWatch) and converts them to news atoms.

No API key required. No rate limits on RSS.

Atoms produced:
  - {TICKER} | catalyst       | {headline}           (when ticker detected)
  - {TICKER} | risk_factor    | {headline}           (when negative keywords)
  - financial_news | key_finding | {headline}         (general market news)

Source prefix: news_wire_<outlet>  (authority 0.60, half-life 1d)
Interval: recommended 30 min
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from ingest.base import BaseIngestAdapter, RawAtom

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
    feedparser = None  # type: ignore

_logger = logging.getLogger(__name__)


# ── RSS feed URLs ─────────────────────────────────────────────────────────────

_DEFAULT_FEEDS: Dict[str, str] = {
    'yahoo_finance_news': 'https://feeds.finance.yahoo.com/rss/2.0/headline?region=US&lang=en-US',
    'ft_home':            'https://www.ft.com/rss/home',
    'investing_com':      'https://www.investing.com/rss/news.rss',
    'bbc_business':       'http://feeds.bbci.co.uk/news/business/rss.xml',
    'cnbc_finance':       'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664',
    'marketwatch':        'http://feeds.marketwatch.com/marketwatch/topstories/',
}


# ── Ticker extraction ─────────────────────────────────────────────────────────

_UPPERCASE_STOPWORDS: Set[str] = {
    'THE', 'IS', 'AT', 'ON', 'AN', 'AND', 'OR', 'FOR', 'IN', 'OF',
    'TO', 'THAT', 'THIS', 'WITH', 'FROM', 'BY', 'ARE', 'WAS', 'BE',
    'HAS', 'HAVE', 'HAD', 'ITS', 'DO', 'DID', 'WHAT', 'HOW', 'WHY',
    'WHEN', 'WHERE', 'WHO', 'CAN', 'WILL', 'THERE', 'THEIR', 'THEY',
    'YOU', 'NOT', 'BUT', 'ALL', 'GET', 'GOT', 'NEW', 'NOW', 'OUT',
    'USE', 'WAY', 'USED', 'ALSO', 'JUST', 'INTO', 'OVER', 'COULD',
    'WOULD', 'SHOULD', 'THAN', 'THEN', 'WHICH', 'SOME', 'MORE',
    'CEO', 'CFO', 'IPO', 'GDP', 'CPI', 'FED', 'SEC', 'FBI', 'DOJ',
    'IMF', 'ECB', 'BOE', 'BOJ', 'NYSE', 'API', 'ETF', 'USA', 'EUR',
    'USD', 'GBP', 'JPY',
}


def _extract_tickers(text: str) -> List[str]:
    """Extract likely ticker symbols from text."""
    candidates = re.findall(r'\b[A-Z]{2,5}\b', text)
    return [t for t in candidates if t not in _UPPERCASE_STOPWORDS]


# ── Negative sentiment keywords (simple heuristic) ───────────────────────────

_NEGATIVE_KEYWORDS = {
    'crash', 'plunge', 'collapse', 'crisis', 'recession', 'layoff',
    'bankruptcy', 'fraud', 'investigation', 'downgrade', 'warning',
    'miss', 'decline', 'drop', 'sell-off', 'selloff', 'bear',
    'default', 'debt', 'loss', 'cut', 'slash', 'probe', 'lawsuit',
}


def _is_negative(headline: str) -> bool:
    """Check if headline contains negative sentiment keywords."""
    lower = headline.lower()
    return any(kw in lower for kw in _NEGATIVE_KEYWORDS)


class RSSAdapter(BaseIngestAdapter):
    """
    RSS news feed ingest adapter.

    Pulls headlines from financial RSS feeds and creates news atoms.
    No API key required.
    """

    def __init__(self, feeds: Optional[Dict[str, str]] = None):
        super().__init__(name='rss_news')
        self.feeds = feeds or _DEFAULT_FEEDS
        self._seen_titles: Set[str] = set()

    def fetch(self) -> List[RawAtom]:
        if not HAS_FEEDPARSER:
            self._logger.error(
                'feedparser not installed — pip install feedparser'
            )
            return []

        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for outlet_name, url in self.feeds.items():
            try:
                feed_atoms = self._fetch_feed(outlet_name, url, now_iso)
                atoms.extend(feed_atoms)
            except Exception as e:
                self._logger.warning(
                    'Failed to fetch RSS feed %s: %s', outlet_name, e
                )

        return atoms

    def _fetch_feed(
        self, outlet_name: str, url: str, now_iso: str
    ) -> List[RawAtom]:
        """Parse a single RSS feed and convert entries to atoms."""
        atoms: List[RawAtom] = []
        source = f'news_wire_{outlet_name}'

        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            self._logger.warning(
                'RSS feed %s returned no entries (bozo=%s)',
                outlet_name, feed.bozo_exception,
            )
            return atoms

        for entry in feed.entries[:20]:  # cap per feed
            title = (entry.get('title') or '').strip()
            if not title or title in self._seen_titles:
                continue

            self._seen_titles.add(title)

            # Limit memory of seen titles (prevent unbounded growth)
            if len(self._seen_titles) > 5000:
                # Keep the most recent ~2500
                self._seen_titles = set(list(self._seen_titles)[-2500:])

            link = entry.get('link', '')
            published = entry.get('published', now_iso)

            meta = {
                'url': link,
                'published': published,
                'outlet': outlet_name,
                'fetched_at': now_iso,
            }

            # Extract tickers mentioned in headline
            tickers = _extract_tickers(title)
            negative = _is_negative(title)

            if tickers:
                # Create per-ticker atoms
                for ticker in tickers[:3]:  # max 3 tickers per headline
                    predicate = 'risk_factor' if negative else 'catalyst'
                    atoms.append(RawAtom(
                        subject=ticker,
                        predicate=predicate,
                        object=f'news: {title[:250]}',
                        confidence=0.55,
                        source=source,
                        metadata=meta,
                    ))
            else:
                # General market news (no specific ticker)
                atoms.append(RawAtom(
                    subject='financial_news',
                    predicate='key_finding',
                    object=title[:250],
                    confidence=0.50,
                    source=source,
                    metadata=meta,
                ))

        return atoms
