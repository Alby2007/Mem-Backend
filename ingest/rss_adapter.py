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
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    # Core financial news
    'bbc_business':         'http://feeds.bbci.co.uk/news/business/rss.xml',
    'cnbc_finance':         'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664',
    'marketwatch':          'http://feeds.marketwatch.com/marketwatch/topstories/',
    # Yahoo Finance
    'yahoo_finance':        'https://finance.yahoo.com/rss/topfinstories',
    # Seeking Alpha
    'seeking_alpha_market': 'https://seekingalpha.com/market_currents.xml',
    # WSJ (public)
    'wsj_markets':          'https://feeds.a.dj.com/rss/RSSMarketsMain.xml',
    # Motley Fool
    'motley_fool':          'https://www.fool.com/feeds/index.aspx',
    # CNBC earnings
    'cnbc_earnings':        'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135',
    # Geopolitical & macro — leading indicators for defence/energy/EM tickers
    'reuters_world':        'https://feeds.reuters.com/reuters/worldNews',
    'bbc_world':            'http://feeds.bbci.co.uk/news/world/rss.xml',
    'al_jazeera':           'https://www.aljazeera.com/xml/rss/all.xml',
    'defense_news':         'https://www.defensenews.com/arc/outboundfeeds/rss/',
    'energy_monitor':       'https://www.energymonitor.ai/feed/',
    # UK-specific sources — FTSE 100 coverage
    'sky_business':         'https://feeds.skynews.com/feeds/rss/business.xml',
    # 'proactive_investors': disabled — returns malformed XML consistently
    # 'investegate':         disabled — returns malformed XML consistently
    'thisismoney':          'https://www.thisismoney.co.uk/money/investing/index.rss',
}

# Max parallel workers for feed fetching
_RSS_WORKERS = 8


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
    """Extract likely ticker symbols from text, including LSE .L suffix (e.g. BARC.L)."""
    # Match standard tickers (2-5 caps) AND LSE .L format
    candidates = re.findall(r'\b([A-Z]{2,5}\.L|[A-Z]{2,5})\b', text)
    result = []
    seen = set()
    for t in candidates:
        # For .L tickers, check the base symbol isn't a stopword
        base = t[:-2] if t.endswith('.L') else t
        if base not in _UPPERCASE_STOPWORDS and t not in seen:
            seen.add(t)
            result.append(t)
    return result


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


def _ensure_extraction_queue(conn: sqlite3.Connection) -> None:
    """Create extraction_queue table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS extraction_queue (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            text             TEXT NOT NULL,
            url              TEXT,
            source           TEXT,
            fetched_at       TEXT,
            processed        INTEGER DEFAULT 0,
            processed_at     TEXT,
            atoms_extracted  INTEGER DEFAULT 0,
            failed_attempts  INTEGER DEFAULT 0
        )
    """)
    conn.commit()


class RSSAdapter(BaseIngestAdapter):
    """
    RSS news feed ingest adapter.

    Pulls headlines from financial RSS feeds and creates news atoms.
    No API key required.
    """

    def __init__(self, feeds: Optional[Dict[str, str]] = None, db_path: Optional[str] = None):
        super().__init__(name='rss_news')
        self.feeds = feeds or _DEFAULT_FEEDS
        self._seen_titles: Set[str] = set()
        self._db_path: Optional[str] = db_path

    def fetch(self) -> List[RawAtom]:
        if not HAS_FEEDPARSER:
            self._logger.error('feedparser not installed — pip install feedparser')
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        queue_rows: List[tuple] = []  # (text, url, source, fetched_at)

        with ThreadPoolExecutor(max_workers=_RSS_WORKERS, thread_name_prefix='rss') as ex:
            futures = {
                ex.submit(self._fetch_feed, name, url, now_iso): name
                for name, url in self.feeds.items()
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    feed_atoms, feed_queue = future.result(timeout=15)
                    atoms.extend(feed_atoms)
                    queue_rows.extend(feed_queue)
                except Exception as e:
                    self._logger.warning('Failed to fetch RSS feed %s: %s', name, e)

        if queue_rows and self._db_path:
            try:
                conn = sqlite3.connect(self._db_path)
                _ensure_extraction_queue(conn)
                conn.executemany(
                    "INSERT INTO extraction_queue (text, url, source, fetched_at) VALUES (?,?,?,?)",
                    queue_rows,
                )
                conn.commit()
                conn.close()
            except Exception as e:
                self._logger.warning('Failed to write extraction_queue: %s', e)

        return atoms

    def _fetch_feed(
        self, outlet_name: str, url: str, now_iso: str
    ):
        """Parse a single RSS feed and convert entries to atoms.

        Returns (atoms, queue_rows) tuple.
        """
        atoms: List[RawAtom] = []
        queue_rows: List[tuple] = []
        source = f'news_wire_{outlet_name}'

        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            self._logger.warning(
                'RSS feed %s returned no entries (bozo=%s)',
                outlet_name, feed.bozo_exception,
            )
            return atoms, queue_rows

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

            # Queue raw text for LLM extraction pass
            summary = (entry.get('summary') or entry.get('description') or '').strip()
            raw_text = f"{title}. {summary}"[:800] if summary else title[:800]
            queue_rows.append((raw_text, link, source, now_iso))

        return atoms, queue_rows
