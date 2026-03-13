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

from ingest.base import BaseIngestAdapter, RawAtom, db_connect

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
    # 'reuters_world': disabled — Reuters RSS DNS unreachable from OCI (times out every cycle)
    'bbc_world':            'http://feeds.bbci.co.uk/news/world/rss.xml',
    'al_jazeera':           'https://www.aljazeera.com/xml/rss/all.xml',
    'defense_news':         'https://www.defensenews.com/arc/outboundfeeds/rss/',
    'energy_monitor':       'https://www.energymonitor.ai/feed/',
    # UK-specific sources — FTSE 100 coverage
    'sky_business':         'https://feeds.skynews.com/feeds/rss/business.xml',
    # 'proactive_investors': disabled — returns malformed XML consistently
    # 'investegate':         disabled — returns malformed XML consistently
    'thisismoney':          'https://www.thisismoney.co.uk/money/investing/index.rss',
    # Asia-Pacific — TSE (.T), HKEX (.HK), KRX (.KS), ASX (.AX) coverage
    'nhk_business':         'https://www3.nhk.or.jp/rss/news/cat6.xml',
    'scmp_business':        'https://www.scmp.com/rss/5/feed',
    'korea_herald_biz':     'https://www.koreaherald.com/rss/020000000000.xml',
    'reuters_asia_biz':     'https://feeds.reuters.com/reuters/asiabusinessnews',
    # Australia — ASX coverage
    'abc_business_au':      'https://www.abc.net.au/news/feed/51120/rss.xml',
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

# ── Company name → exchange ticker map ───────────────────────────────────────
# Handles natural-language company names in BBC/Sky/ThisIsMoney/NHK/SCMP text.
# Keys are lowercase for case-insensitive matching.
#
# Two tiers:
#   _COMPANY_NAME_MAP        — multi-word or unique names, safe for plain
#                              substring match (no false-positive risk).
#   _COMPANY_NAME_MAP_STRICT — short/common words (bp, bt, next, vw, gsk…)
#                              that require whole-word regex matching so
#                              "the next big thing" doesn't → NXT.L.

_COMPANY_NAME_MAP: Dict[str, str] = {
    # ── LSE / FTSE (.L) ───────────────────────────────────────────────────────
    'barclays':             'BARC.L',
    'lloyds':               'LLOY.L',
    'lloyds banking':       'LLOY.L',
    'lloyds bank':          'LLOY.L',
    'hsbc':                 'HSBA.L',
    'natwest':              'NWG.L',
    'natwest group':        'NWG.L',
    'standard chartered':   'STAN.L',
    'shell plc':            'SHEL.L',
    'british petroleum':    'BP.L',
    'bp plc':               'BP.L',
    'rio tinto':            'RIO.L',
    'rio tinto plc':        'RIO.L',
    'glencore':             'GLEN.L',
    'diageo':               'DGE.L',
    'astrazeneca':          'AZN.L',
    'astra zeneca':         'AZN.L',
    'glaxosmithkline':      'GSK.L',
    'unilever':             'ULVR.L',
    'rolls-royce':          'RR.L',
    'rolls royce':          'RR.L',
    'bae systems':          'BA.L',
    'vodafone':             'VOD.L',
    'bt group':             'BT.A.L',
    'tesco':                'TSCO.L',
    'sainsburys':           'SBRY.L',
    "sainsbury's":          'SBRY.L',
    'marks and spencer':    'MKS.L',
    'national grid':        'NG.L',
    'centrica':             'CNA.L',
    'reckitt':              'RKT.L',
    'reckitt benckiser':    'RKT.L',
    'smith+nephew':         'SN.L',
    'smith & nephew':       'SN.L',
    'imperial brands':      'IMB.L',
    'british american tobacco': 'BATS.L',
    'antofagasta':          'ANTO.L',
    'anglo american':       'AAL.L',
    'compass group':        'CPG.L',
    'haleon':               'HLN.L',
    'melrose':              'MRO.L',
    'prudential':           'PRU.L',
    'legal & general':      'LGEN.L',
    'aviva':                'AV.L',
    'admiral':              'ADM.L',
    'segro':                'SGRO.L',
    'land securities':      'LAND.L',
    'british land':         'BLND.L',
    'kingfisher':           'KGF.L',
    'jd sports':            'JD.L',
    'experian':             'EXPN.L',
    'relx':                 'REL.L',
    'pearson':              'PSON.L',
    'informa':              'INF.L',
    'intercontinental hotels': 'IHG.L',
    'whitbread':            'WTB.L',
    'flutter entertainment': 'FLTR.L',
    # ── XETRA (.DE) ───────────────────────────────────────────────────────────
    'deutsche bank':        'DBK.DE',
    'commerzbank':          'CBK.DE',
    'volkswagen':           'VOW3.DE',
    'mercedes-benz':        'MBG.DE',
    'siemens':              'SIE.DE',
    'allianz':              'ALV.DE',
    'bayer':                'BAYN.DE',
    'adidas':               'ADS.DE',
    'infineon':             'IFX.DE',
    'airbus':               'AIR.DE',
    'munich re':            'MUV2.DE',
    'munich reinsurance':   'MUV2.DE',
    'continental':          'CON.DE',
    'fresenius':            'FRE.DE',
    'henkel':               'HEN3.DE',
    # ── ASX (.AX) ─────────────────────────────────────────────────────────────
    'bhp group':            'BHP.AX',
    'commonwealth bank':    'CBA.AX',
    'westpac':              'WBC.AX',
    'anz bank':             'ANZ.AX',
    'national australia bank': 'NAB.AX',
    'macquarie':            'MQG.AX',
    'woodside':             'WDS.AX',
    'fortescue':            'FMG.AX',
    'rio tinto australia':  'RIO.AX',
    'wesfarmers':           'WES.AX',
    'woolworths':           'WOW.AX',
    'telstra':              'TLS.AX',
    'afterpay':             'SQ2.AX',
    # ── TSE (.T) ─────────────────────────────────────────────────────────────
    'toyota':               '7203.T',
    'toyota motor':         '7203.T',
    'sony':                 '6758.T',
    'sony group':           '6758.T',
    'softbank':             '9984.T',
    'softbank group':       '9984.T',
    'nintendo':             '7974.T',
    'honda':                '7267.T',
    'honda motor':          '7267.T',
    'mitsubishi':           '8058.T',
    'mitsubishi corporation': '8058.T',
    'panasonic':            '6752.T',
    'ntt docomo':           '9432.T',
    'hitachi':              '6501.T',
    'fujitsu':              '6702.T',
    'canon':                '7751.T',
    'nikon':                '7731.T',
    'kddi':                 '9433.T',
    'fast retailing':       '9983.T',
    'uniqlo':               '9983.T',
    'keyence':              '6861.T',
    'recruit holdings':     '6098.T',
    'daikin':               '6367.T',
    'shin-etsu':            '4063.T',
    'shin etsu chemical':   '4063.T',
    # ── HKEX (.HK) ───────────────────────────────────────────────────────────
    'tencent':              '0700.HK',
    'tencent holdings':     '0700.HK',
    'alibaba':              '9988.HK',
    'alibaba group':        '9988.HK',
    'meituan':              '3690.HK',
    'xiaomi':               '1810.HK',
    'jd.com':               '9618.HK',
    'netease':              '9999.HK',
    'hsbc hong kong':       '0005.HK',
    'cnooc':                '0883.HK',
    'petrochina':           '0857.HK',
    'ping an insurance':    '2318.HK',
    'china mobile':         '0941.HK',
    'byd company':          '1211.HK',
    'aia group':            '1299.HK',
    'anta sports':          '2020.HK',
    'country garden':       '2007.HK',
    # ── KRX (.KS) ─────────────────────────────────────────────────────────────
    'samsung electronics':  '005930.KS',
    'sk hynix':             '000660.KS',
    'hyundai motor':        '005380.KS',
    'lg electronics':       '066570.KS',
    'kb financial':         '105560.KS',
    'celltrion':            '068270.KS',
}

# Short / ambiguous words — require whole-word matching to avoid false positives.
# E.g. "next" must not fire on "the next big thing"; "bp" not on "bps" or "ibps".
_COMPANY_NAME_MAP_STRICT: Dict[str, str] = {
    # LSE
    'shell':    'SHEL.L',
    'bp':       'BP.L',
    'gsk':      'GSK.L',
    'bt':       'BT.A.L',
    'm&s':      'MKS.L',
    'next':     'NXT.L',
    'bat':      'BATS.L',
    'ihg':      'IHG.L',
    'flutter':  'FLTR.L',
    'aviva':    'AV.L',
    # XETRA
    'sap':      'SAP.DE',
    'vw':       'VOW3.DE',
    'bmw':      'BMW.DE',
    'mercedes': 'MBG.DE',
    'basf':     'BAS.DE',
    'puma':     'PUM.DE',
    'linde':    'LIN.DE',
    # ASX
    'bhp':      'BHP.AX',
    'cba':      'CBA.AX',
    'anz':      'ANZ.AX',
    'nab':      'NAB.AX',
    'coles':    'COL.AX',
    # TSE
    'ntt':      '9432.T',
    'nec':      '6701.T',
    'recruit':  '6098.T',
    # HKEX
    'ping an':  '2318.HK',
    'byd':      '1211.HK',
    'aia':      '1299.HK',
    # KRX
    'samsung':  '005930.KS',
    'hynix':    '000660.KS',
    'hyundai':  '005380.KS',
    'kia':      '000270.KS',
    'posco':    '005490.KS',
    'kakao':    '035720.KS',
    'naver':    '035420.KS',
}

# Pre-compile word-boundary patterns for the strict map (once at import time)
_STRICT_PATTERNS: List[tuple] = [
    (re.compile(r'(?<![a-z])' + re.escape(name) + r'(?![a-z])', re.IGNORECASE), ticker)
    for name, ticker in _COMPANY_NAME_MAP_STRICT.items()
]


def _extract_tickers(text: str) -> List[str]:
    """Extract likely ticker symbols from text.

    Three passes:
    1. Safe company-name map — multi-word / unique names, plain substring match.
    2. Strict company-name map — short/ambiguous words, word-boundary regex.
    3. Regex — explicit exchange-suffix symbols already in the text
       (.L .DE .AX .HK .T .KS .AS .PA …) plus bare 2-5 cap US tickers.
    """
    result: List[str] = []
    seen: set = set()
    text_lower = text.lower()

    # Pass 1 — safe names (longest first to avoid partial overlaps)
    for name in sorted(_COMPANY_NAME_MAP, key=len, reverse=True):
        if name in text_lower:
            ticker = _COMPANY_NAME_MAP[name]
            if ticker not in seen:
                seen.add(ticker)
                result.append(ticker)

    # Pass 2 — strict / ambiguous names (whole-word boundary match)
    for pattern, ticker in _STRICT_PATTERNS:
        if ticker not in seen and pattern.search(text):
            seen.add(ticker)
            result.append(ticker)

    # Pass 3 — regex for explicit ticker symbols
    # Matches exchange-suffix tickers (e.g. 7203.T, 0700.HK) and bare caps
    candidates = re.findall(
        r'\b([A-Z0-9]{1,6}'
        r'(?:\.(?:L|DE|AX|HK|T|KS|AS|PA|MI|SW|ST|TO|NZ|CO|BR))'
        r'|[A-Z]{2,5})\b',
        text,
    )
    for t in candidates:
        base = re.sub(r'\.[A-Z]+$', '', t)
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
                conn = db_connect(self._db_path)
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
