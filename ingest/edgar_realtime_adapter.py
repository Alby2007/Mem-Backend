"""
ingest/edgar_realtime_adapter.py — Real-Time SEC EDGAR 8-K Poller

Polls the SEC's real-time 8-K Atom feed every 3 minutes (vs the 6-hour
interval of the full EDGARAdapter). Gets material events — earnings surprises,
FDA decisions, merger announcements — into the KB within minutes of filing.

FEED
====
https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&
    dateb=&owner=include&count=40&search_text=&output=atom

Publishes the 40 most recent 8-K filings in Atom/XML format. Updated within
minutes of each new filing at the SEC.

DEDUPLICATION
=============
In-memory set `_seen_ids` is populated from the `edgar_realtime_seen` DB table
on __init__ — not just written to it. This ensures that on server restart the
adapter does not re-ingest filings it already processed in a previous run.

WATCHLIST MATCHING
==================
Matches company names in the feed against a reverse-lookup dict built from the
known ticker→company mapping. Partial name matching (company name CONTAINS a
known ticker's company fragment) to handle SEC's verbose entity names.

ATOMS PRODUCED
==============
  {TICKER} | catalyst | 8-K filed: {company} — {title}
  source: regulatory_filing_sec_realtime  (authority 0.95, same as EDGARAdapter)

Also queues each matched filing into extraction_queue for LLM processing.

INTERVAL
========
180s (3 minutes) — fast enough to catch material events within one filing
cycle. Well under SEC rate limits (10 req/s).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional, Set

import requests

from ingest.base import BaseIngestAdapter, RawAtom, db_connect
# extraction_queue disabled — dead storage, never consumed by LLM adapter

_logger = logging.getLogger(__name__)

# SEC real-time 8-K Atom feed
_FEED_URL = (
    'https://www.sec.gov/cgi-bin/browse-edgar'
    '?action=getcurrent&type=8-K&dateb=&owner=include'
    '&count=40&search_text=&output=atom'
)

# Atom XML namespace
_ATOM_NS = 'http://www.w3.org/2005/Atom'

# Deduplication table name
_SEEN_TABLE = 'edgar_realtime_seen'

# Source prefix — same authority tier as full EDGAR adapter
_SOURCE = 'regulatory_filing_sec_realtime'

# SEC requires a User-Agent header identifying the application
_USER_AGENT = os.environ.get(
    'EDGAR_USER_AGENT',
    'TradingGalaxyKB admin@tradinggalaxy.dev',
)

# Request timeout
_TIMEOUT = 15

# Known ticker → company name fragments for watchlist matching
# Built from the default EDGAR ticker list; extend as needed
_TICKER_COMPANY_FRAGMENTS = {
    'AAPL': ['apple'],
    'MSFT': ['microsoft'],
    'GOOGL': ['alphabet', 'google'],
    'AMZN': ['amazon'],
    'NVDA': ['nvidia'],
    'META': ['meta platforms', 'facebook'],
    'TSLA': ['tesla'],
    'AVGO': ['broadcom'],
    'JPM': ['jpmorgan', 'jp morgan'],
    'V': ['visa'],
    'MA': ['mastercard'],
    'BAC': ['bank of america'],
    'GS': ['goldman sachs'],
    'MS': ['morgan stanley'],
    'UNH': ['unitedhealth', 'united health'],
    'JNJ': ['johnson & johnson', 'johnson and johnson'],
    'LLY': ['eli lilly'],
    'ABBV': ['abbvie'],
    'PFE': ['pfizer'],
    'MRK': ['merck'],
    'XOM': ['exxon mobil', 'exxonmobil'],
    'CVX': ['chevron'],
    'COP': ['conoco'],
    'WMT': ['walmart'],
    'COST': ['costco'],
    'MCD': ["mcdonald's", 'mcdonalds'],
    'CAT': ['caterpillar'],
    'HON': ['honeywell'],
    'RTX': ['raytheon', 'rtx corporation'],
    'DIS': ['disney'],
    'NFLX': ['netflix'],
    'CMCSA': ['comcast'],
    'AMD': ['advanced micro devices'],
    'INTC': ['intel'],
    'QCOM': ['qualcomm'],
    'CRM': ['salesforce'],
    'ADBE': ['adobe'],
    'NOW': ['servicenow'],
    'SNOW': ['snowflake'],
    'PYPL': ['paypal'],
    'COIN': ['coinbase'],
    'BLK': ['blackrock'],
    'SCHW': ['charles schwab'],
    'AXP': ['american express'],
    'BRK-B': ['berkshire hathaway'],
    'CVS': ['cvs health'],
    'ABBV': ['abbvie'],
    'GILD': ['gilead'],
    'NEE': ['nextera'],
    'DUK': ['duke energy'],
    'SO': ['southern company'],
    'AMT': ['american tower'],
    'PLD': ['prologis'],
    'EQIX': ['equinix'],
}

# Build reverse lookup: fragment → ticker (lowercase fragment → ticker)
_FRAGMENT_TO_TICKER: dict = {}
for _ticker, _fragments in _TICKER_COMPANY_FRAGMENTS.items():
    for _frag in _fragments:
        _FRAGMENT_TO_TICKER[_frag.lower()] = _ticker


def _ensure_seen_table(conn: sqlite3.Connection) -> None:
    """Create edgar_realtime_seen table if it doesn't exist."""
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_SEEN_TABLE} (
            filing_id   TEXT PRIMARY KEY,
            company     TEXT,
            ticker      TEXT,
            seen_at     TEXT NOT NULL
        )
    """)
    conn.commit()


def _load_seen_ids(conn: sqlite3.Connection) -> Set[str]:
    """Load all previously seen filing IDs from the DB."""
    _ensure_seen_table(conn)
    c = conn.cursor()
    c.execute(f"SELECT filing_id FROM {_SEEN_TABLE}")
    return {row[0] for row in c.fetchall()}


def _mark_seen(conn: sqlite3.Connection, filing_id: str,
               company: str, ticker: Optional[str]) -> None:
    """Record a filing ID as processed."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        f"INSERT OR IGNORE INTO {_SEEN_TABLE} (filing_id, company, ticker, seen_at) "
        f"VALUES (?, ?, ?, ?)",
        (filing_id, company, ticker, now_iso),
    )
    conn.commit()


def _match_ticker(company_name: str) -> Optional[str]:
    """
    Match a SEC company name to a watchlist ticker.
    Returns the first matching ticker or None.
    """
    name_lower = company_name.lower()
    for fragment, ticker in _FRAGMENT_TO_TICKER.items():
        if fragment in name_lower:
            return ticker
    return None


def _parse_feed(xml_text: str) -> list:
    """
    Parse the SEC EDGAR Atom feed XML.
    Returns list of dicts: {id, title, company, updated, link}
    """
    entries = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        _logger.error('edgar_realtime: XML parse error: %s', e)
        return entries

    ns = {'atom': _ATOM_NS}
    for entry in root.findall('atom:entry', ns):
        filing_id = (entry.findtext('atom:id', namespaces=ns) or '').strip()
        title     = (entry.findtext('atom:title', namespaces=ns) or '').strip()
        updated   = (entry.findtext('atom:updated', namespaces=ns) or '').strip()
        link_el   = entry.find('atom:link', ns)
        link      = link_el.get('href', '') if link_el is not None else ''

        # Company name is typically the first part of the title before ' ('
        company = title.split('(')[0].strip() if '(' in title else title

        if filing_id:
            entries.append({
                'id':      filing_id,
                'title':   title,
                'company': company,
                'updated': updated,
                'link':    link,
            })

    return entries


class EDGARRealtimeAdapter(BaseIngestAdapter):
    """
    Real-time SEC EDGAR 8-K Atom feed poller.

    Polls every 180s (3 min) and deduplicates via the edgar_realtime_seen
    table. In-memory seen set is populated from the DB on init — safe across
    server restarts.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='edgar_realtime')
        self._db_path = db_path

        # Populate in-memory seen set from DB on init (restart-safe)
        conn = db_connect(db_path)
        try:
            self._seen_ids: Set[str] = _load_seen_ids(conn)
            pass  # extraction_queue table init removed
        finally:
            conn.close()

        _logger.info(
            'edgar_realtime: initialised with %d known filing IDs from DB',
            len(self._seen_ids),
        )

    def fetch(self) -> list:
        now_iso = datetime.now(timezone.utc).isoformat()

        # ── Fetch the Atom feed ───────────────────────────────────────────────
        try:
            resp = requests.get(
                _FEED_URL,
                headers={'User-Agent': _USER_AGENT},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            _logger.warning('edgar_realtime: feed fetch failed: %s', e)
            return []

        entries = _parse_feed(resp.text)
        if not entries:
            _logger.debug('edgar_realtime: no entries in feed')
            return []

        _logger.info('edgar_realtime: %d entries in feed', len(entries))

        atoms: list = []
        new_count = 0

        conn = db_connect(self._db_path)
        try:
            for entry in entries:
                filing_id = entry['id']

                # Skip already-seen filings (in-memory check first, fast)
                if filing_id in self._seen_ids:
                    continue

                company = entry['company']
                ticker  = _match_ticker(company)

                # Mark seen in memory + DB before processing
                self._seen_ids.add(filing_id)
                _mark_seen(conn, filing_id, company, ticker)

                if ticker is None:
                    # Not on our watchlist — record dedup but skip atom
                    continue

                title = entry['title']
                link  = entry['link']

                # ── Emit catalyst atom ────────────────────────────────────────
                obj = f"8-K filed: {company} — {title}"[:250]
                atoms.append(RawAtom(
                    subject   = ticker.lower(),
                    predicate = 'catalyst',
                    object    = obj,
                    confidence = 0.90,
                    source    = _SOURCE,
                    metadata  = {
                        'filing_id': filing_id,
                        'company':   company,
                        'link':      link,
                        'updated':   entry['updated'],
                        'as_of':     now_iso,
                    },
                    upsert    = False,  # Each 8-K is a distinct event
                ))

                # extraction_queue writes disabled — dead storage

                new_count += 1

            conn.commit()

        finally:
            conn.close()

        _logger.info(
            'edgar_realtime: %d new matched filings → %d atoms',
            new_count, len(atoms),
        )
        return atoms
