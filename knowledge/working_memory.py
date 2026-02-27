"""
knowledge/working_memory.py — On-Demand Fetch → Working Memory → KB Commit Loop

When the retrieval layer finds no atoms for a ticker, the chat endpoint calls
fetch_on_demand() to pull live data for that ticker into a per-session working
memory store.  After the LLM response is built, commit_session() decides which
atoms are worth writing back to the persistent KB (confidence ≥ 0.70).

Design:
  - Sessions are in-memory dicts — ephemeral by design, no DB overhead
  - fetch_on_demand caps at 2 tickers per request to bound latency
  - yf.fast_info for price (~300ms), yf.info for direction/regime (~500ms)
  - Recent headlines come from existing KB catalyst atoms (no new HTTP call)
  - Commit writes via kg.add_fact() — same path as all scheduled ingest
  - Staleness: atom is "missing" if KB has 0 last_price rows for ticker

Zero-LLM, pure Python.
"""

from __future__ import annotations

import logging
import re as _re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import quote_plus

_logger = logging.getLogger(__name__)

# Only commit atoms at or above this confidence level back to the persistent KB
_COMMIT_THRESHOLD = 0.70

# Max on-demand fetches per chat request (latency guard)
MAX_ON_DEMAND_TICKERS = 6

# ── LLM-initiated fetch support ───────────────────────────────────────────────

DATA_REQUEST_SYSTEM_PROMPT = """\
You are a trading intelligence system. Given a user question and knowledge context,
decide if you have enough data to answer well.

If you have enough context: respond with exactly:
ANSWER: <your full answer here>

If you are missing live price, signal, or news data for a specific ticker that would
materially improve your answer: respond with exactly:
DATA_REQUEST: <TICKER1>,<TICKER2>

If the context says 'No KB signals available' for one or more holdings, and you need
current news or context to answer the question, respond with exactly:
SEARCH_REQUEST: <concise search query, e.g. 'NVDA Nvidia news 2025'>

Rules:
- Only request DATA_REQUEST if the ticker is directly relevant to the question
- Only request tickers — no free text after DATA_REQUEST
- If context is empty but the question is general, just ANSWER with what you know from context
- Maximum 2 tickers per DATA_REQUEST
- LIVE PRICE RULE: If the user is asking for the CURRENT price, rate, level, or value of
  a ticker (keywords: current, now, today, live, latest, trading at, worth, rate, spot),
  you MUST issue DATA_REQUEST for that ticker even if stale last_price atoms already exist
  in context — the KB data may be hours or days old.
- TICKER FORMAT for DATA_REQUEST: use the KB ticker format exactly as it appears in the
  context (e.g. XAUUSD for gold, XAGUSD for silver, GBPUSD for cable, SPX for S&P 500,
  EURUSD for EUR/USD). Do NOT use yfinance formats like GC=F or ^GSPC.
- Prefer SEARCH_REQUEST over refusing to answer when KB signals are absent
- Only one SEARCH_REQUEST per response — make it specific and focused
"""

def parse_llm_response(text: str) -> tuple[str, list[str]]:
    """
    Parse a pass-1 LLM response.
    Returns (mode, payload) where mode is 'answer', 'data_request', or 'search_request'.
    - 'answer':         payload[0] is the answer text
    - 'data_request':   payload is list of ticker strings
    - 'search_request': payload[0] is the search query string
    """
    text = (text or '').strip()
    if text.upper().startswith('DATA_REQUEST:'):
        raw = text[len('DATA_REQUEST:'):].strip()
        tickers = [t.strip().upper() for t in _re.split(r'[,\s]+', raw) if t.strip()]
        return 'data_request', tickers[:MAX_ON_DEMAND_TICKERS]
    if text.upper().startswith('SEARCH_REQUEST:'):
        query = text[len('SEARCH_REQUEST:'):].strip()
        return 'search_request', [query]
    if text.upper().startswith('ANSWER:'):
        return 'answer', [text[len('ANSWER:'):].strip()]
    # If model didn't follow format, treat the whole thing as an answer
    return 'answer', [text]

# A ticker is considered "missing" from the KB if it has fewer than this many
# last_price atoms (catches both brand-new and recently-cleared tickers)
_MISSING_THRESHOLD = 1


# Maps KB/display tickers → yfinance ticker symbols for on-demand fetch
_YF_TICKER_MAP: dict[str, str] = {
    # Precious metals
    'XAUUSD': 'GC=F',   'GOLD':   'GC=F',
    'XAGUSD': 'SI=F',   'SILVER': 'SI=F',
    'XPTUSD': 'PL=F',
    'XPDUSD': 'PA=F',
    'XCUUSD': 'HG=F',
    # Energy
    'CL':     'CL=F',   'CRUDE':  'CL=F',   'OIL':    'CL=F',
    'BZ':     'BZ=F',   'BRENT':  'BZ=F',
    'NG':     'NG=F',   'NATGAS': 'NG=F',
    # Agriculture
    'ZW':     'ZW=F',   'ZC':     'ZC=F',   'ZS':     'ZS=F',
    'KC':     'KC=F',   'SB':     'SB=F',   'CC':     'CC=F',
    # Forex pairs
    'GBPUSD': 'GBPUSD=X', 'EURUSD': 'EURUSD=X', 'USDJPY': 'JPY=X',
    'GBPEUR': 'GBPEUR=X', 'AUDUSD': 'AUDUSD=X', 'USDCAD': 'CAD=X',
    'USDCHF': 'CHF=X',   'NZDUSD': 'NZDUSD=X', 'EURGBP': 'EURGBP=X',
    'EURJPY': 'EURJPY=X','GBPJPY': 'GBPJPY=X', 'USDCNH': 'CNH=X',
    'DXY':    'DX-Y.NYB',
    # Indices (KB name → yfinance)
    'SPX':    '^GSPC',   'NDX':    '^NDX',    'DJI':    '^DJI',
    'FTSE':   '^FTSE',   'DAX':    '^GDAXI',  'CAC':    '^FCHI',
    'NI225':  '^N225',   'HSI':    '^HSI',    'ASX':    '^AXJO',
    'SX5E':   '^STOXX50E', 'VIX':  '^VIX',   'MCX':    '^MCX',
    # Crypto (KB ticker → yfinance)
    'BTC':    'BTC-USD', 'BITCOIN': 'BTC-USD', 'BTCUSD': 'BTC-USD',
    'ETH':    'ETH-USD', 'ETHEREUM':'ETH-USD', 'ETHUSD': 'ETH-USD',
    'SOL':    'SOL-USD', 'XRP':    'XRP-USD', 'BNB':    'BNB-USD',
    'ADA':    'ADA-USD', 'DOGE':   'DOGE-USD','AVAX':   'AVAX-USD',
    'DOT':    'DOT-USD', 'LINK':   'LINK-USD','MATIC':  'MATIC-USD',
    'LTC':    'LTC-USD', 'BCH':    'BCH-USD', 'SHIB':   'SHIB-USD',
    'UNI':    'UNI-USD', 'ATOM':   'ATOM-USD','XLM':    'XLM-USD',
    # Rebranded / renamed tickers
    'SQ':     'XYZ',    # Block Inc. (formerly Square) rebranded ticker
    # Pass-through: already-resolved yfinance symbols (from TICKER_ALIASES)
    'BTC-USD':'BTC-USD', 'ETH-USD':'ETH-USD', 'SOL-USD':'SOL-USD',
    'XRP-USD':'XRP-USD', 'BNB-USD':'BNB-USD', 'ADA-USD':'ADA-USD',
    'DOGE-USD':'DOGE-USD','AVAX-USD':'AVAX-USD','GBPUSD=X':'GBPUSD=X',
    'EURUSD=X':'EURUSD=X','GBP=X':  'GBP=X',  'JPY=X':  'JPY=X',
    'GC=F':   'GC=F',   'SI=F':   'SI=F',    'CL=F':   'CL=F',
    'BZ=F':   'BZ=F',   'NG=F':   'NG=F',    'PL=F':   'PL=F',
    '^GSPC':  '^GSPC',  '^NDX':   '^NDX',    '^FTSE':  '^FTSE',
    '^VIX':   '^VIX',   '^DJI':   '^DJI',
}


def _extract_ticker_hint(query: str) -> str:
    """
    Best-effort extraction of a ticker symbol from a free-text search query.
    Returns the first ALL-CAPS word that looks like a ticker, else 'web_search'.
    """
    tokens = query.split()
    for tok in tokens:
        clean = tok.strip('.,;:()').upper()
        if 2 <= len(clean) <= 6 and clean.isalpha():
            return clean
    return 'web_search'


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class CommitResult:
    committed: int = 0
    discarded: int = 0
    tickers:   List[str] = field(default_factory=list)


@dataclass
class _Session:
    session_id: str
    atoms:      List[dict] = field(default_factory=list)
    fetch_log:  List[str]  = field(default_factory=list)


# ── Staleness check ────────────────────────────────────────────────────────────

def kb_has_atoms(ticker: str, db_path: str) -> bool:
    """Return True if the KB has at least one last_price atom for this ticker."""
    try:
        conn = sqlite3.connect(db_path, timeout=3)
        row = conn.execute(
            "SELECT COUNT(*) FROM facts WHERE LOWER(subject) = ? AND predicate = 'last_price'",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        return (row[0] if row else 0) >= _MISSING_THRESHOLD
    except Exception:
        return True  # on error assume present — don't add latency


# ── Helpers ────────────────────────────────────────────────────────────────────

def _price_regime_from_ratio(ratio: float) -> str:
    """Map 52-week position ratio to a price_regime label."""
    if ratio >= 0.85:
        return 'near_52w_high'
    if ratio <= 0.20:
        return 'near_52w_low'
    return 'mid_range'


def _direction_from_target(current: float, target: float) -> str:
    if target <= 0 or current <= 0:
        return 'neutral'
    pct = (target - current) / current
    if pct > 0.10:
        return 'long'
    if pct < -0.10:
        return 'short'
    return 'neutral'


def _should_commit(atom: dict) -> bool:
    pred = atom.get('predicate', '')
    if pred in ('last_price', 'price_regime', 'signal_direction', 'market_cap_tier'):
        return True
    return atom.get('confidence', 0.0) >= _COMMIT_THRESHOLD


# ── WorkingMemory ──────────────────────────────────────────────────────────────

class WorkingMemory:
    """
    Per-process singleton (created once in api.py).
    Thread-safe for reads; GIL protects the dict writes on CPython.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}

    # ── Session lifecycle ──────────────────────────────────────────────────────

    def open_session(self, session_id: str) -> None:
        if session_id not in self._sessions:
            self._sessions[session_id] = _Session(session_id=session_id)

    def close_without_commit(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    # ── On-demand fetch ────────────────────────────────────────────────────────

    def fetch_on_demand(self, ticker: str, session_id: str, db_path: str) -> List[dict]:
        """
        Fetch live price + fundamentals for a ticker absent from the KB.
        Atoms are stored in the session only — not written to KB yet.
        Returns the list of new atoms fetched.
        """
        try:
            import yfinance as yf
        except ImportError:
            _logger.warning('yfinance not installed — on-demand fetch skipped')
            return []

        session = self._sessions.get(session_id)
        if session is None:
            self.open_session(session_id)
            session = self._sessions[session_id]

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[dict] = []

        # Resolve KB ticker to yfinance symbol (commodities, forex, indices use different formats)
        yf_symbol = _YF_TICKER_MAP.get(ticker.upper(), ticker)

        try:
            t = yf.Ticker(yf_symbol)
            fi = t.fast_info

            # last_price
            price = getattr(fi, 'last_price', None) or getattr(fi, 'regularMarketPrice', None)
            if price:
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'last_price',
                    'object':     f'{price:.4f}',
                    'confidence': 0.95,
                    'source':     'exchange_feed_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })

            # price_regime from 52w position
            high_52w = getattr(fi, 'year_high', None) or getattr(fi, 'fiftyTwoWeekHigh', None)
            low_52w  = getattr(fi, 'year_low',  None) or getattr(fi, 'fiftyTwoWeekLow',  None)
            if price and high_52w and low_52w and high_52w > low_52w:
                ratio = (price - low_52w) / (high_52w - low_52w)
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'price_regime',
                    'object':     _price_regime_from_ratio(ratio),
                    'confidence': 0.85,
                    'source':     'exchange_feed_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })

        except Exception as e:
            _logger.debug('fast_info fetch failed for %s: %s', ticker, e)

        # Analyst target → signal_direction (slower path, best-effort)
        try:
            t = yf.Ticker(yf_symbol)
            info = t.info
            target = info.get('targetMeanPrice') or info.get('targetMedianPrice')
            price_now = info.get('regularMarketPrice') or info.get('currentPrice')
            if target and price_now:
                direction = _direction_from_target(float(price_now), float(target))
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'signal_direction',
                    'object':     direction,
                    'confidence': 0.75,
                    'source':     'broker_research_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })
                upside = round((float(target) - float(price_now)) / float(price_now) * 100, 1)
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'upside_pct',
                    'object':     str(upside),
                    'confidence': 0.75,
                    'source':     'broker_research_on_demand_yf',
                    'fetched_at': now_iso,
                    'upsert':     True,
                })
        except Exception as e:
            _logger.debug('info fetch failed for %s: %s', ticker, e)

        # Recent headlines — query existing KB catalyst atoms (no new HTTP call)
        try:
            conn = sqlite3.connect(db_path, timeout=3)
            rows = conn.execute(
                """SELECT object, confidence, source FROM facts
                   WHERE LOWER(subject) = ? AND predicate IN ('catalyst','risk_factor')
                   ORDER BY confidence DESC LIMIT 3""",
                (ticker.lower(),),
            ).fetchall()
            conn.close()
            for obj, conf, src in rows:
                atoms.append({
                    'subject':    ticker,
                    'predicate':  'catalyst',
                    'object':     obj,
                    'confidence': float(conf),
                    'source':     src,
                    'fetched_at': now_iso,
                    'upsert':     False,
                })
        except Exception as e:
            _logger.debug('KB catalyst lookup failed for %s: %s', ticker, e)

        session.atoms.extend(atoms)
        session.fetch_log.append(f'{ticker} fetched at {now_iso} ({len(atoms)} atoms)')
        _logger.info('on-demand fetch: %s → %d atoms', ticker, len(atoms))
        return atoms

    # ── Web search on-demand ───────────────────────────────────────────────────

    def web_search_on_demand(self, query: str, session_id: str) -> List[dict]:
        """
        Search the web for query when KB has no signals for a ticker.
        Primary: DuckDuckGo HTML endpoint (no API key).
        Fallback: Google News RSS.
        Atoms stored in session at confidence=0.65 (below commit threshold).
        Returns the list of new atoms added.
        """
        session = self._sessions.get(session_id)
        if session is None:
            self.open_session(session_id)
            session = self._sessions[session_id]

        atoms = self._ddg_search(query, session_id) or self._google_news_fallback(query, session_id)
        if atoms:
            session.atoms.extend(atoms)
            session.fetch_log.append(f'web search "{query[:60]}" → {len(atoms)} snippets')
            _logger.info('web_search_on_demand: "%s" → %d atoms', query, len(atoms))
        else:
            _logger.warning('web_search_on_demand: no results for "%s"', query)
        return atoms

    def _ddg_search(self, query: str, session_id: str) -> List[dict]:
        """Scrape DuckDuckGo HTML endpoint. Returns empty list on CAPTCHA or error."""
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError:
            _logger.warning('requests/beautifulsoup4 not installed — DDG search skipped')
            return []

        _DDG_HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-GB,en;q=0.9',
        }
        try:
            time.sleep(0.5)
            resp = requests.post(
                'https://html.duckduckgo.com/html/',
                data={'q': query, 'b': '', 'kl': 'uk-en'},
                headers=_DDG_HEADERS,
                timeout=8,
            )
            if 'challenge-form' in resp.text or resp.status_code != 200:
                _logger.warning('DuckDuckGo returned CAPTCHA or error (status=%s) — trying RSS fallback', resp.status_code)
                return []
            soup = BeautifulSoup(resp.text, 'html.parser')
            results = soup.select('.result__body')[:5]
            if not results:
                results = soup.select('.results_links')[:5]
            atoms = []
            now_iso = datetime.now(timezone.utc).isoformat()
            ticker_hint = _extract_ticker_hint(query)
            for r in results:
                title_el   = r.select_one('.result__title') or r.select_one('a.result__a')
                snippet_el = r.select_one('.result__snippet')
                title   = title_el.get_text(strip=True)   if title_el   else ''
                snippet = snippet_el.get_text(strip=True) if snippet_el else ''
                text = f'{title}: {snippet}' if snippet else title
                if not text:
                    continue
                atoms.append({
                    'subject':    ticker_hint,
                    'predicate':  'news_snippet',
                    'object':     text[:400],
                    'confidence': 0.65,
                    'source':     'web_search_ddg',
                    'fetched_at': now_iso,
                    'upsert':     False,
                })
            _logger.info('DDG search: "%s" → %d results', query, len(atoms))
            return atoms
        except Exception as e:
            _logger.warning('DDG search failed: %s', e)
            return []

    def _google_news_fallback(self, query: str, session_id: str) -> List[dict]:
        """Fetch Google News RSS as fallback when DDG is blocked."""
        try:
            import requests
            import xml.etree.ElementTree as ET
        except ImportError:
            _logger.warning('requests not installed — Google News RSS fallback skipped')
            return []

        _GNEWS_RSS = 'https://news.google.com/rss/search?q={q}&hl=en-GB&gl=GB&ceid=GB:en'
        try:
            resp = requests.get(
                _GNEWS_RSS.format(q=quote_plus(query)),
                timeout=8,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; TradingGalaxy/1.0)'},
            )
            if resp.status_code != 200:
                _logger.warning('Google News RSS returned status %s', resp.status_code)
                return []
            root = ET.fromstring(resp.text)
            items = root.findall('.//item')[:5]
            atoms = []
            now_iso = datetime.now(timezone.utc).isoformat()
            ticker_hint = _extract_ticker_hint(query)
            for item in items:
                title   = (item.findtext('title')       or '').strip()
                desc    = (item.findtext('description') or '').strip()
                text = f'{title}: {desc}' if desc else title
                if not text:
                    continue
                atoms.append({
                    'subject':    ticker_hint,
                    'predicate':  'news_snippet',
                    'object':     text[:400],
                    'confidence': 0.65,
                    'source':     'web_search_gnews',
                    'fetched_at': now_iso,
                    'upsert':     False,
                })
            _logger.info('Google News RSS: "%s" → %d results', query, len(atoms))
            return atoms
        except Exception as e:
            _logger.warning('Google News RSS fallback failed: %s', e)
            return []

    # ── Session context for prompt injection ──────────────────────────────────

    def get_session_snippet(self, session_id: str) -> str:
        """
        Return session atoms as a formatted context string for LLM injection.
        Returns empty string if no session atoms.
        """
        session = self._sessions.get(session_id)
        if not session or not session.atoms:
            return ''

        now_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        lines = [f'=== LIVE DATA (fetched live at {now_ts} — treat as current) ===']
        for a in session.atoms:
            ts = a.get('fetched_at', '')[:16]
            lines.append(
                f"{a['subject']} | {a['predicate']} | {a['object']}"
                f"  [conf:{a['confidence']:.2f}, fetched:{ts}]"
            )
        if session.fetch_log:
            lines.append(f'Fetch log: {"; ".join(session.fetch_log)}')
        return '\n'.join(lines)

    def get_fetched_tickers(self, session_id: str) -> List[str]:
        session = self._sessions.get(session_id)
        if not session:
            return []
        return list({
            a['subject'] for a in session.atoms
            if 'fetched_at' in a
        })

    # ── Commit ────────────────────────────────────────────────────────────────

    def commit_session(self, session_id: str, kg) -> CommitResult:
        """
        Write high-confidence session atoms back to the persistent KB via kg.add_fact().
        Cleans up the session afterwards.
        """
        session = self._sessions.get(session_id)
        if not session:
            return CommitResult()

        result = CommitResult()
        committed_tickers: set = set()

        for atom in session.atoms:
            if not _should_commit(atom):
                result.discarded += 1
                continue
            try:
                ok = kg.add_fact(
                    subject=atom['subject'],
                    predicate=atom['predicate'],
                    object=atom['object'],
                    confidence=atom['confidence'],
                    source=atom['source'],
                    metadata={'fetched_at': atom.get('fetched_at', ''), 'on_demand': True},
                    upsert=atom.get('upsert', False),
                )
                if ok:
                    result.committed += 1
                    committed_tickers.add(atom['subject'])
                else:
                    result.discarded += 1
            except Exception as e:
                _logger.debug('commit failed for atom %s|%s: %s',
                              atom['subject'], atom['predicate'], e)
                result.discarded += 1

        result.tickers = list(committed_tickers)
        self._sessions.pop(session_id, None)
        if result.committed:
            _logger.info('commit_session %s: committed=%d discarded=%d tickers=%s',
                         session_id, result.committed, result.discarded, result.tickers)
        return result
