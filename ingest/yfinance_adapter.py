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

import concurrent.futures
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Sentinel objects for non-error non-data return paths (avoids string type violations)
_SENTINEL_RATE_LIMITED = object()   # 429 rate limit — caller should back off
_SENTINEL_AUTH_FAIL    = object()   # 401/crumb failure — caller should abort

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
    'XYZ',      # Block Inc. (NYSE: XYZ — rebranded from SQ in 2024)
    # High-conviction watchlist — US mega-cap with strong KB coverage
    'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT', 'MA',
    # Additional FTSE names common in UK ISA portfolios
    'STAN.L', 'HL.L', 'IAG.L', 'PRU.L', 'EXPN.L',
    'DGE.L', 'FRES.L', 'SMT.L', 'ABDN.L', 'MNG.L',
    'IMB.L', 'SSE.L', 'SVT.L', 'SGRO.L', 'LAND.L',
]

# Parallel workers for per-ticker info() calls.
# Keep low to avoid Yahoo Finance per-IP burst rate limiting from OCI.
_MAX_WORKERS = 2

# ETF quoteType values that don't carry equity fundamentals
_ETF_QUOTE_TYPES = {'ETF', 'MUTUALFUND', 'INDEX', 'FUTURE', 'CRYPTOCURRENCY'}

# Tickers that never have fundamentals (commodities, FX pairs, indices).
# info() calls for these produce 404/empty — skip them in _fetch_info_atoms.
_NO_FUNDAMENTALS_TICKERS = {
    'XPTUSD=X', 'XAUUSD=X', 'XAGUSD=X',  # precious metal spot FX
    'GBPUSD=X', 'EURGBP=X', 'EURUSD=X', 'USDJPY=X', 'USDCHF=X',  # FX pairs
    '^FTSE', '^FTMC', '^GSPC', '^VIX', '^DJI', '^IXIC', '^RUT',   # indices
    'GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F', 'ZC=F', 'ZS=F',      # futures
    'LBS=F', 'CT=F',                                                # lumber/cotton futures — 404 on yf
}

# Tickers confirmed dead/delisted/404 on yfinance — skip OHLCV and price fetch entirely.
# These are added here rather than the DB denylist so they're filtered pre-request.
_STATIC_DEAD_TICKERS = {
    'XPTUSD',       # not a valid yf symbol (platinum is XPTUSD=X or PL=F)
    'RYA.L',        # Ryanair — listed on Dublin/NASDAQ as RYAAY, not RYA.L on LSE
    'TATAMOTORS.NS',# Indian NSE — yf 404s from OCI IP (geo-restricted)
    'SQ',           # Block Inc. rebranded ticker to XYZ in 2024
    '0011.HK',      # HK-listed — yf 404s from OCI
    'LBS=F',        # Lumber futures — extremely low liquidity, frequent 404
    'CT=F',         # Cotton futures — frequent 404 on free yf tier
}

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

# US tickers now handled by PolygonPriceAdapter (grouped daily call).
# Excluded from _bulk_download_prices() and _parallel_info_fetch() to avoid
# duplicate last_price atoms.  _cache_ohlcv_candles() still runs for these.
# Loaded lazily at fetch-time so the DB is available and the set is current.
def _get_us_polygon_tickers(db_path: Optional[str] = None) -> set:
    try:
        from ingest.polygon_price_adapter import get_us_polygon_tickers
        return get_us_polygon_tickers(db_path)
    except Exception:
        return set()


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

    Fast path: yf.download() fetches all last_price atoms in batches of
    _DOWNLOAD_BATCH_SIZE with per-batch deadlines.

    Parallel path: ThreadPoolExecutor with _MAX_WORKERS concurrent threads
    for per-ticker info() calls (fundamentals, analyst targets, beta).

    Time-series atoms (last_price, price_target, signal_direction,
    volatility_regime) are marked upsert=True so repeat runs overwrite
    the existing row from the same source rather than appending new rows.
    """

    # Process-lifetime denylist of tickers confirmed as delisted by yfinance.
    # Populated from the DB on init and updated when new delisted tickers are found.
    # Class-level so it persists across scheduler cycles.
    _delisted_cache: set = set()

    def __init__(self, tickers: Optional[List[str]] = None, db_path: Optional[str] = None):
        super().__init__(name='yfinance')
        self._db_path = db_path
        # Load persisted delisted tickers from DB
        if db_path:
            self._load_delisted_from_db(db_path)
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

    @classmethod
    def _load_delisted_from_db(cls, db_path: str) -> None:
        """Load previously-confirmed-delisted tickers from the DB denylist table."""
        try:
            import sqlite3 as _sq
            conn = _sq.connect(db_path, timeout=5)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS yfinance_delisted "
                "(ticker TEXT PRIMARY KEY, noted_at TEXT)"
            )
            conn.commit()
            rows = conn.execute("SELECT ticker FROM yfinance_delisted").fetchall()
            conn.close()
            for (t,) in rows:
                cls._delisted_cache.add(t.upper())
        except Exception:
            pass

    @classmethod
    def _mark_delisted(cls, ticker: str, db_path: Optional[str]) -> None:
        """Add a ticker to the process-level denylist and persist to DB."""
        key = ticker.upper()
        if key in cls._delisted_cache:
            return
        cls._delisted_cache.add(key)
        if not db_path:
            return
        try:
            import sqlite3 as _sq
            from datetime import datetime, timezone as _tz
            conn = _sq.connect(db_path, timeout=5)
            conn.execute(
                "INSERT OR IGNORE INTO yfinance_delisted (ticker, noted_at) VALUES (?, ?)",
                (key, datetime.now(_tz.utc).isoformat()),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    @staticmethod
    def _clear_yf_session() -> None:
        """
        Clear yfinance's cached crumb/session so the next request fetches a
        fresh one. Guards against stale crumbs set at process startup when
        Yahoo Finance may have been temporarily unreachable.
        Safe to call; all paths are wrapped in try/except.
        """
        try:
            # yfinance 0.2.x / 1.x: shared requests Session lives on yf.shared
            import yfinance.shared as _yfs
            if hasattr(_yfs, '_session') and _yfs._session is not None:
                _yfs._session.cookies.clear()
        except Exception:
            pass
        try:
            # yfinance 1.x peewee-backed cache
            from yfinance.data import YfData
            if hasattr(YfData, '_crumb'):
                YfData._crumb = None  # type: ignore[attr-defined]
        except Exception:
            pass

    def fetch(self) -> List[RawAtom]:
        if not HAS_YFINANCE:
            self._logger.error('yfinance not installed — pip install yfinance')
            return []

        import socket as _socket
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        # Clear any stale crumb/session from process startup before fetching.
        self._clear_yf_session()

        # Pre-warm the crumb by fetching a single known-good ticker before
        # launching parallel threads. This ensures all workers share a valid
        # crumb rather than each racing to establish one simultaneously.
        try:
            _prewarm = yf.Ticker('SPY')
            _ = _prewarm.fast_info.get('lastPrice')
        except Exception:
            pass

        # Apply a global socket timeout so no yfinance network call blocks indefinitely.
        # This is the primary guard against the scheduler timer chain being killed by
        # a hung HTTP connection (e.g. bad tickers returning no response from Yahoo).
        old_timeout = _socket.getdefaulttimeout()
        _socket.setdefaulttimeout(30)
        try:
            # ── 1. Bulk last_price via yf.download() ──────────────────────
            bulk_prices = self._bulk_download_prices(now_iso)
            atoms.extend(bulk_prices)

            # ── 2. Parallel per-ticker info() for fundamentals + targets ──
            info_atoms = self._parallel_info_fetch(now_iso, bulk_prices)
            atoms.extend(info_atoms)

            # ── 3. OHLCV cache — daily candles for pattern detection ───────
            # Uses Ticker.history() (same per-ticker REST path as fast_info,
            # not the blocked bulk download endpoint) so it works on OCI IPs.
            # Only runs when a db_path is configured (i.e. in production).
            if self._db_path:
                self._cache_ohlcv_candles()
        finally:
            _socket.setdefaulttimeout(old_timeout)

        self._logger.info(
            'fetch complete: %d tickers, %d total atoms (%d price, %d info)',
            len(self.tickers), len(atoms), len(bulk_prices), len(info_atoms),
        )
        return atoms

    # ── OHLCV cache ────────────────────────────────────────────────────────

    _OHLCV_DDL = """
    CREATE TABLE IF NOT EXISTS ohlcv_cache (
        ticker      TEXT NOT NULL,
        interval    TEXT NOT NULL,
        ts          TEXT NOT NULL,
        open        REAL,
        high        REAL,
        low         REAL,
        close       REAL,
        volume      REAL,
        cached_at   TEXT NOT NULL,
        PRIMARY KEY (ticker, interval, ts)
    )"""

    # Browser-like headers for direct Yahoo chart API calls.
    # /v8/finance/chart/ is NOT rate-limited like yf.download() on OCI IPs.
    _CHART_HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://finance.yahoo.com',
    }

    def _fetch_chart_candles(self, sym: str, session, interval: str = '1d', range_: str = '6mo') -> list:
        """Fetch candles via Yahoo chart API. Returns list of
        (ts_iso, open, high, low, close, volume) or empty list on error."""
        import requests as _req
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'
        params = {'range': range_, 'interval': interval, 'includeAdjustedClose': 'true'}
        try:
            r = session.get(url, params=params, headers=self._CHART_HEADERS, timeout=15)
            if r.status_code == 429:
                return _SENTINEL_RATE_LIMITED  # type: ignore[return-value]
            if r.status_code != 200:
                return []
            data = r.json()
            result = data.get('chart', {}).get('result', [])
            if not result:
                return []
            res = result[0]
            timestamps = res.get('timestamp', [])
            q = res.get('indicators', {}).get('quote', [{}])[0]
            adj = res.get('indicators', {}).get('adjclose', [{}])
            closes = (adj[0].get('adjclose') if adj else None) or q.get('close', [])
            opens = q.get('open', []); highs = q.get('high', [])
            lows  = q.get('low',  []); vols  = q.get('volume', [])
            rows = []
            for i, ts in enumerate(timestamps):
                try:
                    o = opens[i]; h = highs[i]; l = lows[i]; c = closes[i]
                    if o is None or h is None or l is None or c is None:
                        continue
                    v = vols[i] if vols and i < len(vols) else 0
                    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    rows.append((ts_iso, float(o), float(h), float(l), float(c), float(v or 0)))
                except Exception:
                    continue
            return rows
        except Exception as e:
            self._logger.debug('chart API fetch failed for %s: %s', sym, e)
            return []

    def _cache_ohlcv_candles(self) -> None:
        """Fetch 180d of daily candles for each ticker via Yahoo chart API
        (direct HTTP, not yfinance bulk download — not rate-limited on OCI IPs)
        and store in ohlcv_cache table for use by PatternAdapter."""
        import sqlite3 as _sq
        import time as _time
        import requests as _req

        try:
            conn = _sq.connect(self._db_path, timeout=15)
            conn.execute(self._OHLCV_DDL)
            conn.commit()
        except Exception as e:
            self._logger.warning('ohlcv_cache table init failed: %s', e)
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        cached = 0
        # Skip tickers that never have OHLCV data on Yahoo (spot FX derivatives)
        _ohlcv_skip = {'XPTUSD=X', 'XAUUSD=X', 'XAGUSD=X'} | {t.upper() for t in _STATIC_DEAD_TICKERS}
        active_tickers = [
            t for t in self.tickers
            if t.upper() not in self._delisted_cache and t.upper() not in _ohlcv_skip
        ]
        session = _req.Session()

        for sym in active_tickers:
            _time.sleep(0.35)  # throttle — 0.35s between tickers
            rows = self._fetch_chart_candles(sym, session)
            if rows is _SENTINEL_RATE_LIMITED:
                self._logger.warning('ohlcv_cache: Yahoo chart API rate-limited at %s — stopping', sym)
                break
            if not rows:
                continue
            db_rows = [(sym, '1d', ts, o, h, l, c, v, now_iso)
                       for ts, o, h, l, c, v in rows]
            try:
                conn.executemany(
                    """INSERT OR REPLACE INTO ohlcv_cache
                       (ticker, interval, ts, open, high, low, close, volume, cached_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    db_rows,
                )
                conn.commit()
                cached += 1
            except Exception as e:
                self._logger.debug('ohlcv_cache write failed for %s: %s', sym, e)

            # ── Intraday candles (15m, 1h) — same chart API, works on OCI ───
            for intraday_interval, intraday_range in (('15m', '60d'), ('1h', '60d')):
                try:
                    intra_rows = self._fetch_chart_candles(
                        sym, session,
                        interval=intraday_interval,
                        range_=intraday_range,
                    )
                    if intra_rows and intra_rows is not _SENTINEL_RATE_LIMITED:
                        intra_db_rows = [
                            (sym, intraday_interval, ts, o, h, l, c, v, now_iso)
                            for ts, o, h, l, c, v in intra_rows
                        ]
                        conn.executemany(
                            """INSERT OR REPLACE INTO ohlcv_cache
                               (ticker, interval, ts, open, high, low, close, volume, cached_at)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            intra_db_rows,
                        )
                        conn.commit()
                except Exception as _ie:
                    self._logger.debug('ohlcv_cache intraday write failed %s/%s: %s', sym, intraday_interval, _ie)

        conn.close()
        self._logger.info('ohlcv_cache: cached daily candles for %d/%d tickers', cached, len(active_tickers))

    # ── Fast bulk price path ───────────────────────────────────────────────

    # Workers for parallel fast_info price fetch.
    # Yahoo Finance rate-limits burst traffic from OCI IPs — keep concurrency low.
    _PRICE_WORKERS = 3
    # Per-ticker deadline for fast_info call
    _PRICE_TICKER_TIMEOUT = 12
    # Small delay between fast_info calls to avoid per-IP burst detection
    _PRICE_REQUEST_DELAY = 0.3

    def _bulk_download_prices(self, now_iso: str) -> List[RawAtom]:
        """
        Fetch last_price for all tickers using yf.Ticker.fast_info in parallel.

        Uses ThreadPoolExecutor instead of yf.download() to avoid Yahoo's
        bulk-download IP rate limit that affects OCI egress IPs.
        fast_info is a lightweight per-ticker REST call (~0.2s each) and
        does not trigger multi-ticker bulk throttling.

        Delisted tickers are skipped (from _delisted_cache) and newly
        discovered ones are persisted to the DB denylist.
        """
        atoms: List[RawAtom] = []
        _us_poly = _get_us_polygon_tickers(self._db_path)
        _dead = {t.upper() for t in _STATIC_DEAD_TICKERS}
        active_tickers = [
            t for t in self.tickers
            if t.upper() not in self._delisted_cache
            and t.upper() not in _us_poly
            and t.upper() not in _dead
        ]
        _polygon_skipped = sum(1 for t in self.tickers if t.upper() in _us_poly)
        if len(active_tickers) < len(self.tickers):
            self._logger.info(
                'Skipping %d delisted + %d polygon-covered tickers, %d active',
                len(self.tickers) - len(active_tickers) - _polygon_skipped,
                _polygon_skipped, len(active_tickers),
            )

        def _fetch_price(symbol: str):
            """Return (symbol, price_float) or (symbol, None) on failure."""
            import time as _time
            _time.sleep(self._PRICE_REQUEST_DELAY)  # throttle to avoid Yahoo burst block
            try:
                fi = yf.Ticker(symbol).fast_info
                price = (
                    fi.get('lastPrice')
                    or fi.get('regularMarketPrice')
                    or fi.get('navPrice')
                )
                if price and float(price) > 0:
                    return symbol, float(price)
            except Exception as e:
                err = str(e).lower()
                if 'delist' in err or 'no price data' in err or '404' in err:
                    return symbol, 'DELISTED'
                if '401' in err or 'crumb' in err or 'unauthorized' in err:
                    return symbol, 'AUTH_FAIL'
            return symbol, None

        ex = ThreadPoolExecutor(max_workers=self._PRICE_WORKERS, thread_name_prefix='yf-price')
        futures = {ex.submit(_fetch_price, sym): sym for sym in active_tickers}
        fetched = 0
        auth_fails = 0
        _AUTH_FAIL_ABORT = 5  # abort if Yahoo blocks this many tickers in a row
        try:
            for future in as_completed(futures, timeout=len(active_tickers) * 1.5 + 60):
                sym = futures[future]
                try:
                    _, price = future.result(timeout=self._PRICE_TICKER_TIMEOUT)
                    if price == 'DELISTED':
                        self._logger.info('Marking %s as delisted — will skip in future runs', sym)
                        self._mark_delisted(sym, self._db_path)
                    elif price == 'AUTH_FAIL':
                        auth_fails += 1
                        if auth_fails >= _AUTH_FAIL_ABORT:
                            self._logger.warning(
                                'Yahoo Finance blocking OCI IP (401 on %d tickers) — '
                                'aborting price fetch; will retry next cycle',
                                auth_fails,
                            )
                            break
                    elif price is not None:
                        auth_fails = 0  # reset on success
                        src = f'exchange_feed_yahoo_{sym.lower().replace("-", "_")}'
                        atoms.append(RawAtom(
                            subject=sym,
                            predicate='last_price',
                            object=str(round(price, 2)),
                            confidence=0.95,
                            source=src,
                            metadata={'as_of': now_iso, 'via': 'fast_info'},
                            upsert=True,
                        ))
                        fetched += 1
                except Exception as e:
                    self._logger.debug('price fetch failed for %s: %s', sym, e)
        except Exception:
            self._logger.warning('price fetch deadline reached — partial results returned')
        finally:
            ex.shutdown(wait=False, cancel_futures=True)

        self._logger.info(
            'bulk_download complete: %d/%d tickers fetched, %d price atoms',
            fetched, len(active_tickers), len(atoms),
        )
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
        ex = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix='yf-info')
        _us_poly = _get_us_polygon_tickers(self._db_path)
        futures = {
            ex.submit(self._fetch_info_atoms, sym, now_iso, bulk_prices): sym
            for sym in self.tickers
            if sym.upper() not in _us_poly
        }
        # Total budget for all info() calls: 120s regardless of ticker count
        _info_auth_fails = 0
        _INFO_AUTH_ABORT = 5
        try:
            for future in as_completed(futures, timeout=120.0):
                sym = futures[future]
                try:
                    result = future.result(timeout=15)
                    if result is _SENTINEL_AUTH_FAIL:
                        _info_auth_fails += 1
                        if _info_auth_fails >= _INFO_AUTH_ABORT:
                            self._logger.warning(
                                'Yahoo Finance blocking OCI IP on info() — '
                                'aborting info fetch; will retry next cycle'
                            )
                            break
                    else:
                        _info_auth_fails = 0
                        all_atoms.extend(result)
                except Exception as e:
                    self._logger.warning('info fetch failed for %s: %s', sym, e)
        except Exception:
            self._logger.warning('info fetch deadline reached — abandoning remaining tickers')
        # Abandon remaining futures — don't block on hung network calls
        ex.shutdown(wait=False, cancel_futures=True)

        return all_atoms

    def _fetch_info_atoms(
        self, symbol: str, now_iso: str, bulk_prices: Dict[str, float]
    ) -> List[RawAtom]:
        """
        Fetch .info() for one ticker and return non-price atoms.
        Retries once on rate-limit / timeout / crumb expiry.
        On 401 (Invalid Crumb), clears the yfinance session cache so the
        next attempt re-fetches a fresh crumb from Yahoo.
        """
        if symbol.upper() in _NO_FUNDAMENTALS_TICKERS:
            return []
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
                is_crumb = '401' in err or 'crumb' in err or 'unauthorized' in err
                is_ratelimit = any(k in err for k in ('429', 'rate', 'too many', 'timeout'))
                if is_crumb:
                    # Signal IP-level block to caller — no point retrying
                    return _SENTINEL_AUTH_FAIL  # type: ignore[return-value]
                if attempt == 0 and is_ratelimit:
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
                # Blend 52w position when target-based signal is neutral:
                # if price is near 52w low → 'short'; near 52w high → 'long'
                # This corrects the structural bias where analyst targets are
                # almost always above price, leaving bearish tickers as 'neutral'.
                if direction == 'neutral':
                    _h52 = info.get('fiftyTwoWeekHigh')
                    _l52 = info.get('fiftyTwoWeekLow')
                    if _h52 and _l52:
                        try:
                            _h, _l, _p = float(_h52), float(_l52), float(current_price)
                            if _h > _l:
                                _ratio = (_p - _l) / (_h - _l)
                                if _ratio <= 0.20:
                                    direction = 'short'
                                elif _ratio >= 0.80:
                                    direction = 'long'
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass
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
            # Normalise: 'Real Estate' → 'real_estate', 'Communication Services' → 'communication_services'
            sector_norm = sector.lower().strip().replace(' ', '_').replace('-', '_')
            atoms.append(RawAtom(
                subject=symbol, predicate='sector', object=sector_norm,
                confidence=0.95, source=src,
                metadata={'industry': info.get('industry'), 'sector_raw': sector},
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

        # ── Volatility regime + raw beta ──────────────────────────────────
        beta = info.get('beta')
        if beta is not None:
            regime = _volatility_regime(float(beta))
            if regime != 'unknown':
                atoms.append(RawAtom(
                    subject=symbol, predicate='volatility_regime', object=regime,
                    confidence=0.80, source=src, metadata={'beta': beta},
                    upsert=True,
                ))
            # Store raw beta — enables systematic-risk-aware quality gates
            atoms.append(RawAtom(
                subject=symbol, predicate='beta',
                object=str(round(float(beta), 3)),
                confidence=0.85, source=src, upsert=True,
            ))

        # ── Price regime from 52-week position (equities) ─────────────────
        # ETFs get price_regime in _etf_atoms(); equities need it here.
        high_52 = info.get('fiftyTwoWeekHigh')
        low_52  = info.get('fiftyTwoWeekLow')
        if high_52 and low_52 and current_price:
            try:
                h, l, p = float(high_52), float(low_52), float(current_price)
                if h > l:
                    ratio = (p - l) / (h - l)
                    pr = ('near_52w_high' if ratio >= 0.85
                          else 'near_52w_low' if ratio <= 0.15
                          else 'mid_range')
                    atoms.append(RawAtom(
                        subject=symbol, predicate='price_regime', object=pr,
                        confidence=0.85, source=src,
                        metadata={'ratio': round(ratio, 3), 'as_of': now_iso},
                        upsert=True,
                    ))
            except (TypeError, ValueError, ZeroDivisionError):
                pass

        # ── Next earnings date (upsert — date changes each quarter) ───────
        try:
            cal = yf.Ticker(symbol).calendar
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

            # ── Macro regime proxy: derive signal_direction from 52w position ──
            # ETFs have no analyst targets, so regime classifier gets signal_direction
            # from where price sits in its 52w range. near_high → bullish, etc.
            # This replaces the broken conviction_tier fallback in the regime classifier.
            _MACRO_PROXIES = {'SPY', 'HYG', 'TLT', 'GLD', 'UUP'}
            if symbol.upper() in _MACRO_PROXIES:
                try:
                    _h, _l, _p = float(high_52), float(low_52), float(price)
                    if _h > _l:
                        _ratio = (_p - _l) / (_h - _l)
                        _etf_dir = 'bullish' if _ratio >= 0.70 else ('bearish' if _ratio <= 0.30 else 'neutral')
                        atoms.append(RawAtom(
                            subject=symbol,
                            predicate='signal_direction',
                            object=_etf_dir,
                            confidence=0.70,
                            source=f'derived_52w_position_{symbol.lower()}',
                            metadata={
                                'derived_from': '52w_range_position',
                                'pct_of_range': round(_ratio * 100, 1),
                                'as_of': now_iso,
                            },
                            upsert=True,
                        ))
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

        return atoms
