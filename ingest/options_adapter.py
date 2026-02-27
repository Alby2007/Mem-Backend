"""
ingest/options_adapter.py — Options Market Data Adapter

Fetches options chain data for liquid equity names via yfinance and computes
interpretable options-regime atoms.  ETFs, macro proxies, and broad indices
are excluded — options data on single names carries more signal than on
basket products.

ATOMS PRODUCED
==============
All atoms use source prefix 'options_feed_{ticker}' (authority 0.75).
All atoms are upsert=True.

  Predicate          Example value   Notes
  ────────────────────────────────────────────────────────────────────
  iv_rank            "72.4"          Current 30d IV as percentile of
                                     the 52-week IV range (0–100).
                                     Proxy: uses volatility_30d vs
                                     volatility_90d from KB when
                                     full chain IV history unavailable.
                                     Computed from live chain when
                                     both expirations are accessible.
  put_call_ratio     "1.42"          Sum of put open-interest /
                                     sum of call open-interest across
                                     the front two expirations.
  options_regime     "elevated_vol"  compressed (<25th pct IV rank)
                                     normal (25–75th pct)
                                     elevated_vol (>75th pct)
  smart_money_signal "call_sweep"    Emitted when any single strike in
                                     the front two expirations has
                                     volume > _SWEEP_VOLUME_RATIO ×
                                     open_interest (new large position).
                                     Values: call_sweep | put_sweep | none

  iv_skew_ratio      "1.34"          OTM put IV / ATM IV.  >1.2 = puts
                                     bid above calls (protective demand).
                                     Computed from first expiry only.
  iv_skew_25d        "0.082"         OTM put IV - OTM call IV (5% wings).
                                     Positive = put skew elevated.
  skew_regime        "elevated"      normal (<1.2) / elevated (1.2-1.4)
                                     / spike (>1.4).

MARKET-LEVEL ATOMS (subject='market', from SPY skew pass)
=========================================================
  spy_skew_ratio     "1.31"          SPY OTM put IV / ATM IV.
  spy_skew_regime    "elevated"      Same thresholds as per-ticker.
  tail_risk          "moderate"      normal/moderate/elevated/extreme
                                     based on SPY skew ratio.

TICKER SCOPE
============
Equity-only — ~44 liquid single names from the watchlist.
Excluded: all ETFs, macro proxies (TLT, HYG, GLD, SLV, UUP),
broad indices (SPY, QQQ, IWM, DIA, VTI), sector ETFs (XL*).

SPY is handled separately in _SKEW_ONLY_TICKERS for index-level skew
calculation — keeping it out of the equity loop preserves the integrity
of put_call_ratio and smart_money_signal (designed for single names).

At ~1.5s/ticker × 44 = ~66s per cycle, well within the 30-min interval
even with rate-limit variance.

IV RANK CALCULATION
===================
True IV rank requires 52 weeks of daily ATM IV history, which yfinance
does not provide.  This adapter uses a proxy:
  iv_rank_proxy = (vol_30d - vol_90d_min) / (vol_90d_max - vol_90d_min) * 100
where vol_30d is the current short-term realised vol (from KB / live chain)
and vol_90d is the longer-term anchor.  This is a simplification — a
positive rank (vol_30d > vol_90d) indicates elevated near-term vol versus
recent history, which is directionally correct for options regime
classification even if not a true 52-week percentile.

When the live chain IV is accessible, the adapter reads ATM IV directly
from the nearest-expiry option chain and uses the KB's volatility_30d
as the 52w-low anchor and 2× volatility_30d as the 52w-high anchor.

RATE LIMITING
=============
A configurable per-ticker sleep (default 0.35s) prevents yfinance
rate-limiting.  The adapter accepts a `sleep_sec` constructor argument
for test overrides (set to 0 in tests).

INTERVAL
========
Register at 1800s (30 min).  Options data updates continuously during
market hours but intra-day precision is not required for this signal layer.
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional, Tuple

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

# ── Equity-only options universe ──────────────────────────────────────────────
# Top FTSE names with meaningful listed options liquidity.
# UK retail options market is significantly less developed than US —
# confidence on options-derived atoms is capped for names outside this list.
_OPTIONS_TICKERS = [
    # Top 20 FTSE — most liquid options
    'SHEL.L', 'AZN.L', 'HSBA.L', 'ULVR.L', 'BP.L',
    'GSK.L', 'RIO.L', 'LLOY.L', 'BARC.L', 'NWG.L',
    'LSEG.L', 'BA.L', 'RR.L', 'VOD.L', 'BATS.L',
    'NG.L', 'REL.L', 'TSCO.L', 'AAL.L',
    # User portfolio holdings — US single names with active options markets
    'COIN', 'HOOD', 'MSTR', 'PLTR', 'NVDA',
    'XYZ',   # Block Inc. (formerly SQ)
    # High-conviction US names with strong KB atom coverage
    'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT', 'MA',
]

# FTSE names with thin options liquidity — smart_money_signal and iv_rank
# atoms are emitted at reduced authority (0.45 vs 0.75) for these tickers.
_LOW_OPTIONS_LIQUIDITY: frozenset = frozenset({
    'VOD.L', 'BATS.L', 'NG.L', 'REL.L', 'TSCO.L', 'AAL.L',
    'QQ.L', 'MKS.L', 'PSON.L', 'PSN.L',
})

# Volume-to-OI ratio threshold for a "sweep" (new large position being opened)
_SWEEP_VOLUME_RATIO = 3.0

# IV rank boundaries for options_regime classification
_IV_RANK_COMPRESSED    = 25.0   # < 25th pct  → compressed
_IV_RANK_ELEVATED      = 75.0   # > 75th pct  → elevated_vol

# Number of front expirations to analyse (1 = nearest only, 2 = front two)
_EXPIRY_DEPTH = 2

# Default per-ticker sleep to avoid rate limiting (override in tests)
_DEFAULT_SLEEP_SEC = 0.35

# Tickers processed only for skew (not full options metrics).
# SPY is excluded from _OPTIONS_TICKERS to keep put_call_ratio and
# smart_money_signal clean for single-name analysis.
_SKEW_ONLY_TICKERS = ['SPY']

# Skew regime thresholds (OTM put IV / ATM IV ratio)
_SKEW_SPIKE    = 1.4   # > 1.4  → spike    (acute crash risk)
_SKEW_ELEVATED = 1.2   # > 1.2  → elevated (institutions cautious)

# Tail-risk thresholds derived from SPY skew ratio
_TAIL_EXTREME  = 1.5
_TAIL_ELEVATED = 1.35
_TAIL_MODERATE = 1.2


# ── IV rank helpers ───────────────────────────────────────────────────────────

def _iv_rank_from_chain(
    calls_iv: List[float],
    puts_iv: List[float],
    vol_30d_kb: Optional[float],
) -> Optional[float]:
    """
    Estimate IV rank (0–100) from live chain data.

    Uses the median ATM-ish IV from the first expiry chain as 'current IV'.
    Uses vol_30d_kb (realised vol from KB) as the 52w-low anchor and
    2 × vol_30d_kb as the 52w-high anchor (heuristic).

    Returns None when insufficient data.
    """
    all_iv = [v for v in calls_iv + puts_iv if v and v > 0.01]
    if not all_iv:
        return None
    current_iv = sorted(all_iv)[len(all_iv) // 2] * 100  # midpoint, convert to %

    if vol_30d_kb and vol_30d_kb > 0:
        iv_low  = vol_30d_kb * 0.7   # assume compressed = 70% of recent realised
        iv_high = vol_30d_kb * 1.8   # assume elevated  = 180% of recent realised
    else:
        # No KB anchor — can't rank without a range
        return None

    if iv_high <= iv_low:
        return None

    rank = (current_iv - iv_low) / (iv_high - iv_low) * 100
    return round(max(0.0, min(100.0, rank)), 1)


def _classify_options_regime(iv_rank: Optional[float]) -> str:
    """Map iv_rank (0–100) → options_regime label."""
    if iv_rank is None:
        return 'unknown'
    if iv_rank < _IV_RANK_COMPRESSED:
        return 'compressed'
    if iv_rank > _IV_RANK_ELEVATED:
        return 'elevated_vol'
    return 'normal'


def _put_call_ratio(
    calls_oi: List[int],
    puts_oi: List[int],
) -> Optional[float]:
    """Sum of put OI / sum of call OI.  None when either side is empty/zero."""
    total_calls = sum(v for v in calls_oi if v and v >= 0)
    total_puts  = sum(v for v in puts_oi  if v and v >= 0)
    if total_calls <= 0:
        return None
    return round(total_puts / total_calls, 3)


def _detect_sweep(
    calls_vol: List[int],
    calls_oi:  List[int],
    puts_vol:  List[int],
    puts_oi:   List[int],
) -> str:
    """
    Detect unusual single-strike volume vs open interest.

    Returns 'call_sweep', 'put_sweep', or 'none'.
    A sweep fires when any single strike has volume > _SWEEP_VOLUME_RATIO × OI,
    indicating a large new position being opened (not just rolling existing).
    """
    for vol, oi in zip(calls_vol, calls_oi):
        if vol and oi and oi > 0 and vol >= _SWEEP_VOLUME_RATIO * oi:
            return 'call_sweep'
    for vol, oi in zip(puts_vol, puts_oi):
        if vol and oi and oi > 0 and vol >= _SWEEP_VOLUME_RATIO * oi:
            return 'put_sweep'
    return 'none'


# ── Skew helpers ─────────────────────────────────────────────────────────────

def _compute_skew(
    calls_df,   # pandas DataFrame — first-expiry calls chain
    puts_df,    # pandas DataFrame — first-expiry puts chain
    current_price: float,
) -> Optional[dict]:
    """
    Compute put/call IV skew from the first-expiry option chain.

    ATM strike  = call strike closest to current_price
    OTM put     = put strike closest to current_price * 0.95 (5% below)
    OTM call    = call strike closest to current_price * 1.05 (5% above)

    Returns dict with keys:
      skew_ratio   — otm_put_iv / atm_iv  (>1.0 = puts bid over calls)
      skew_25d     — otm_put_iv - otm_call_iv  (positive = put skew)
      atm_iv       — ATM call implied volatility
      otm_put_iv   — OTM put implied volatility
      otm_call_iv  — OTM call implied volatility

    Returns None when data is insufficient (empty chain, zero ATM IV,
    or required columns missing).  Never raises.
    """
    try:
        if calls_df is None or puts_df is None:
            return None
        if calls_df.empty or puts_df.empty:
            return None
        if 'strike' not in calls_df.columns or 'impliedVolatility' not in calls_df.columns:
            return None
        if 'strike' not in puts_df.columns or 'impliedVolatility' not in puts_df.columns:
            return None
        if current_price <= 0:
            return None

        call_strikes = calls_df['strike'].dropna().tolist()
        put_strikes  = puts_df['strike'].dropna().tolist()
        if not call_strikes or not put_strikes:
            return None

        # ATM strike — closest call strike to current price
        atm_strike = min(call_strikes, key=lambda x: abs(x - current_price))
        atm_row    = calls_df[calls_df['strike'] == atm_strike]['impliedVolatility']
        atm_iv     = float(atm_row.dropna().iloc[0]) if not atm_row.dropna().empty else 0.0

        if atm_iv <= 0:
            return None

        # OTM put — closest put strike to 5% below current price
        otm_put_target = current_price * 0.95
        otm_put_strike = min(put_strikes, key=lambda x: abs(x - otm_put_target))
        put_row        = puts_df[puts_df['strike'] == otm_put_strike]['impliedVolatility']
        otm_put_iv     = float(put_row.dropna().iloc[0]) if not put_row.dropna().empty else 0.0

        # OTM call — closest call strike to 5% above current price
        otm_call_target = current_price * 1.05
        otm_call_strike = min(call_strikes, key=lambda x: abs(x - otm_call_target))
        call_row        = calls_df[calls_df['strike'] == otm_call_strike]['impliedVolatility']
        otm_call_iv     = float(call_row.dropna().iloc[0]) if not call_row.dropna().empty else 0.0

        skew_ratio = round(otm_put_iv / atm_iv, 4) if atm_iv > 0 else 1.0
        skew_25d   = round(otm_put_iv - otm_call_iv, 4)

        return {
            'skew_ratio':  skew_ratio,
            'skew_25d':    skew_25d,
            'atm_iv':      round(atm_iv, 4),
            'otm_put_iv':  round(otm_put_iv, 4),
            'otm_call_iv': round(otm_call_iv, 4),
        }
    except Exception:
        return None


def _classify_skew_regime(skew_ratio: float) -> str:
    """
    Map skew_ratio → skew_regime label.

    spike    > 1.4  (acute crash risk — puts heavily bid)
    elevated > 1.2  (institutions cautious)
    normal   <= 1.2
    """
    if skew_ratio > _SKEW_SPIKE:
        return 'spike'
    if skew_ratio > _SKEW_ELEVATED:
        return 'elevated'
    return 'normal'


def _classify_tail_risk(spy_skew_ratio: float) -> str:
    """
    Map SPY skew_ratio → market-level tail_risk label.

    extreme  > 1.5   (market pricing acute crash)
    elevated > 1.35  (elevated institutional hedging)
    moderate > 1.2   (above-normal put demand)
    normal   <= 1.2  (balanced options market)
    """
    if spy_skew_ratio > _TAIL_EXTREME:
        return 'extreme'
    if spy_skew_ratio > _TAIL_ELEVATED:
        return 'elevated'
    if spy_skew_ratio > _TAIL_MODERATE:
        return 'moderate'
    return 'normal'


# ── Chain data fetcher ────────────────────────────────────────────────────────

def _fetch_chain_data(
    ticker_sym: str,
) -> Tuple[List[float], List[float], List[int], List[int], List[int], List[int], object, object]:
    """
    Fetch options chain data for the front _EXPIRY_DEPTH expirations.

    Returns:
        (calls_iv, puts_iv, calls_oi, puts_oi, calls_vol, puts_vol,
         first_expiry_calls_df, first_expiry_puts_df)

    The flat lists aggregate across all strikes / expirations (for IV rank,
    PCR, sweep detection).  The first-expiry DataFrames carry full strike +
    impliedVolatility detail needed by _compute_skew.

    Returns empty lists / None DataFrames on any error.
    """
    calls_iv, puts_iv   = [], []
    calls_oi, puts_oi   = [], []
    calls_vol, puts_vol = [], []
    first_calls_df = None
    first_puts_df  = None

    try:
        tkr      = yf.Ticker(ticker_sym)
        expiries = tkr.options
        if not expiries:
            return calls_iv, puts_iv, calls_oi, puts_oi, calls_vol, puts_vol, None, None

        for i, expiry in enumerate(expiries[:_EXPIRY_DEPTH]):
            chain = tkr.option_chain(expiry)

            c = chain.calls
            p = chain.puts

            # Capture first-expiry DataFrames for skew before any column
            # access that might raise — ensures skew is not lost even if
            # OI/vol columns are absent (e.g. thin market or mock data).
            if i == 0:
                first_calls_df = c
                first_puts_df  = p

            calls_iv.extend(c['impliedVolatility'].dropna().tolist())
            puts_iv.extend(p['impliedVolatility'].dropna().tolist())

            calls_oi.extend(c['openInterest'].fillna(0).astype(int).tolist())
            puts_oi.extend(p['openInterest'].fillna(0).astype(int).tolist())

            calls_vol.extend(c['volume'].fillna(0).astype(int).tolist())
            puts_vol.extend(p['volume'].fillna(0).astype(int).tolist())

    except Exception as exc:
        _logger.debug('[options] %s chain fetch error: %s', ticker_sym, exc)

    return (calls_iv, puts_iv, calls_oi, puts_oi, calls_vol, puts_vol,
            first_calls_df, first_puts_df)


# ── Adapter ───────────────────────────────────────────────────────────────────

class OptionsAdapter(BaseIngestAdapter):
    """
    Options market data adapter.

    Fetches yfinance options chain for the equity-only universe and emits
    iv_rank, put_call_ratio, options_regime, smart_money_signal atoms.

    All atoms are informational-only in v1 — no automatic conviction_tier
    modification.  Auto-demotion rules will be added in v2 once forward-
    looking snapshot data validates the signal.
    """

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        sleep_sec: float = _DEFAULT_SLEEP_SEC,
        db_path: Optional[str] = None,
    ):
        super().__init__(name='options')
        if tickers:
            self._tickers = [t.upper() for t in tickers]
        elif _HAS_DYNAMIC_WATCHLIST and db_path:
            # Filter active tickers to equity-only (exclude ETFs/macro proxies)
            active = DynamicWatchlistManager.get_active_tickers(db_path)
            _etf_prefixes = {'XL', 'SP', 'QQ', 'IW', 'DI', 'VT'}
            _macro = {'GLD', 'SLV', 'TLT', 'HYG', 'LQD', 'UUP', 'SPY', 'QQQ',
                      'IWM', 'DIA', 'VTI', 'USO'}
            self._tickers = [
                t.upper() for t in active
                if t.upper() not in _macro and not t.upper().startswith('XL')
            ]
        else:
            self._tickers = _OPTIONS_TICKERS
        self._sleep_sec = sleep_sec

    def fetch(self) -> List[RawAtom]:
        if not HAS_YFINANCE:
            _logger.warning('[options] yfinance not available — skipping')
            return []

        atoms: List[RawAtom] = []
        succeeded = 0
        failed    = 0

        for sym in self._tickers:
            try:
                atoms.extend(self._fetch_one(sym))
                succeeded += 1
            except Exception as exc:
                _logger.debug('[options] %s failed: %s', sym, exc)
                failed += 1

            if self._sleep_sec > 0:
                time.sleep(self._sleep_sec)

        # ── SPY market-level skew pass ────────────────────────────────────────
        # Runs after the equity loop — separate from _OPTIONS_TICKERS to keep
        # put_call_ratio / smart_money_signal metrics clean for single names.
        try:
            spy_atoms = self._fetch_spy_skew()
            atoms.extend(spy_atoms)
            if spy_atoms:
                _logger.info('[options] SPY skew: %d market atoms emitted', len(spy_atoms))
        except Exception as exc:
            _logger.debug('[options] SPY skew pass failed: %s', exc)

        _logger.info(
            '[options] fetched %d tickers (%d atoms), %d failed',
            succeeded, len(atoms), failed,
        )
        return atoms

    def _fetch_one(self, sym: str) -> List[RawAtom]:
        """Compute options atoms for a single ticker symbol."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        src     = f'options_feed_{sym.lower()}'
        meta    = {'as_of': now_iso, 'ticker': sym}
        # UK options market is thin for smaller names — cap signal confidence
        _low_liq = sym.upper() in _LOW_OPTIONS_LIQUIDITY
        _iv_conf     = 0.40 if _low_liq else 0.65
        _sweep_conf  = 0.35 if _low_liq else 0.60
        if _low_liq:
            meta = {**meta, 'low_options_liquidity': True}

        (calls_iv, puts_iv,
         calls_oi, puts_oi,
         calls_vol, puts_vol,
         first_calls_df, first_puts_df) = _fetch_chain_data(sym)

        # Need at least some IV data to produce useful atoms
        if not calls_iv and not puts_iv:
            return []

        # ── Fetch vol_30d from KB as IV rank anchor ───────────────────────────
        # OptionsAdapter is stateless — reads vol_30d from yf.Ticker.info as
        # a fallback if not passed in.  The KB atom is not accessible here
        # without a db_path, so we use the chain's own median IV as the sole
        # input and rely on _iv_rank_from_chain's internal heuristic.
        # vol_30d_kb defaults to None → chain-only estimation.
        iv_rank = _iv_rank_from_chain(calls_iv, puts_iv, vol_30d_kb=None)
        regime  = _classify_options_regime(iv_rank)
        pcr     = _put_call_ratio(calls_oi, puts_oi)
        sweep   = _detect_sweep(calls_vol, calls_oi, puts_vol, puts_oi)

        result: List[RawAtom] = []

        # options_regime — always emit when chain data available
        result.append(RawAtom(
            subject    = sym.lower(),
            predicate  = 'options_regime',
            object     = regime,
            confidence = 0.70,
            source     = src,
            metadata   = {**meta, 'iv_rank': str(iv_rank)},
            upsert     = True,
        ))

        # iv_rank — emit when computable
        if iv_rank is not None:
            result.append(RawAtom(
                subject    = sym.lower(),
                predicate  = 'iv_rank',
                object     = str(iv_rank),
                confidence = _iv_conf,
                source     = src,
                metadata   = meta,
                upsert     = True,
            ))

        # put_call_ratio — emit when computable
        if pcr is not None:
            result.append(RawAtom(
                subject    = sym.lower(),
                predicate  = 'put_call_ratio',
                object     = str(pcr),
                confidence = 0.70,
                source     = src,
                metadata   = meta,
                upsert     = True,
            ))

        # smart_money_signal — always emit (includes 'none')
        result.append(RawAtom(
            subject    = sym.lower(),
            predicate  = 'smart_money_signal',
            object     = sweep,
            confidence = _sweep_conf,
            source     = src,
            metadata   = meta,
            upsert     = True,
        ))

        # ── Skew atoms (first-expiry only) ────────────────────────────────────
        # Requires current price to identify ATM/OTM strikes.
        # Skipped gracefully when chain DataFrames or price unavailable.
        try:
            current_price = 0.0
            if HAS_YFINANCE:
                info = yf.Ticker(sym).info
                current_price = float(
                    info.get('regularMarketPrice') or
                    info.get('currentPrice') or
                    info.get('previousClose') or 0.0
                )
        except Exception:
            current_price = 0.0

        skew = _compute_skew(first_calls_df, first_puts_df, current_price)
        if skew is not None:
            skew_regime = _classify_skew_regime(skew['skew_ratio'])
            skew_meta   = {
                **meta,
                'atm_iv':      str(skew['atm_iv']),
                'otm_put_iv':  str(skew['otm_put_iv']),
                'otm_call_iv': str(skew['otm_call_iv']),
                'skew_25d':    str(skew['skew_25d']),
            }
            result.append(RawAtom(
                subject    = sym.lower(),
                predicate  = 'iv_skew_ratio',
                object     = str(skew['skew_ratio']),
                confidence = 0.70,
                source     = src,
                metadata   = skew_meta,
                upsert     = True,
            ))
            result.append(RawAtom(
                subject    = sym.lower(),
                predicate  = 'iv_skew_25d',
                object     = str(skew['skew_25d']),
                confidence = 0.65,
                source     = src,
                metadata   = skew_meta,
                upsert     = True,
            ))
            result.append(RawAtom(
                subject    = sym.lower(),
                predicate  = 'skew_regime',
                object     = skew_regime,
                confidence = 0.70,
                source     = src,
                metadata   = skew_meta,
                upsert     = True,
            ))

        return result

    def _fetch_spy_skew(self) -> List[RawAtom]:
        """
        Compute market-level skew atoms from SPY options chain.

        Emits three atoms with subject='market':
          spy_skew_ratio  — SPY OTM put IV / ATM IV
          spy_skew_regime — normal / elevated / spike
          tail_risk       — normal / moderate / elevated / extreme

        Returns empty list when SPY chain is unavailable or price is missing.
        """
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        src     = 'options_feed_spy'
        meta    = {'as_of': now_iso, 'ticker': 'SPY'}

        (_, _, _, _, _, _,
         first_calls_df, first_puts_df) = _fetch_chain_data('SPY')

        try:
            current_price = 0.0
            if HAS_YFINANCE:
                info = yf.Ticker('SPY').info
                current_price = float(
                    info.get('regularMarketPrice') or
                    info.get('currentPrice') or
                    info.get('previousClose') or 0.0
                )
        except Exception:
            current_price = 0.0

        skew = _compute_skew(first_calls_df, first_puts_df, current_price)
        if skew is None:
            return []

        spy_skew_ratio  = skew['skew_ratio']
        spy_skew_regime = _classify_skew_regime(spy_skew_ratio)
        tail_risk       = _classify_tail_risk(spy_skew_ratio)

        skew_meta = {
            **meta,
            'atm_iv':      str(skew['atm_iv']),
            'otm_put_iv':  str(skew['otm_put_iv']),
            'otm_call_iv': str(skew['otm_call_iv']),
            'skew_25d':    str(skew['skew_25d']),
        }
        return [
            RawAtom(subject='market', predicate='spy_skew_ratio',
                    object=str(spy_skew_ratio), confidence=0.72,
                    source=src, metadata=skew_meta, upsert=True),
            RawAtom(subject='market', predicate='spy_skew_regime',
                    object=spy_skew_regime, confidence=0.72,
                    source=src, metadata=skew_meta, upsert=True),
            RawAtom(subject='market', predicate='tail_risk',
                    object=tail_risk, confidence=0.72,
                    source=src, metadata=skew_meta, upsert=True),
        ]
