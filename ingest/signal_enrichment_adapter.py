"""
ingest/signal_enrichment_adapter.py — Second-Order Signal Enrichment

Reads existing KB atoms and computes regime-conditional, interpretable
signal atoms. No external API calls. Runs after yfinance_adapter each cycle.

ATOMS PRODUCED
==============

  price_regime   — where the current price sits within its 52-week range
    Values: near_52w_high | mid_range | near_52w_low
    Source: derived_signal_price_regime_{ticker}
    Logic: uses 52w_high / 52w_low from yfinance metadata if present;
           falls back to estimating from last_price vs price_target ratio.

  upside_pct     — percentage upside from last_price to consensus price_target
    Values: e.g. "34.2" (as string, LLM-readable)
    Source: derived_signal_upside_{ticker}
    Note: negative values = downside. Stored as plain percent string.

  signal_quality — coherence assessment across signal_direction,
                   volatility_regime, price_regime, and upside_pct
    Values: strong | confirmed | extended | conflicted | weak
    Source: derived_signal_quality_{ticker}

  macro_confirmation — whether equity signal aligns with cross-asset macro
    Values: confirmed | partial | unconfirmed | no_data
    Source: derived_signal_macro_confirm_{ticker}

SIGNAL QUALITY DECISION RULES
==============================
Rules are explicit and documented here so the classification logic is
auditable when revisited. Each rule applies in priority order (first match wins).

  STRONG: all four conditions supportive
    - signal_direction IN (long, near_high)   AND
    - upside_pct >= 15                         AND
    - price_regime != near_52w_high            AND   ← not already extended
    - volatility_regime IN (low_volatility, medium_volatility)

  CONFIRMED: signal and upside aligned, price not extended
    - signal_direction IN (long, near_high)   AND
    - upside_pct >= 8                          AND
    - price_regime != near_52w_high

  EXTENDED: bullish signal but price is already near the top
    - signal_direction IN (long, near_high)   AND
    - price_regime == near_52w_high

  CONFLICTED: opposing signals between direction and other dimensions
    - signal_direction IN (long, near_high)   AND upside_pct < 0  ← price > target
    OR
    - signal_direction IN (short, near_low)   AND upside_pct >= 15 ← target much higher
    OR
    - signal_direction IN (long, near_high)   AND
      volatility_regime == high_volatility    AND
      price_regime == near_52w_high           ← extended + high vol = conflicted

  WEAK: neutral signal or insufficient data
    - signal_direction == neutral             OR
    - upside_pct data missing

MACRO CONFIRMATION RULES
=========================
Checks alignment between equity signal and three cross-asset macro proxies
available in the KB: HYG (credit), TLT (rates), SPY (broad market).

  CONFIRMED: all three proxies align with equity signal
    - equity signal is long/bullish
    - HYG signal_direction != near_low  (credit NOT selling off)
    - TLT signal_direction != near_high (rates NOT spiking / bond rally = risk-off)
    - SPY signal_direction IN (near_high, long)

  PARTIAL: majority (2/3) proxies align
  UNCONFIRMED: majority proxies contradict equity signal
  NO_DATA: insufficient macro proxy atoms in KB

SOURCE PREFIX
=============
All atoms use 'derived_signal_' prefix → authority 0.65 in authority.py.
Correctly scores below exchange_feed (1.0) and broker_research (0.80)
since these are computed from observed data, not directly observed.

INTERVAL
========
Register at same interval as yfinance_adapter (300s) so enrichment
always reflects the latest price/signal cycle.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# Macro proxy tickers used for cross-asset confirmation
_CREDIT_PROXY  = 'hyg'   # credit spreads — near_high = risk-on
_RATES_PROXY   = 'tlt'   # long-duration treasuries — near_high = risk-off (rates falling)
_MARKET_PROXY  = 'spy'   # broad equity market

# signal_direction values that indicate bullish positioning
_BULLISH_SIGNALS = {'long', 'near_high', 'near_52w_high'}
# signal_direction values that indicate bearish positioning
_BEARISH_SIGNALS = {'short', 'near_low',  'near_52w_low'}

# upside_pct thresholds (in percent)
_UPSIDE_STRONG    = 15.0   # >= 15%: meaningful analyst conviction
_UPSIDE_CONFIRMED =  8.0   # >=  8%: moderate upside
_UPSIDE_CONFLICT  =  0.0   # <  0%: price already above target (downside)

# volatility regimes that permit 'strong' classification
_LOW_VOL_REGIMES  = {'low_volatility', 'medium_volatility'}
_HIGH_VOL_REGIME  = 'high_volatility'


# ── KB snapshot reader ────────────────────────────────────────────────────────

def _read_kb_atoms(
    kg_db_path: str,
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Read current KB atoms directly from SQLite.

    Returns:
        ticker_atoms: { ticker_lower: { predicate: object_value } }
                      Only the most recent (highest-confidence) row per
                      (subject, predicate) pair is kept.
        macro_signals: { proxy_ticker: signal_direction_value }
    """
    ticker_atoms: Dict[str, Dict[str, str]] = {}
    conn = sqlite3.connect(kg_db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    c = conn.cursor()

    try:
        # Fetch all relevant predicates ordered so highest confidence wins
        c.execute("""
            SELECT subject, predicate, object, confidence
            FROM facts
            WHERE predicate IN (
                'last_price', 'price_target', 'signal_direction',
                'volatility_regime', 'market_cap_tier', 'sector',
                'earnings_quality'
            )
            ORDER BY subject, predicate, confidence DESC
        """)
        for row in c.fetchall():
            subj = row['subject'].lower().strip()
            pred = row['predicate'].strip()
            obj  = row['object'].strip()
            if subj not in ticker_atoms:
                ticker_atoms[subj] = {}
            # First row per (subj, pred) is highest confidence — keep it
            if pred not in ticker_atoms[subj]:
                ticker_atoms[subj][pred] = obj
    finally:
        conn.close()

    # Extract macro proxy signals separately for clarity
    macro_signals = {
        proxy: ticker_atoms.get(proxy, {}).get('signal_direction', '')
        for proxy in (_CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY)
    }

    return ticker_atoms, macro_signals


# ── Classification logic ───────────────────────────────────────────────────────

def _classify_price_regime(
    last_price: Optional[float],
    price_target: Optional[float],
    signal_direction: str,
) -> str:
    """
    Classify where current price sits within its expected range.

    Uses price_target as a proxy for fair value when 52w high/low is
    unavailable (yfinance metadata not stored as KB atoms).

    Decision rules:
      near_52w_high  → price >= 95% of price_target  (at or above fair value)
      near_52w_low   → price <= 75% of price_target  (well below fair value)
      mid_range      → everything between

    Falls back to signal_direction mapping when price/target missing:
      long/near_high → mid_range (assume not yet extended)
      neutral        → mid_range
      short/near_low → near_52w_low
    """
    if last_price is not None and price_target is not None and price_target > 0:
        ratio = last_price / price_target
        if ratio >= 0.95:
            return 'near_52w_high'
        elif ratio <= 0.75:
            return 'near_52w_low'
        else:
            return 'mid_range'

    # Fallback: infer from signal_direction
    if signal_direction in _BEARISH_SIGNALS:
        return 'near_52w_low'
    return 'mid_range'


def _classify_signal_quality(
    signal_direction: str,
    volatility_regime: str,
    price_regime: str,
    upside_pct: Optional[float],
) -> str:
    """
    Classify the coherence of a ticker's signal composite.

    Rules are applied in priority order — first match wins.
    See module docstring for full decision rule documentation.
    """
    is_bullish = signal_direction in _BULLISH_SIGNALS
    is_bearish = signal_direction in _BEARISH_SIGNALS
    is_neutral = not is_bullish and not is_bearish

    # ── CONFLICTED: internal contradictions take highest priority ────────────
    # Rule C1: bullish signal but price already above consensus target
    if is_bullish and upside_pct is not None and upside_pct < _UPSIDE_CONFLICT:
        return 'conflicted'
    # Rule C2: bearish signal but large upside gap remaining (target >> price)
    if is_bearish and upside_pct is not None and upside_pct >= _UPSIDE_STRONG:
        return 'conflicted'
    # Rule C3: bullish signal + high volatility + price already extended
    if (is_bullish
            and volatility_regime == _HIGH_VOL_REGIME
            and price_regime == 'near_52w_high'):
        return 'conflicted'

    # ── EXTENDED: price already near top, signal still bullish ───────────────
    # Rule E1: bullish signal but price regime says we're already near the high
    if is_bullish and price_regime == 'near_52w_high':
        return 'extended'

    # ── STRONG: all conditions clearly supportive ─────────────────────────────
    # Rule S1: bullish + large upside + not extended + not high vol
    if (is_bullish
            and upside_pct is not None and upside_pct >= _UPSIDE_STRONG
            and price_regime != 'near_52w_high'
            and volatility_regime in _LOW_VOL_REGIMES):
        return 'strong'

    # ── CONFIRMED: signal and upside aligned, price not extended ─────────────
    # Rule CF1: bullish + moderate upside + not extended
    if (is_bullish
            and upside_pct is not None and upside_pct >= _UPSIDE_CONFIRMED
            and price_regime != 'near_52w_high'):
        return 'confirmed'

    # ── WEAK: neutral signal or missing data ─────────────────────────────────
    # Rule W1: neutral direction or no upside data available
    if is_neutral or upside_pct is None:
        return 'weak'

    # ── Bearish cases ─────────────────────────────────────────────────────────
    # Bearish + not conflicted = confirmed (downside confirmed)
    if is_bearish:
        return 'confirmed'

    return 'weak'


def _classify_macro_confirmation(
    equity_signal: str,
    macro_signals: Dict[str, str],
) -> str:
    """
    Assess whether equity signal direction aligns with cross-asset macro proxies.

    Proxies checked:
      HYG (credit):   near_high / not near_low  → risk-on  → confirms long equity
      TLT (rates):    near_high = rates falling (risk-off)  → contradicts long equity
                      NOT near_high (mid_range/near_low)   → confirms long equity
      SPY (market):   near_high / long           → confirms long equity

    Scoring:
      Each proxy that confirms adds +1, contradicts adds -1, missing = 0.
      confirmed   : score == 3   (all three proxies confirm)
      partial     : score == 1 or 2
      unconfirmed : score <= 0
      no_data     : all three proxies missing from KB
    """
    hyg_sig = macro_signals.get(_CREDIT_PROXY, '')
    tlt_sig = macro_signals.get(_RATES_PROXY, '')
    spy_sig = macro_signals.get(_MARKET_PROXY, '')

    if not any([hyg_sig, tlt_sig, spy_sig]):
        return 'no_data'

    is_bullish_equity = equity_signal in _BULLISH_SIGNALS
    score = 0
    proxies_present = 0

    # HYG: near_high = credit spreads tight = risk-on → confirms long equity
    if hyg_sig:
        proxies_present += 1
        if is_bullish_equity:
            score += 1 if hyg_sig in _BULLISH_SIGNALS else -1
        else:
            score += 1 if hyg_sig in _BEARISH_SIGNALS else -1

    # TLT: near_high = rates falling = risk-off → CONTRADICTS long equity
    # TLT NOT near_high (rates stable/rising) → confirms long equity
    if tlt_sig:
        proxies_present += 1
        if is_bullish_equity:
            # Rising bonds (near_high) = risk-off = bad for longs
            score += -1 if tlt_sig in _BULLISH_SIGNALS else 1
        else:
            # Falling bonds = risk-on = bad for shorts
            score += 1 if tlt_sig in _BULLISH_SIGNALS else -1

    # SPY: near_high = broad market up → confirms long equity
    if spy_sig:
        proxies_present += 1
        if is_bullish_equity:
            score += 1 if spy_sig in _BULLISH_SIGNALS else -1
        else:
            score += 1 if spy_sig in _BEARISH_SIGNALS else -1

    if proxies_present == 0:
        return 'no_data'
    if score >= proxies_present:
        return 'confirmed'
    elif score > 0:
        return 'partial'
    else:
        return 'unconfirmed'


# ── Adapter ───────────────────────────────────────────────────────────────────

class SignalEnrichmentAdapter(BaseIngestAdapter):
    """
    Second-order signal enrichment. Reads current KB atoms and emits
    regime-conditional, interpretable signal atoms:

      price_regime       — where price sits vs fair value range
      upside_pct         — % upside to consensus target
      signal_quality     — coherence of signal composite (strong/confirmed/
                           extended/conflicted/weak)
      macro_confirmation — cross-asset alignment (confirmed/partial/
                           unconfirmed/no_data)

    All atoms: upsert=True, source prefix 'derived_signal_' (authority 0.65).
    No external API calls — reads only from the KB itself.
    """

    def __init__(self, tickers: Optional[List[str]] = None, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='signal_enrichment')
        self._db_path = db_path
        # If tickers supplied, only enrich those; otherwise enrich everything in KB
        self._tickers = [t.lower() for t in tickers] if tickers else None

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()

        ticker_atoms, macro_signals = _read_kb_atoms(self._db_path)

        atoms: List[RawAtom] = []
        enriched = 0
        skipped  = 0

        for ticker, preds in ticker_atoms.items():
            # Skip subjects that have no signal_direction (not an equity/ETF)
            if 'signal_direction' not in preds and 'last_price' not in preds:
                skipped += 1
                continue
            # Honour ticker filter if set
            if self._tickers and ticker not in self._tickers:
                continue

            # ── Extract raw values ─────────────────────────────────────────
            signal_dir  = preds.get('signal_direction', '')
            vol_regime  = preds.get('volatility_regime', '')

            last_price: Optional[float] = None
            price_target: Optional[float] = None
            upside_pct_val: Optional[float] = None

            try:
                last_price = float(preds['last_price'])
            except (KeyError, ValueError, TypeError):
                pass

            try:
                price_target = float(preds['price_target'])
            except (KeyError, ValueError, TypeError):
                pass

            if last_price is not None and price_target is not None and last_price > 0:
                upside_pct_val = round((price_target - last_price) / last_price * 100, 2)

            # ── Compute derived atoms ──────────────────────────────────────
            price_regime = _classify_price_regime(last_price, price_target, signal_dir)
            sig_quality  = _classify_signal_quality(
                signal_dir, vol_regime, price_regime, upside_pct_val
            )
            macro_conf   = _classify_macro_confirmation(signal_dir, macro_signals)

            src_base = f'derived_signal_{ticker}'
            meta = {
                'as_of':           now_iso,
                'input_signal':    signal_dir,
                'input_vol':       vol_regime,
                'input_price':     str(last_price),
                'input_target':    str(price_target),
                'upside_pct':      str(upside_pct_val),
                'price_regime':    price_regime,
                'macro_signals':   str(macro_signals),
            }

            # price_regime atom
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'price_regime',
                object     = price_regime,
                confidence = 0.75,   # derived, not observed
                source     = f'{src_base}_price_regime',
                metadata   = meta,
                upsert     = True,
            ))

            # upside_pct atom (only emit when computable)
            if upside_pct_val is not None:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'upside_pct',
                    object     = str(upside_pct_val),
                    confidence = 0.75,
                    source     = f'{src_base}_upside',
                    metadata   = meta,
                    upsert     = True,
                ))

            # signal_quality atom (only emit when signal_direction exists)
            if signal_dir:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'signal_quality',
                    object     = sig_quality,
                    confidence = 0.70,   # composite of three inputs
                    source     = f'{src_base}_quality',
                    metadata   = meta,
                    upsert     = True,
                ))

            # macro_confirmation atom (only emit when signal_direction exists)
            if signal_dir:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'macro_confirmation',
                    object     = macro_conf,
                    confidence = 0.65,   # depends on proxy coverage
                    source     = f'{src_base}_macro_confirm',
                    metadata   = meta,
                    upsert     = True,
                ))

            enriched += 1

        _logger.info(
            '[signal_enrichment] enriched %d tickers (%d atoms), skipped %d subjects',
            enriched, len(atoms), skipped,
        )
        return atoms
