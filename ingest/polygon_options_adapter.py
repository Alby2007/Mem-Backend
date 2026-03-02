"""
ingest/polygon_options_adapter.py — Polygon/Massive Options Greeks Adapter

Fetches real Greeks (delta, gamma, theta, vega), true implied volatility,
and open interest from the Massive (Polygon) options snapshot API.

Complements the existing OptionsAdapter (yfinance) — both run in parallel.
This adapter adds high-quality Greeks atoms that yfinance cannot provide.

Requires env var: POLYGON_API_KEY

ATOMS PRODUCED
==============
All atoms use source prefix 'polygon_options_{ticker}' (authority 0.80).
All atoms are upsert=True.  US-listed tickers only (FTSE names excluded —
not covered by Polygon US options data).

  Predicate          Example value   Notes
  ─────────────────────────────────────────────────────────────────────
  delta_atm          "0.51"          Delta of ATM call (front expiry)
  gamma_atm          "0.032"         Gamma of ATM contract
  theta_atm          "-0.18"         Daily theta (time decay per day)
  vega_atm           "0.42"          Vega (IV sensitivity)
  iv_true            "31.4"          True implied volatility % (ATM)
  put_call_oi_ratio  "1.38"          Total put OI / total call OI
                                     (real OI, more accurate than yfinance)
  gamma_exposure     "2340000"       Aggregate GEX across all contracts
                                     (dealer hedging pressure proxy)

ENDPOINT
========
GET https://api.massive.com/v3/snapshot/options/{ticker}?apiKey={key}

Response: results[] → each result has:
  greeks.delta, greeks.gamma, greeks.theta, greeks.vega
  implied_volatility
  open_interest
  details.contract_type  ("call" | "put")
  details.strike_price
  details.expiration_date

TICKER SCOPE
============
US single-name equities only — same universe as OptionsAdapter minus FTSE
(.L suffix) names which are not in Polygon's US options coverage.

INTERVAL
========
1800s (30 min) — matches OptionsAdapter cadence.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_BASE_URL = 'https://api.massive.com/v3/snapshot/options'

# US-only options universe (FTSE .L tickers excluded)
_US_OPTIONS_TICKERS = [
    'COIN', 'HOOD', 'MSTR', 'PLTR', 'NVDA',
    'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT', 'MA',
    'SPY',   # index-level for market Greeks baseline
]

_DEFAULT_SLEEP_SEC = 0.5   # Polygon rate limit buffer
_MAX_CONTRACTS     = 200   # cap per ticker to avoid huge payloads


def _api_key() -> Optional[str]:
    return os.environ.get('POLYGON_API_KEY', '').strip() or None


def _nearest_expiry_atm_contracts(
    results: list,
    current_price: float,
) -> tuple[list, list]:
    """
    Filter results to the nearest expiry only, split into calls and puts.
    Returns (atm_contracts, all_contracts_this_expiry).
    ATM = contracts within 5% of current price.
    """
    if not results or current_price <= 0:
        return [], results

    # Find nearest expiry date
    expiries = sorted({
        r.get('details', {}).get('expiration_date', '')
        for r in results
        if r.get('details', {}).get('expiration_date')
    })
    if not expiries:
        return [], results

    nearest = expiries[0]
    front = [r for r in results
             if r.get('details', {}).get('expiration_date') == nearest]

    lo = current_price * 0.95
    hi = current_price * 1.05
    atm = [
        r for r in front
        if lo <= (r.get('details', {}).get('strike_price') or 0) <= hi
    ]
    return atm, front


def _extract_greeks(contracts: list) -> Optional[dict]:
    """
    Average Greeks across ATM contracts (calls only for delta/gamma/theta/vega).
    Returns None when no valid data.
    """
    calls = [r for r in contracts
             if r.get('details', {}).get('contract_type') == 'call']
    if not calls:
        return None

    fields = ['delta', 'gamma', 'theta', 'vega']
    sums   = {f: 0.0 for f in fields}
    counts = {f: 0   for f in fields}
    iv_sum, iv_count = 0.0, 0

    for c in calls:
        g = c.get('greeks') or {}
        for f in fields:
            v = g.get(f)
            if v is not None:
                try:
                    sums[f]   += float(v)
                    counts[f] += 1
                except (TypeError, ValueError):
                    pass
        iv = c.get('implied_volatility')
        if iv is not None:
            try:
                iv_sum   += float(iv)
                iv_count += 1
            except (TypeError, ValueError):
                pass

    result = {}
    for f in fields:
        if counts[f] > 0:
            result[f] = round(sums[f] / counts[f], 4)
    if iv_count > 0:
        result['iv'] = round((iv_sum / iv_count) * 100, 2)  # → percent

    return result if result else None


def _put_call_oi(results: list) -> Optional[float]:
    """Total put OI / total call OI across all contracts."""
    call_oi = sum(
        (r.get('open_interest') or 0)
        for r in results
        if r.get('details', {}).get('contract_type') == 'call'
    )
    put_oi = sum(
        (r.get('open_interest') or 0)
        for r in results
        if r.get('details', {}).get('contract_type') == 'put'
    )
    if call_oi <= 0:
        return None
    return round(put_oi / call_oi, 3)


def _gamma_exposure(results: list, current_price: float) -> Optional[float]:
    """
    Aggregate gamma exposure (GEX) proxy.
    GEX = gamma * open_interest * 100 * current_price²
    Positive = calls dominate (dealers short gamma, amplify moves).
    Negative = puts dominate (dealers long gamma, dampen moves).
    """
    if current_price <= 0:
        return None

    gex = 0.0
    has_data = False
    for r in results:
        g    = (r.get('greeks') or {}).get('gamma')
        oi   = r.get('open_interest')
        ctype = r.get('details', {}).get('contract_type')
        if g is None or oi is None or not ctype:
            continue
        try:
            sign = 1 if ctype == 'call' else -1
            gex += sign * float(g) * float(oi) * 100 * (current_price ** 2)
            has_data = True
        except (TypeError, ValueError):
            pass

    return round(gex, 0) if has_data else None


class PolygonOptionsAdapter(BaseIngestAdapter):
    """
    Fetches real options Greeks from Polygon/Massive and emits KB atoms.

    Gracefully skips if POLYGON_API_KEY is not set or API is unavailable.
    """

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        sleep_sec: float = _DEFAULT_SLEEP_SEC,
    ):
        super().__init__(name='polygon_options')
        self._tickers   = tickers or _US_OPTIONS_TICKERS
        self._sleep_sec = sleep_sec

    def fetch(self) -> List[RawAtom]:
        key = _api_key()
        if not key:
            _logger.info('[polygon_options] POLYGON_API_KEY not set — skipping')
            return []

        try:
            import requests
        except ImportError:
            _logger.warning('[polygon_options] requests not installed — skipping')
            return []

        atoms: List[RawAtom] = []
        succeeded, failed = 0, 0

        for sym in self._tickers:
            try:
                new = self._fetch_one(sym, key, requests)
                atoms.extend(new)
                succeeded += 1
            except Exception as exc:
                _logger.debug('[polygon_options] %s failed: %s', sym, exc)
                failed += 1

            if self._sleep_sec > 0:
                time.sleep(self._sleep_sec)

        _logger.info(
            '[polygon_options] fetched %d tickers (%d atoms), %d failed',
            succeeded, len(atoms), failed,
        )
        return atoms

    def _fetch_one(self, sym: str, key: str, requests) -> List[RawAtom]:
        url     = f'{_BASE_URL}/{sym}'
        resp    = requests.get(url, params={'apiKey': key, 'limit': _MAX_CONTRACTS}, timeout=10)

        if resp.status_code == 404:
            _logger.debug('[polygon_options] %s: 404 not found', sym)
            return []
        if resp.status_code != 200:
            _logger.warning('[polygon_options] %s: HTTP %d', sym, resp.status_code)
            return []

        data    = resp.json()
        results = data.get('results', [])
        if not results:
            return []

        # Get current price from the snapshot's underlying asset field
        current_price = 0.0
        underlying = data.get('underlying_asset', {})
        if underlying:
            try:
                current_price = float(
                    underlying.get('price') or
                    underlying.get('last_price') or 0.0
                )
            except (TypeError, ValueError):
                current_price = 0.0

        now_iso = datetime.now(timezone.utc).isoformat()
        src     = f'polygon_options_{sym.lower()}'
        meta    = {'as_of': now_iso, 'ticker': sym}

        atm_contracts, front_contracts = _nearest_expiry_atm_contracts(
            results, current_price
        )

        greeks  = _extract_greeks(atm_contracts or front_contracts)
        pc_oi   = _put_call_oi(results)
        gex     = _gamma_exposure(front_contracts or results, current_price)

        result: List[RawAtom] = []
        subj = sym.lower()

        if greeks:
            for predicate, key_name, conf in [
                ('delta_atm', 'delta', 0.80),
                ('gamma_atm', 'gamma', 0.80),
                ('theta_atm', 'theta', 0.80),
                ('vega_atm',  'vega',  0.80),
            ]:
                if key_name in greeks:
                    result.append(RawAtom(
                        subject    = subj,
                        predicate  = predicate,
                        object     = str(greeks[key_name]),
                        confidence = conf,
                        source     = src,
                        metadata   = meta,
                        upsert     = True,
                    ))

            if 'iv' in greeks:
                result.append(RawAtom(
                    subject    = subj,
                    predicate  = 'iv_true',
                    object     = str(greeks['iv']),
                    confidence = 0.82,
                    source     = src,
                    metadata   = meta,
                    upsert     = True,
                ))

        if pc_oi is not None:
            result.append(RawAtom(
                subject    = subj,
                predicate  = 'put_call_oi_ratio',
                object     = str(pc_oi),
                confidence = 0.78,
                source     = src,
                metadata   = meta,
                upsert     = True,
            ))

        if gex is not None:
            result.append(RawAtom(
                subject    = subj,
                predicate  = 'gamma_exposure',
                object     = str(gex),
                confidence = 0.70,
                source     = src,
                metadata   = meta,
                upsert     = True,
            ))

        return result
