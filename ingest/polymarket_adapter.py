"""
ingest/polymarket_adapter.py — Polymarket Prediction Market Adapter

Fetches prediction market odds from Polymarket's Gamma API for a curated
list of macro and geopolitical markets. Uses direct slug-based fetching
rather than keyword search (the ?q= search param is non-functional on the
Gamma API and returns stale 2020 markets regardless of query text).

ATOMS PRODUCED
==============
  subject         predicate                    value
  ─────────────────────────────────────────────────────────────────
  polymarket      {atom_slug}_yes_prob         float 0..1 (YES price)
  polymarket      {atom_slug}_volume           float (total USD volume)
  macro_risk      {category}_market            "{label}:{prob}" roll-up

CURATED MARKETS
===============
12 high-liquidity markets across fed_policy, geopolitical, macro, crypto.
Minimum volume: $400k to ensure price discovery is meaningful.
Slugs are fixed to specific market IDs — update when markets expire.

Schedule: 3600s (hourly — prediction markets move fast)
"""

from __future__ import annotations

import json as _json
import logging
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_GAMMA_BASE = 'https://gamma-api.polymarket.com/markets'
_MIN_VOLUME = 400_000   # ignore markets with < $400k volume (thin / unreliable)

# ── Curated market list ─────────────────────────────────────────────────────
# (atom_slug, category, polymarket_slug, description)
# Fetch via GET /markets?slug={polymarket_slug}
# YES price = outcomePrices[0] (JSON-encoded string array)
# Last verified: 2026-03-18

_CURATED_MARKETS: List[Tuple[str, str, str, str]] = [
    # Fed policy
    ('fed_cuts_none_2026',      'fed_policy',   'will-no-fed-rate-cuts-happen-in-2026',         'No Fed rate cuts in 2026'),
    ('fed_1cut_2026',           'fed_policy',   'will-1-fed-rate-cut-happen-in-2026',           '1 Fed rate cut in 2026'),
    ('fed_2cuts_2026',          'fed_policy',   'will-2-fed-rate-cuts-happen-in-2026',          '2 Fed rate cuts in 2026'),
    ('fed_3cuts_2026',          'fed_policy',   'will-3-fed-rate-cuts-happen-in-2026',          '3 Fed rate cuts in 2026'),
    # Geopolitical
    ('ukraine_ceasefire_mar31', 'geopolitical', 'russia-x-ukraine-ceasefire-by-march-31-2026',  'Ukraine ceasefire by Mar 31 2026'),
    ('ukraine_ceasefire_2026',  'geopolitical', 'russia-x-ukraine-ceasefire-before-2027',       'Ukraine ceasefire by end 2026'),
    ('taiwan_invasion_2026',    'geopolitical', 'will-china-invade-taiwan-before-2027',         'China invades Taiwan by end 2026'),
    ('taiwan_blockade_jun30',   'geopolitical', 'will-china-blockade-taiwan-by-june-30',        'China blockade Taiwan by Jun 30'),
    ('zelenskyy_out_2026',      'geopolitical', 'zelenskyy-out-as-ukraine-president-before-2027','Zelenskyy out as president 2026'),
    # Macro
    ('us_recession_2026',       'macro',        'us-recession-by-end-of-2026',                  'US recession by end 2026'),
    # Crypto
    ('btc_150k_mar31',          'crypto',       'will-bitcoin-hit-150k-by-march-31-2026',       'Bitcoin $150k by Mar 31 2026'),
    ('btc_150k_jun30',          'crypto',       'will-bitcoin-hit-150k-by-june-30-2026',        'Bitcoin $150k by Jun 30 2026'),
]


def _prob_label(prob: float) -> str:
    """Convert probability to a directional label for roll-up atoms."""
    if prob >= 0.70: return 'high_probability'
    if prob >= 0.50: return 'likely'
    if prob >= 0.30: return 'unlikely'
    return 'low_probability'


def _fetch_market_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a single market by its exact Polymarket slug.
    Returns the market dict or None on failure.
    Uses direct slug param which is reliable (unlike ?q= text search).
    """
    url = _GAMMA_BASE + '?' + urllib.parse.urlencode({'slug': slug})
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingGalaxyKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        markets = data if isinstance(data, list) else data.get('markets', [])
        return markets[0] if markets else None
    except Exception as exc:
        _logger.debug('Polymarket slug fetch failed for %r: %s', slug, exc)
        return None


def _extract_yes_prob(market: Dict[str, Any]) -> Optional[float]:
    """
    Extract YES probability from market dict.
    outcomePrices is a JSON-encoded string like '["0.355", "0.645"]'
    where [0] = YES price, [1] = NO price.
    """
    prices_raw = market.get('outcomePrices')
    if not prices_raw:
        return None
    try:
        prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        return float(prices[0])
    except (ValueError, TypeError, IndexError):
        return None


class PolymarketAdapter(BaseIngestAdapter):
    """
    Fetches prediction market probabilities from Polymarket via direct slug lookup.
    Produces atoms for each curated market plus category-level roll-ups.
    """

    name = 'polymarket_adapter'

    def __init__(self):
        super().__init__(self.name)

    def fetch(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []

        # Category roll-up accumulators: category → list of (prob, slug)
        category_probs: Dict[str, List[Tuple[float, str]]] = {}

        for atom_slug, category, poly_slug, description in _CURATED_MARKETS:
            market = _fetch_market_by_slug(poly_slug)
            if not market:
                _logger.debug('Polymarket: no market found for slug %r', poly_slug)
                continue

            # Volume check
            vol_raw = market.get('volumeNum') or market.get('volume') or 0
            try:
                vol = float(vol_raw)
            except (ValueError, TypeError):
                vol = 0.0
            if vol < _MIN_VOLUME:
                _logger.debug('Polymarket: skipping %r (volume $%.0f < $%.0f)', poly_slug, vol, _MIN_VOLUME)
                continue

            yes_prob = _extract_yes_prob(market)
            if yes_prob is None:
                _logger.debug('Polymarket: could not extract YES prob for %r', poly_slug)
                continue

            question = market.get('question', description)[:200]

            # YES probability atom
            atoms.append(RawAtom(
                subject='polymarket',
                predicate=f'{atom_slug}_yes_prob',
                object=str(round(yes_prob, 4)),
                confidence=min(0.95, 0.5 + vol / 50_000_000),  # higher vol → higher confidence
                source=f'polymarket_{atom_slug}',
                upsert=True,
                metadata={
                    'question':    question,
                    'category':    category,
                    'slug':        poly_slug,
                    'volume_usd':  round(vol, 2),
                    'label':       _prob_label(yes_prob),
                },
            ))

            # Volume atom
            atoms.append(RawAtom(
                subject='polymarket',
                predicate=f'{atom_slug}_volume',
                object=str(round(vol, 2)),
                confidence=0.90,
                source=f'polymarket_{atom_slug}',
                upsert=True,
                metadata={'question': question, 'category': category},
            ))

            # Accumulate for category roll-up
            category_probs.setdefault(category, []).append((yes_prob, atom_slug))

            _logger.info(
                'Polymarket %s: YES=%.3f vol=$%.0fM',
                atom_slug, yes_prob, vol / 1_000_000,
            )

        # Category-level roll-up atoms (e.g. macro_risk.fed_policy_market)
        for cat, entries in category_probs.items():
            if not entries:
                continue
            avg_prob = sum(p for p, _ in entries) / len(entries)
            label = _prob_label(avg_prob)
            atoms.append(RawAtom(
                subject='macro_risk',
                predicate=f'{cat}_market',
                object=f'{label}:{round(avg_prob, 3)}',
                confidence=0.80,
                source='polymarket_rollup',
                upsert=True,
                metadata={'category': cat, 'markets': [s for _, s in entries]},
            ))

        _logger.info('PolymarketAdapter: %d atoms from %d curated markets', len(atoms), len(_CURATED_MARKETS))
        return atoms

    def transform(self, raw: List[RawAtom]) -> List[RawAtom]:
        return raw
