"""
ingest/polymarket_adapter.py — Polymarket Prediction Market Adapter

Fetches prediction market odds from Polymarket's Gamma API for a curated
list of macro and geopolitical markets. Prediction market probabilities are
calibrated, forward-looking signals that complement the KB's backward-looking
historical data.

No API key required. Free public API.
Endpoint: https://gamma-api.polymarket.com/markets

Atoms produced:
  - polymarket | {slug}_yes_prob  | {float 0..1}     — current YES probability
  - polymarket | {slug}_volume    | {float}           — total volume USD (liquidity)
  - macro_risk | fed_policy_market   | {label}        — roll-up category atoms
  - macro_risk | trade_war_market    | {label}
  - macro_risk | geopolitical_market | {label}

Source: prediction_market_polymarket  (authority 0.73, half-life 1d)
Schedule: 3600s (hourly — prediction markets move fast)
"""

from __future__ import annotations

import json as _json
import logging
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_GAMMA_BASE = 'https://gamma-api.polymarket.com/markets'

# Minimum liquidity filter — ignore markets with < $10k volume (too thin / unreliable)
_MIN_VOLUME_USD = 10_000

# Curated market list: (slug_keyword, category, atom_slug, description)
# slug_keyword is matched against the Polymarket market question/slug
# category: 'fed_policy' | 'trade_war' | 'geopolitical' | 'macro' | 'energy'
_CURATED_MARKETS: List[Tuple[str, str, str, str]] = [
    # Fed policy
    ('fed rate cut',        'fed_policy',   'fed_rate_cut_2025',       'Fed rate cut in 2025'),
    ('fed rate hike',       'fed_policy',   'fed_rate_hike_2025',      'Fed rate hike in 2025'),
    ('rate cut march',      'fed_policy',   'fed_cut_march',           'Fed rate cut March 2025'),
    ('rate cut june',       'fed_policy',   'fed_cut_june',            'Fed rate cut June 2025'),
    ('fed funds rate',      'fed_policy',   'fed_funds_target',        'Fed funds rate target'),
    # Trade / tariffs
    ('us china tariff',     'trade_war',    'us_china_tariff',         'US-China tariff escalation'),
    ('us tariff',           'trade_war',    'us_tariff_broad',         'Broad US tariff policy'),
    ('trade war',           'trade_war',    'trade_war_escalation',    'Global trade war escalation'),
    # Geopolitical
    ('ukraine ceasefire',   'geopolitical', 'ukraine_ceasefire',       'Ukraine ceasefire'),
    ('ukraine russia',      'geopolitical', 'ukraine_russia_peace',    'Ukraine-Russia peace deal'),
    ('israel gaza',         'geopolitical', 'israel_gaza_ceasefire',   'Israel-Gaza ceasefire'),
    ('iran nuclear',        'geopolitical', 'iran_nuclear',            'Iran nuclear deal/conflict'),
    ('taiwan',              'geopolitical', 'china_taiwan_conflict',   'China-Taiwan conflict'),
    # Macro / economic
    ('recession 2025',      'macro',        'us_recession_2025',       'US recession 2025'),
    ('recession 2026',      'macro',        'us_recession_2026',       'US recession 2026'),
    ('inflation',           'macro',        'us_inflation_path',       'US inflation trajectory'),
    ('sp500',               'macro',        'sp500_outcome',           'S&P 500 outcome'),
    ('us gdp',              'macro',        'us_gdp_growth',           'US GDP growth'),
    # Energy / commodities
    ('oil price',           'energy',       'oil_price_target',        'Oil price outcome'),
    ('natural gas',         'energy',       'natgas_price',            'Natural gas price'),
    # Crypto
    ('bitcoin',             'crypto',       'btc_price_outcome',       'Bitcoin price outcome'),
]


def _prob_label(prob: float) -> str:
    """Convert probability to a directional label for roll-up atoms."""
    if prob >= 0.70:
        return 'high_probability'
    if prob >= 0.50:
        return 'likely'
    if prob >= 0.30:
        return 'unlikely'
    return 'low_probability'


def _fetch_markets(keyword: str) -> List[Dict[str, Any]]:
    """Fetch markets matching a keyword from Gamma API."""
    params = {
        'q':         keyword,
        'limit':     5,
        'active':    'true',
        'closed':    'false',
    }
    url = _GAMMA_BASE + '?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingGalaxyKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and 'markets' in data:
            return data['markets']
        return []
    except Exception as exc:
        _logger.debug('Polymarket fetch failed for %r: %s', keyword, exc)
        return []


def _best_market(markets: List[Dict[str, Any]], keyword: str) -> Optional[Dict[str, Any]]:
    """Pick the highest-volume market above the liquidity filter."""
    keyword_lower = keyword.lower()
    candidates = []
    for m in markets:
        question = (m.get('question') or m.get('title') or '').lower()
        if keyword_lower not in question:
            continue
        try:
            vol = float(m.get('volume', 0) or m.get('volumeNum', 0) or 0)
        except (TypeError, ValueError):
            vol = 0.0
        if vol < _MIN_VOLUME_USD:
            continue
        candidates.append((vol, m))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _extract_yes_prob(market: Dict[str, Any]) -> Optional[float]:
    """Extract the YES probability from a Polymarket market dict."""
    # Gamma API returns outcomePrices as JSON string array, e.g. '["0.72", "0.28"]'
    outcome_prices = market.get('outcomePrices')
    if outcome_prices:
        try:
            if isinstance(outcome_prices, str):
                prices = _json.loads(outcome_prices)
            else:
                prices = outcome_prices
            # First entry is YES price
            return round(float(prices[0]), 4)
        except (ValueError, IndexError, _json.JSONDecodeError):
            pass

    # Fallback: bestBid/bestAsk midpoint
    bid = market.get('bestBid')
    ask = market.get('bestAsk')
    if bid is not None and ask is not None:
        try:
            return round((float(bid) + float(ask)) / 2, 4)
        except (TypeError, ValueError):
            pass

    # Fallback: lastTradePrice
    ltp = market.get('lastTradePrice') or market.get('price')
    if ltp is not None:
        try:
            return round(float(ltp), 4)
        except (TypeError, ValueError):
            pass

    return None


class PolymarketAdapter(BaseIngestAdapter):
    """
    Polymarket prediction market adapter.

    Fetches YES probabilities and liquidity for a curated list of macro and
    geopolitical markets. Provides calibrated forward-looking signals.
    No API key required.
    """

    def __init__(self):
        super().__init__(name='polymarket')

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'prediction_market_polymarket'
        meta_base = {'fetched_at': now_iso, 'source_url': _GAMMA_BASE}

        atoms: List[RawAtom] = []
        seen_slugs: Dict[str, bool] = {}

        # Category roll-up accumulators: category → list of (prob, slug)
        category_probs: Dict[str, List[float]] = {}

        for keyword, category, atom_slug, description in _CURATED_MARKETS:
            # Avoid duplicate slug writes from overlapping keyword matches
            if atom_slug in seen_slugs:
                continue

            markets = _fetch_markets(keyword)
            market  = _best_market(markets, keyword)
            if not market:
                _logger.debug('Polymarket: no liquid market found for %r', keyword)
                continue

            yes_prob = _extract_yes_prob(market)
            if yes_prob is None:
                _logger.debug('Polymarket: could not extract YES prob for %r', keyword)
                continue

            try:
                volume = float(market.get('volume', 0) or market.get('volumeNum', 0) or 0)
            except (TypeError, ValueError):
                volume = 0.0

            question = market.get('question') or market.get('title') or description
            seen_slugs[atom_slug] = True

            atoms.append(RawAtom(
                subject='polymarket',
                predicate=f'{atom_slug}_yes_prob',
                object=str(yes_prob),
                confidence=0.73,
                source=source,
                metadata={
                    **meta_base,
                    'question':    question[:200],
                    'category':    category,
                    'description': description,
                    'market_id':   market.get('id', ''),
                },
                upsert=True,
            ))
            atoms.append(RawAtom(
                subject='polymarket',
                predicate=f'{atom_slug}_volume',
                object=str(round(volume, 0)),
                confidence=0.73,
                source=source,
                metadata={**meta_base, 'question': question[:200], 'category': category},
                upsert=True,
            ))

            category_probs.setdefault(category, []).append(yes_prob)

        # ── Category roll-up atoms ─────────────────────────────────────────────
        _CATEGORY_PREDICATES = {
            'fed_policy':    'fed_policy_market',
            'trade_war':     'trade_war_market',
            'geopolitical':  'geopolitical_market',
            'macro':         'macro_market',
            'energy':        'energy_market',
        }
        for cat, probs in category_probs.items():
            if not probs:
                continue
            avg_prob = sum(probs) / len(probs)
            label    = _prob_label(avg_prob)
            predicate = _CATEGORY_PREDICATES.get(cat, f'{cat}_market')
            atoms.append(RawAtom(
                subject='macro_risk',
                predicate=predicate,
                object=f'{label}:{round(avg_prob, 2)}',
                confidence=0.70,
                source=source,
                metadata={
                    **meta_base,
                    'category':     cat,
                    'avg_prob':     round(avg_prob, 4),
                    'market_count': len(probs),
                },
                upsert=True,
            ))

        self._logger.info(
            'Polymarket adapter: %d markets processed, %d atoms produced',
            len(seen_slugs), len(atoms),
        )
        return atoms
