"""
analytics/state_discretizer.py — Market State Discretization

Maps high-dimensional raw state dicts (from market_state_snapshots JSON) to
a small set of canonical state labels. This collapses the ~3,780 theoretical
combinations into the 30-80 states that actually occur in practice, giving the
transition matrix enough observations per state to be statistically meaningful.

CANONICAL DIMENSIONS
--------------------
  regime       : recovery | risk_on | risk_off | stagflation | unknown
  volatility   : low | medium | high | extreme | unknown
  fed_stance   : dovish | neutral | restrictive | unknown
  sector       : technology | energy | financials | healthcare | consumer | other | unknown
  tension      : low | medium | high | unknown
  signal_bias  : bullish | bearish | mixed | unknown

state_id format: "{regime}_{volatility}_{fed_stance}_{sector}_{tension}_{signal_bias}"
Example: "recovery_high_restrictive_technology_medium_bullish"

USAGE
-----
    from analytics.state_discretizer import discretize, discretize_global, CanonicalState
    cs = discretize(snapshot_state_dict)
    print(cs.state_id)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


# ── Canonical state dataclass ──────────────────────────────────────────────────

@dataclass
class CanonicalState:
    state_id:        str   # deterministic joined string of the 6 dimensions
    regime:          str   # recovery | risk_on | risk_off | stagflation | unknown
    volatility:      str   # low | medium | high | extreme | unknown
    fed_stance:      str   # dovish | neutral | restrictive | unknown
    dominant_sector: str   # technology | energy | financials | healthcare | consumer | other | unknown
    tension:         str   # low | medium | high | unknown
    signal_bias:     str   # bullish | bearish | mixed | unknown

    def label(self) -> str:
        """Human-readable label, e.g. 'Recovery · High Vol · Restrictive · Tech'"""
        parts = [
            self.regime.replace('_', ' ').title(),
            self.volatility.title() + ' Vol',
            self.fed_stance.title(),
            self.dominant_sector.title(),
        ]
        if self.tension not in ('unknown', 'low'):
            parts.append(self.tension.title() + ' Tension')
        return ' · '.join(p for p in parts if p and 'unknown' not in p.lower())


# ── Bucket maps ───────────────────────────────────────────────────────────────

_REGIME_MAP = {
    # risk_on
    'risk_on_expansion':     'risk_on',
    'risk_on':               'risk_on',
    'bull_market':           'risk_on',
    'bull':                  'risk_on',
    'expansion':             'risk_on',
    'growth':                'risk_on',
    'risk on':               'risk_on',
    # risk_off
    'risk_off_contraction':  'risk_off',
    'risk_off':              'risk_off',
    'contraction':           'risk_off',
    'recession':             'risk_off',
    'bear_market':           'risk_off',
    'bear':                  'risk_off',
    'risk off':              'risk_off',
    # recovery
    'recovery':              'recovery',
    'post_correction':       'recovery',
    'rebound':               'recovery',
    'bounce':                'recovery',
    # stagflation
    'stagflation':           'stagflation',
    'stag':                  'stagflation',
    # late_cycle — map to risk_off for simplicity
    'late_cycle':            'risk_off',
    'late cycle':            'risk_off',
}

_VOL_MAP = {
    'low':     'low',
    'low_vol': 'low',
    'calm':    'low',
    'quiet':   'low',
    'medium':  'medium',
    'normal':  'medium',
    'mid':     'medium',
    'moderate': 'medium',
    'high':    'high',
    'elevated': 'high',
    'extreme': 'extreme',
    'panic':   'extreme',
    'crisis':  'extreme',
}

_FED_MAP = {
    'dovish':               'dovish',
    'easing':               'dovish',
    'accommodative':        'dovish',
    'cutting':              'dovish',
    'rate_cut':             'dovish',
    'neutral':              'neutral',
    'on_hold':              'neutral',
    'hold':                 'neutral',
    'pause':                'neutral',
    'restrictive':          'restrictive',
    'hawkish':              'restrictive',
    'tightening':           'restrictive',
    'hiking':               'restrictive',
    'rate_hike':            'restrictive',
    'neutral_to_restrictive': 'restrictive',
    # collapse to nearest
    'slightly_dovish':      'dovish',
    'slightly_hawkish':     'restrictive',
}

_SECTOR_MAP = {
    'technology':          'technology',
    'tech':                'technology',
    'information_technology': 'technology',
    'it':                  'technology',
    'software':            'technology',
    'semiconductors':      'technology',
    'energy':              'energy',
    'oil':                 'energy',
    'oil_gas':             'energy',
    'oil & gas':           'energy',
    'natural_gas':         'energy',
    'financials':          'financials',
    'financial':           'financials',
    'banking':             'financials',
    'banks':               'financials',
    'insurance':           'financials',
    'healthcare':          'healthcare',
    'health':              'healthcare',
    'pharma':              'healthcare',
    'biotech':             'healthcare',
    'consumer':            'consumer',
    'consumer_cyclical':   'consumer',
    'consumer_discretionary': 'consumer',
    'consumer_staples':    'consumer',
    'retail':              'consumer',
    'industrials':         'other',
    'materials':           'other',
    'utilities':           'other',
    'real_estate':         'other',
    'communication':       'other',
    'communications':      'other',
    'defense':             'other',
    'other':               'other',
}

_TENSION_MAP = {
    'low':    'low',
    'medium': 'medium',
    'mid':    'medium',
    'high':   'high',
}


# ── Helper functions ──────────────────────────────────────────────────────────

def _map_regime(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    return _REGIME_MAP.get(val.lower().strip(), 'unknown')


def _map_volatility(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    return _VOL_MAP.get(val.lower().strip(), 'unknown')


def _map_fed(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    return _FED_MAP.get(val.lower().strip(), 'unknown')


def _map_sector(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    v = val.lower().strip()
    if v in _SECTOR_MAP:
        return _SECTOR_MAP[v]
    # Partial match
    for k, mapped in _SECTOR_MAP.items():
        if k in v or v in k:
            return mapped
    return 'other'


def _map_tension(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    return _TENSION_MAP.get(val.lower().strip(), 'unknown')


def _signal_bias_from_direction(
    direction: Optional[str],
    conviction: Optional[str],
) -> str:
    if not direction:
        return 'unknown'
    d = direction.lower().strip()
    c = (conviction or '').lower().strip()
    if d in ('bullish', 'long', 'buy'):
        if c in ('high', 'medium', 'mid', 'moderate', ''):
            return 'bullish'
        return 'mixed'
    if d in ('bearish', 'short', 'sell'):
        return 'bearish'
    if d in ('neutral', 'mixed', 'sideways'):
        return 'mixed'
    return 'unknown'


def _signal_bias_from_sectors(
    bullish_sectors: List[str],
    bearish_sectors: List[str],
) -> str:
    """For global snapshots: bias from ratio of bullish vs bearish sectors."""
    n_bull = len(bullish_sectors)
    n_bear = len(bearish_sectors)
    total = n_bull + n_bear
    if total == 0:
        return 'unknown'
    if n_bull > n_bear * 1.5:
        return 'bullish'
    if n_bear > n_bull * 1.5:
        return 'bearish'
    return 'mixed'


def _build_state_id(*parts: str) -> str:
    return '_'.join(p or 'unknown' for p in parts)


# ── Main discretization functions ─────────────────────────────────────────────

def discretize(state_json: dict) -> CanonicalState:
    """
    Map a raw ticker snapshot state dict to a CanonicalState.
    Reads: signal_direction, conviction_tier, price_regime, volatility_regime,
           sector, central_bank_stance (may be absent for ticker snaps),
           gdelt_tension_level.
    """
    regime    = _map_regime(
        state_json.get('regime_label')
        or state_json.get('market_regime')
        or state_json.get('price_regime')
    )
    volatility = _map_volatility(state_json.get('volatility_regime'))
    fed        = _map_fed(state_json.get('central_bank_stance'))
    sector     = _map_sector(state_json.get('sector'))
    tension    = _map_tension(state_json.get('gdelt_tension_level'))
    bias       = _signal_bias_from_direction(
        state_json.get('signal_direction'),
        state_json.get('conviction_tier'),
    )

    sid = _build_state_id(regime, volatility, fed, sector, tension, bias)
    return CanonicalState(
        state_id        = sid,
        regime          = regime,
        volatility      = volatility,
        fed_stance      = fed,
        dominant_sector = sector,
        tension         = tension,
        signal_bias     = bias,
    )


def discretize_global(state_json: dict) -> CanonicalState:
    """
    Map a raw global market snapshot state dict to a CanonicalState.
    Reads: regime_label, volatility_regime, central_bank_stance,
           gdelt_tension_level, top_sectors_bullish, top_sectors_bearish.
    """
    regime = _map_regime(
        state_json.get('regime_label')
        or state_json.get('market_regime')
    )
    volatility = _map_volatility(state_json.get('volatility_regime'))
    fed        = _map_fed(state_json.get('central_bank_stance'))
    tension    = _map_tension(state_json.get('gdelt_tension_level'))

    # Dominant sector: first bullish sector if available
    bull_sectors = state_json.get('top_sectors_bullish') or []
    bear_sectors = state_json.get('top_sectors_bearish') or []
    dominant     = _map_sector(bull_sectors[0] if bull_sectors else None)

    bias = _signal_bias_from_sectors(bull_sectors, bear_sectors)

    sid = _build_state_id(regime, volatility, fed, dominant, tension, bias)
    return CanonicalState(
        state_id        = sid,
        regime          = regime,
        volatility      = volatility,
        fed_stance      = fed,
        dominant_sector = dominant,
        tension         = tension,
        signal_bias     = bias,
    )


def decode_state_id(state_id: str) -> CanonicalState:
    """
    Reconstruct a CanonicalState from a stored state_id string.
    Useful for displaying stored transition records without re-querying snapshots.
    """
    parts = state_id.split('_', 5)
    while len(parts) < 6:
        parts.append('unknown')
    return CanonicalState(
        state_id        = state_id,
        regime          = parts[0],
        volatility      = parts[1],
        fed_stance      = parts[2],
        dominant_sector = parts[3],
        tension         = parts[4],
        signal_bias     = parts[5] if len(parts) > 5 else 'unknown',
    )
