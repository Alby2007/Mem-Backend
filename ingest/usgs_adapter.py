"""
ingest/usgs_adapter.py — USGS Earthquake Feed Ingest Adapter

Fetches M4.5+ earthquakes from the USGS Earthquake Hazards GeoJSON feed and
emits risk atoms only for events near commodity-producing regions.

No API key required. Feed updated every 5 minutes by USGS.

Atoms produced:
  - usgs_risk | {region}_earthquake | M{magnitude}_{depth}km
  - usgs_risk | {region}_seismic_activity | elevated | normal

Source prefix: geopolitical_data_usgs  (authority 0.70, half-life 1d)
Interval: recommended 1h
"""

from __future__ import annotations

import json as _json
import logging
import math
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_USGS_FEED = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson'

# Minimum magnitude to emit an atom (M4.5+ from feed, but we further filter)
_MIN_MAGNITUDE = 4.5
_ELEVATED_MAGNITUDE = 6.0  # M6+ = elevated seismic activity

# ── Commodity-producing regions with bounding boxes ──────────────────────────
# Each region: (label, min_lat, max_lat, min_lon, max_lon, commodity_note)
class _Region(NamedTuple):
    label:          str
    min_lat:        float
    max_lat:        float
    min_lon:        float
    max_lon:        float
    commodity_note: str


_COMMODITY_REGIONS: List[_Region] = [
    # Chile / Peru — copper (world's largest producers)
    _Region('chile_copper_zone',   -56.0, -15.0, -76.0, -65.0, 'copper'),
    # Indonesia — nickel, coal, palm oil
    _Region('indonesia_mining',    -11.0,   6.0, 105.0, 141.0, 'nickel,coal'),
    # Japan — energy import hub, nuclear facilities
    _Region('japan_energy',         30.0,  46.0, 129.0, 146.0, 'energy_importer'),
    # New Zealand — dairy, agriculture
    _Region('new_zealand_agri',    -47.0, -34.0, 165.0, 179.0, 'agriculture'),
    # Turkey — copper, borates, strategic chokepoint (Bosphorus)
    _Region('turkey_bosphorus',     36.0,  42.0,  26.0,  45.0, 'copper,chokepoint'),
    # Iran — oil, gas (Strait of Hormuz proximity)
    _Region('iran_hormuz',          24.0,  40.0,  44.0,  64.0, 'oil,gas'),
    # Pacific Northwest USA — tech supply chain, ports
    _Region('us_pacific_northwest', 42.0,  49.0, -124.0, -116.0, 'ports,tech'),
    # Greece / Aegean — shipping routes
    _Region('aegean_shipping',      35.0,  42.0,  19.0,  30.0, 'shipping'),
    # Taiwan — semiconductors
    _Region('taiwan_semiconductors', 21.5,  25.5, 119.0, 122.5, 'semiconductors'),
    # Philippines — nickel, shipping lanes
    _Region('philippines_nickel',    4.5,  21.0, 116.0, 127.0, 'nickel'),
    # Mexico — silver, oil
    _Region('mexico_mining',        14.0,  32.0, -118.0, -86.0, 'silver,oil'),
    # Kazakhstan / Central Asia — uranium, oil, gas
    _Region('central_asia_energy',  37.0,  56.0,  50.0,  90.0, 'uranium,oil,gas'),
]


def _in_region(lat: float, lon: float, region: _Region) -> bool:
    return (region.min_lat <= lat <= region.max_lat and
            region.min_lon <= lon <= region.max_lon)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


class USGSAdapter(BaseIngestAdapter):
    """
    USGS earthquake feed adapter.

    Fetches M4.5+ earthquakes from the past 24h and emits atoms only for
    events that fall within defined commodity-producing regions.
    No API key required.
    """

    def __init__(self):
        super().__init__(name='usgs_seismic')
        self._seen_ids: set = set()

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        source = 'geopolitical_data_usgs'
        meta_base = {'fetched_at': now_iso, 'source_url': _USGS_FEED}

        try:
            req = urllib.request.Request(
                _USGS_FEED,
                headers={'User-Agent': 'TradingKB/1.0', 'Accept': 'application/json'},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        except Exception as exc:
            self._logger.warning('USGS feed fetch failed: %s', exc)
            return []

        features = data.get('features', [])
        region_max_mag: Dict[str, float] = {}

        for feature in features:
            eq_id = feature.get('id', '')
            if eq_id in self._seen_ids:
                continue

            props = feature.get('properties', {})
            geom  = feature.get('geometry', {})
            coords = geom.get('coordinates', [])
            if len(coords) < 3:
                continue

            lon, lat, depth = float(coords[0]), float(coords[1]), float(coords[2])
            mag = props.get('mag')
            if mag is None or float(mag) < _MIN_MAGNITUDE:
                continue
            mag = float(mag)

            place = props.get('place', 'Unknown')
            eq_time = props.get('time', 0)

            # Check which commodity regions this earthquake affects
            for region in _COMMODITY_REGIONS:
                if _in_region(lat, lon, region):
                    self._seen_ids.add(eq_id)
                    # Track max magnitude per region for seismic_activity roll-up
                    prev_max = region_max_mag.get(region.label, 0.0)
                    if mag > prev_max:
                        region_max_mag[region.label] = mag

                    atoms.append(RawAtom(
                        subject='usgs_risk',
                        predicate=f'{region.label}_earthquake',
                        object=f'M{mag:.1f}_{int(depth)}km',
                        confidence=0.78,
                        source=source,
                        metadata={
                            **meta_base,
                            'magnitude': mag,
                            'depth_km': round(depth, 1),
                            'lat': lat,
                            'lon': lon,
                            'place': place,
                            'commodity': region.commodity_note,
                            'usgs_id': eq_id,
                        },
                    ))
                    break  # one atom per earthquake (first matching region)

        # ── Seismic activity level roll-up per region ─────────────────────────
        for region_label, max_mag in region_max_mag.items():
            level = 'elevated' if max_mag >= _ELEVATED_MAGNITUDE else 'normal'
            atoms.append(RawAtom(
                subject='usgs_risk',
                predicate=f'{region_label}_seismic_activity',
                object=level,
                confidence=0.70,
                source=source,
                metadata={**meta_base, 'max_magnitude_24h': max_mag},
            ))

        # Limit seen_ids memory
        if len(self._seen_ids) > 2000:
            self._seen_ids = set(list(self._seen_ids)[-1000:])

        self._logger.info('USGS adapter: %d atoms from %d earthquakes checked', len(atoms), len(features))
        return atoms
