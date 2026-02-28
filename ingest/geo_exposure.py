"""
ingest/geo_exposure.py — Ticker-to-Geopolitical-Region Exposure Config

Defines which geographic regions each tracked ticker has revenue/supply
exposure to. Used by signal_enrichment_adapter.py to derive the
`geopolitical_risk_exposure` atom per ticker when GDELT/UCDP/ACLED
atoms indicate elevated regional risk.

REGIONS must match the region keys used in gdelt_adapter.py region roll-ups
and the country keys used in ucdp_adapter.py / acled_adapter.py.

Maintenance: Add tickers as the universe expands. Keep this file as the
single source of truth for geo-exposure — do not hardcode in adapters.
"""

from __future__ import annotations

from typing import Dict, List

# ── Region definitions (must match gdelt_adapter._PAIRS region tags) ──────────
# Regions used: europe_east, asia_east, middle_east, latam, africa,
#               asia_south, north_america, europe, global_defence

# ── Ticker → list of exposed regions ──────────────────────────────────────────
TICKER_GEO_EXPOSURE: Dict[str, List[str]] = {

    # ── Energy: Middle East / Russia primary ─────────────────────────────────
    'shel.l':   ['middle_east', 'europe_east', 'africa'],       # Shell (Nigeria, Russia, Qatar)
    'bp.l':     ['middle_east', 'europe_east'],                  # BP (Iraq, Azerbaijan)
    'xom':      ['middle_east', 'latam'],                        # ExxonMobil (Guyana, Middle East)
    'cvx':      ['middle_east', 'latam'],                        # Chevron
    'tte':      ['middle_east', 'africa'],                       # TotalEnergies (Nigeria, Libya, Angola)
    'equnor':   ['europe_east', 'middle_east'],                  # Equinor (Norway, Barents)

    # ── Defence: elevated by any geopolitical tension ─────────────────────────
    'ba.l':     ['global_defence'],                              # BAE Systems
    'qq.l':     ['global_defence'],                              # QinetiQ
    'chg.l':    ['global_defence'],                              # Chemring
    'lmt':      ['global_defence'],                              # Lockheed Martin
    'rtx':      ['global_defence'],                              # RTX (Raytheon)
    'noc':      ['global_defence'],                              # Northrop Grumman
    'ba':       ['global_defence'],                              # Boeing (defence segment)
    'ldos':     ['global_defence'],                              # Leidos

    # ── Banks: EM, China, sanctions exposed ──────────────────────────────────
    'hsba.l':   ['asia_east', 'middle_east'],                    # HSBC (HK, China, ME)
    'stan.l':   ['asia_south', 'asia_east', 'africa'],           # Standard Chartered
    'barc.l':   ['europe', 'middle_east'],                       # Barclays
    'gs':       ['asia_east', 'europe_east'],                    # Goldman Sachs
    'jpm':      ['asia_east', 'europe_east'],                    # JPMorgan

    # ── Mining: commodity regions ─────────────────────────────────────────────
    'glen.l':   ['africa', 'europe_east'],                       # Glencore (DRC, Kazakhstan)
    'aav.l':    ['latam'],                                       # Antofagasta (Chile, Peru)
    'bhp':      ['asia_east', 'latam'],                          # BHP (China demand, LatAm supply)
    'rio':      ['asia_east', 'africa'],                         # Rio Tinto (Guinea, Mongolia)
    'aal.l':    ['africa', 'latam'],                             # Anglo American
    'fmc':      ['latam'],                                       # FMC (lithium, LatAm)

    # ── Technology: China revenue / Taiwan supply chain ───────────────────────
    'aapl':     ['asia_east'],                                   # Apple (China revenue + Taiwan chips)
    'nvda':     ['asia_east'],                                   # Nvidia (China revenue + TSMC)
    'amd':      ['asia_east'],                                   # AMD (TSMC fab)
    'qcom':     ['asia_east'],                                   # Qualcomm (China sales)
    'tsm':      ['asia_east'],                                   # TSMC (Taiwan primary risk)
    'asml':     ['asia_east'],                                   # ASML (China export controls)
    'arm':      ['asia_east'],                                   # ARM (China licensing)

    # ── Consumer / Retail: supply chain China exposed ─────────────────────────
    'nke':      ['asia_east', 'asia_south'],                     # Nike (Vietnam, China manufacturing)
    'sbux':     ['asia_east'],                                   # Starbucks (China revenue)
    'mcd':      ['europe_east', 'middle_east'],                  # McDonald's (Russia exit impact)

    # ── Commodities / Agriculture: EM exposed ────────────────────────────────
    'adm':      ['europe_east', 'latam'],                        # ADM (Ukraine grain)
    'bunge':    ['europe_east', 'latam'],                        # Bunge (Ukraine, Brazil)
    'cf':       ['europe_east'],                                 # CF Industries (natgas, Ukraine grain)
    'mos':      ['europe_east', 'latam'],                        # Mosaic (potash, Belarus sanctions)
}

# ── Region → GDELT tension pair keys (for enrichment lookup) ─────────────────
# Maps exposure regions to the gdelt_tension atom predicates that signal risk
REGION_TO_GDELT_PAIRS: Dict[str, List[str]] = {
    'europe_east':   ['us_russia_score', 'russia_ukraine_score'],
    'asia_east':     ['us_china_score', 'china_taiwan_score'],
    'middle_east':   ['us_iran_score'],
    'latam':         ['us_venezuela_score'],
    'africa':        [],  # No GDELT pair currently — use UCDP/ACLED only
    'asia_south':    [],  # No GDELT pair currently
    'europe':        ['us_russia_score'],
    'north_america': [],
    'global_defence': [
        'us_russia_score', 'russia_ukraine_score',
        'us_china_score', 'china_taiwan_score', 'us_iran_score',
    ],
}

# Tension score threshold (0–100) above which risk is 'elevated'
GEO_RISK_ELEVATED_THRESHOLD = 60
GEO_RISK_MODERATE_THRESHOLD = 35
