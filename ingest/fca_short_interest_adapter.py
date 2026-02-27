"""
ingest/fca_short_interest_adapter.py — FCA Short Selling Register Adapter

Fetches the UK FCA short position disclosure register (updated daily) and
produces short interest atoms for the Trading KB.

SOURCE
======
  URL: https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx
  Data: All net short positions >= 0.5% of issued share capital, disclosed
        by fund managers under UK Short Selling Regulation (UK SSR).
  No API key required. Updated by the FCA each trading day.

ATOMS PRODUCED
==============
  {TICKER} | short_interest_pct      | "3.2"   — total % of float sold short
                                                  (sum of all disclosed positions)
  {TICKER} | short_interest_holders  | "4"     — number of distinct short holders
  {TICKER} | short_squeeze_potential | high | moderate | low | minimal
  {TICKER} | short_vs_signal         | tension | aligned | neutral
                                                  (cross-ref with KB signal_direction)

SOURCE PREFIX
=============
  alt_data_fca_shorts  (authority 0.55, half-life 3d)
  Short interest is a lagging indicator — use as context, not signal.

INTERVAL
========
  86400s (24h) — FCA updates daily after market close

NOTES
=====
  - Positions < 0.5% are not publicly disclosed under UK SSR, so the total
    short interest is a floor, not the full picture.
  - ISIN → ticker mapping is built from a curated dict covering FTSE 100/250
    constituents that overlap with the KB watchlist.
  - Issuers with no known ticker mapping are skipped gracefully.
  - Short squeeze potential is classified from (short_pct, avg_daily_volume):
      high     ≥ 8% short, lower liquidity
      moderate ≥ 4% short
      low      ≥ 2% short
      minimal  < 2% short
"""

from __future__ import annotations

import io
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_FCA_XLSX_URL = (
    'https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx'
)
_TIMEOUT  = 30
_SOURCE   = 'alt_data_fca_shorts'

# ── ISIN → LSE ticker mapping ──────────────────────────────────────────────────
# Covers FTSE 100/250 names that overlap with the KB watchlist.
# Add more as needed. Source: LSE / Refinitiv ISIN lookup.
_ISIN_TO_TICKER: Dict[str, str] = {
    # FTSE 100
    'GB0031348658': 'HSBA.L',   # HSBC
    'GB0000595859': 'LLOY.L',   # Lloyds Banking Group
    'GB0007980591': 'BP.L',     # BP
    'GB00B03MLX29': 'RDSA.L',   # Shell (legacy)
    'GB00BP6MXD84': 'SHEL.L',   # Shell (current)
    'GB0004544929': 'AZN.L',    # AstraZeneca
    'GB0009252882': 'GSK.L',    # GSK
    'GB0005405286': 'ULVR.L',   # Unilever
    'GB00B8C3BL03': 'VOD.L',    # Vodafone
    'GB00BH4HKS39': 'BARC.L',   # Barclays
    'GB0008706128': 'NWG.L',    # NatWest
    'GB00B7T77214': 'STAN.L',   # Standard Chartered
    'GB0004082847': 'LGEN.L',   # Legal & General
    'GB00BDB6Q211': 'PHNX.L',   # Phoenix Group
    'GB00BP9MXK08': 'LSEG.L',   # London Stock Exchange Group
    'GB0002634946': 'BA.L',     # BAE Systems
    'GB00B1YW4409': 'RR.L',     # Rolls-Royce
    'GB00B24CGK77': 'IAG.L',    # IAG (BA parent)
    'GB0006834356': 'REL.L',    # RELX
    'GB00B3GN4412': 'WPP.L',    # WPP
    'GB00BMJJJF91': 'AUTO.L',   # Autotrader
    'GB0031274896': 'MNG.L',    # M&G
    'GB00B5ZN1N88': 'NG.L',     # National Grid
    'GB00BH0P3Z91': 'SSE.L',    # SSE
    'GB00BD6K4575': 'SGE.L',    # Sage Group
    'GB00BLD4ZL61': 'DXCM.L',   # DexCom (LSE)
    'GB00B1FZS350': 'OCDO.L',   # Ocado
    'GB00B02J6398': 'GRG.L',    # Greggs
    'GB00B63QSB39': 'WIZZ.L',   # Wizz Air
    'GB00BYW0PQ60': 'FUTR.L',   # Future PLC
    'GB00BVGBWW93': 'WH.L',     # WH Smith
    'GB0009697037': 'LAND.L',   # Land Securities
    'GB00B02J6398': 'GRG.L',    # Greggs
    'GB00BNKGZC51': 'IHG.L',    # IHG Hotels
    'GB00BLP5YB54': 'EXPN.L',   # Experian
    'GB00BJ5JH161': 'FERG.L',   # Ferguson
    'GB00B24CGK77': 'IAG.L',    # IAG
    'GB00BFXZC448': 'DCC.L',    # DCC
    'GB00BVFNZH21': 'IBST.L',   # Ibstock
    'GB0031437502': 'TSCO.L',   # Tesco
    'GB0006731235': 'MKS.L',    # M&S
    'GB0009895292': 'AAL.L',    # Anglo American
    'GB0001426021': 'ANTO.L',   # Antofagasta
    'GB00B10RZP78': 'GLEN.L',   # Glencore
    'GB0004588357': 'BHP.L',    # BHP
    'GB00BH0P3Z91': 'SSE.L',    # SSE
    'GB00B1FZS350': 'OCDO.L',   # Ocado
    'GB00BJVNSS43': 'ADM.L',    # Admiral Group
    'GB00B41H7133': 'BATS.L',   # BAT
    'GB0008762899': 'IMB.L',    # Imperial Brands
    'GB00B4BNMY34': 'RDSB.L',   # Shell B
    'GB00B3FLWH99': 'PSON.L',   # Pearson
    'GB00B2PDGW16': 'ABF.L',    # Associated British Foods
    'GB0034060557': 'AHT.L',    # Ashtead
    'GB00B63H8491': 'CNA.L',    # Centrica
    'GB00B8C3BL03': 'VOD.L',    # Vodafone
    # Mid-cap additions
    'GB00BF8Q6K64': 'ABDN.L',   # abrdn
    'GB00B3KJDQ49': 'AJB.L',    # AJ Bell
    'GB0002875804': 'ASH.L',    # Ashmore Group
    'GB00BGJYPP46': 'PSN.L',    # Persimmon
    'GB0006710230': 'TW.L',     # Taylor Wimpey
    'GB0002168080': 'BWY.L',    # Bellway
    'GB0033776197': 'BKG.L',    # Berkeley Group
    'GB00B1YW4409': 'RR.L',     # Rolls-Royce (duplicate key removed at parse)
}

# Issuer name fragments → ticker (fallback when ISIN not in map)
_NAME_TO_TICKER: Dict[str, str] = {
    'hsbc':           'HSBA.L',
    'lloyds':         'LLOY.L',
    'barclays':       'BARC.L',
    'natwest':        'NWG.L',
    'standard chart': 'STAN.L',
    'bp plc':         'BP.L',
    'shell':          'SHEL.L',
    'astrazeneca':    'AZN.L',
    'gsk':            'GSK.L',
    'unilever':       'ULVR.L',
    'vodafone':       'VOD.L',
    'bt group':       'BT.A.L',
    'rolls-royce':    'RR.L',
    'bae systems':    'BA.L',
    'ocado':          'OCDO.L',
    'greggs':         'GRG.L',
    'wizz air':       'WIZZ.L',
    'future plc':     'FUTR.L',
    'wh smith':       'WH.L',
    'wpp':            'WPP.L',
    'land securities':'LAND.L',
    'ibstock':        'IBST.L',
    'dcc plc':        'DCC.L',
    'autotrader':     'AUTO.L',
    'auto trader':    'AUTO.L',
    'tesco':          'TSCO.L',
    'marks & spencer':'MKS.L',
    'anglo american': 'AAL.L',
    'antofagasta':    'ANTO.L',
    'glencore':       'GLEN.L',
    'bhp':            'BHP.L',
    'admiral':        'ADM.L',
    'experian':       'EXPN.L',
    'segro':          'SGRO.L',
    'rightmove':      'RMV.L',
    'persimmon':      'PSN.L',
    'taylor wimpey':  'TW.L',
    'bellway':        'BWY.L',
    'berkeley':       'BKG.L',
    'legal & general':'LGEN.L',
    'phoenix group':  'PHNX.L',
    'lseg':           'LSEG.L',
    'abdn':           'ABDN.L',
    'abrdn':          'ABDN.L',
    'imperial brands':'IMB.L',
    'british american tobacco': 'BATS.L',
    'national grid':  'NG.L',
    'centrica':       'CNA.L',
    'sage group':     'SGE.L',
    'relx':           'REL.L',
    'associated british foods': 'ABF.L',
    'pearson':        'PSON.L',
    'ashtead':        'AHT.L',
    'ihg':            'IHG.L',
    'ferguson':       'FERG.L',
}


def _resolve_ticker(isin: str, issuer_name: str) -> Optional[str]:
    """Try ISIN lookup first, then issuer name fragment matching."""
    ticker = _ISIN_TO_TICKER.get(isin)
    if ticker:
        return ticker
    name_lower = issuer_name.lower()
    for fragment, t in _NAME_TO_TICKER.items():
        if fragment in name_lower:
            return t
    return None


def _classify_squeeze(short_pct: float) -> str:
    """Classify short squeeze potential from aggregate disclosed short %."""
    if short_pct >= 8.0:
        return 'high'
    if short_pct >= 4.0:
        return 'moderate'
    if short_pct >= 2.0:
        return 'low'
    return 'minimal'


def _cross_ref_signal(ticker: str, db_path: str) -> Optional[str]:
    """
    Read signal_direction from KB for this ticker.
    Returns 'tension' if signal is bullish vs high shorts,
    'aligned' if bearish and high shorts, 'neutral' otherwise.
    """
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='signal_direction' LIMIT 1",
            (ticker.lower(),),
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _cross_ref_tension(signal_dir: Optional[str], short_pct: float) -> str:
    """
    Characterise the tension between KB signal and short interest.
    - tension:  KB is bullish but stock is heavily shorted (contrarian setup)
    - aligned:  KB is bearish and stock is heavily shorted (confirms view)
    - neutral:  no clear signal or low short interest
    """
    if short_pct < 2.0 or not signal_dir:
        return 'neutral'
    if signal_dir in ('bullish', 'long'):
        return 'tension'
    if signal_dir in ('bearish', 'short'):
        return 'aligned'
    return 'neutral'


# ── Adapter ───────────────────────────────────────────────────────────────────

class FCAShortInterestAdapter(BaseIngestAdapter):
    """
    FCA Short Selling Register ingest adapter.

    Downloads the daily FCA XLSX disclosure file, aggregates short positions
    by issuer, resolves tickers via ISIN/name mapping, and writes:
      - short_interest_pct       — total disclosed short % of float
      - short_interest_holders   — count of distinct position holders
      - short_squeeze_potential  — derived classification
      - short_vs_signal          — tension/aligned/neutral vs KB signal

    Only covers positions >= 0.5% (the public disclosure threshold).
    Positions below this are not publicly available under UK SSR.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='fca_short_interest')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        now_iso  = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        # ── Download XLSX ────────────────────────────────────────────────────
        try:
            import pandas as pd
            resp = requests.get(
                _FCA_XLSX_URL, timeout=_TIMEOUT,
                headers={'User-Agent': 'TradingGalaxyKB/1.0'},
            )
            resp.raise_for_status()
        except Exception as e:
            _logger.error('FCA short interest download failed: %s', e)
            return []

        # ── Parse current disclosures sheet ─────────────────────────────────
        try:
            all_sheets = pd.read_excel(io.BytesIO(resp.content), sheet_name=None)
            # The active (current) sheet name contains "Current Disclosures"
            current_sheet = next(
                (v for k, v in all_sheets.items() if 'Current' in k),
                None,
            )
            if current_sheet is None:
                _logger.error('FCA XLSX: no "Current Disclosures" sheet found')
                return []
        except Exception as e:
            _logger.error('FCA XLSX parse failed: %s', e)
            return []

        df = current_sheet.copy()
        df.columns = ['holder', 'issuer', 'isin', 'short_pct', 'position_date']

        # Drop rows with missing critical fields
        df = df.dropna(subset=['isin', 'issuer', 'short_pct'])
        df['short_pct'] = pd.to_numeric(df['short_pct'], errors='coerce')
        df = df.dropna(subset=['short_pct'])

        _logger.info('FCA: %d current disclosures loaded', len(df))

        # ── Aggregate by issuer ─────────────────────────────────────────────
        # Sum all disclosed short positions per issuer (multiple holders)
        agg = df.groupby(['isin', 'issuer']).agg(
            total_short_pct=('short_pct', 'sum'),
            holder_count=('holder', 'nunique'),
            latest_date=('position_date', 'max'),
        ).reset_index()

        resolved = 0
        skipped  = 0

        for _, row in agg.iterrows():
            isin         = str(row['isin']).strip()
            issuer       = str(row['issuer']).strip()
            total_pct    = float(row['total_short_pct'])
            holder_count = int(row['holder_count'])
            latest_date  = str(row['latest_date'])[:10]

            ticker = _resolve_ticker(isin, issuer)
            if not ticker:
                skipped += 1
                _logger.debug('FCA: no ticker for ISIN=%s issuer=%r', isin, issuer)
                continue

            source = f'{_SOURCE}_{ticker.lower().replace(".", "_")}'
            meta   = {
                'fetched_at':    now_iso,
                'isin':          isin,
                'issuer':        issuer,
                'position_date': latest_date,
                'holder_count':  holder_count,
            }

            # Short interest %
            atoms.append(RawAtom(
                subject=ticker, predicate='short_interest_pct',
                object=f'{total_pct:.1f}',
                confidence=0.85, source=source,
                metadata=meta, upsert=True,
            ))

            # Holder count
            atoms.append(RawAtom(
                subject=ticker, predicate='short_interest_holders',
                object=str(holder_count),
                confidence=0.90, source=source,
                metadata=meta, upsert=True,
            ))

            # Squeeze potential
            squeeze = _classify_squeeze(total_pct)
            atoms.append(RawAtom(
                subject=ticker, predicate='short_squeeze_potential',
                object=squeeze,
                confidence=0.70, source=source,
                metadata={**meta, 'short_pct': total_pct},
                upsert=True,
            ))

            # Cross-reference with KB signal direction
            signal_dir = _cross_ref_signal(ticker, self._db_path)
            tension    = _cross_ref_tension(signal_dir, total_pct)
            atoms.append(RawAtom(
                subject=ticker, predicate='short_vs_signal',
                object=tension,
                confidence=0.65, source=source,
                metadata={**meta, 'kb_signal': signal_dir or 'unknown', 'short_pct': total_pct},
                upsert=True,
            ))

            resolved += 1
            _logger.debug(
                'FCA: %s short=%.1f%% holders=%d squeeze=%s tension=%s',
                ticker, total_pct, holder_count, squeeze, tension,
            )

        _logger.info(
            'FCA short interest: %d issuers resolved, %d skipped (no ticker), %d atoms',
            resolved, skipped, len(atoms),
        )
        return atoms
