"""
ingest/earnings_calendar_adapter.py — Earnings Calendar Enrichment Adapter

Reads next_earnings_date atoms already in the KB (populated by YFinanceAdapter)
and enriches them with:
  - earnings_risk atoms for tickers within 7 days of reporting
  - earnings_implied_move from options pricing (via yfinance chain)
  - Pre-earnings position sizing warnings suppressed/flagged in the KB

ATOMS PRODUCED
==============
  {TICKER} | earnings_date         | YYYY-MM-DD
  {TICKER} | days_to_earnings      | "3"   (integer days)
  {TICKER} | earnings_risk         | elevated | moderate | low
  {TICKER} | earnings_implied_move | "±4.2%"  (from ATM straddle price)
  {TICKER} | pre_earnings_flag     | within_48h | within_7d | clear

SOURCE
======
  source prefix: earnings_calendar_{ticker}  (authority 0.85, half-life 7d)

INTERVAL
========
  Register at 3600s (1 hour) — earnings dates don't change intraday but
  the days_to_earnings countdown needs daily refresh.

DESIGN NOTES
============
  - If options data is unavailable (FTSE thin market) the implied move is
    estimated from the ticker's volatility_30d atom in the KB:
      implied_move_proxy = volatility_30d × sqrt(1/252) × sqrt(days_to_event)
    This is a rough approximation but better than no estimate.
  - Tickers within 48h of earnings get pre_earnings_flag=within_48h.
    The position calculator (prompt_builder.py) checks this flag to warn the LLM.
  - Tickers within 7 days get pre_earnings_flag=within_7d.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom, db_connect

_logger = logging.getLogger(__name__)

_SOURCE_PREFIX = 'earnings_calendar'
_WITHIN_48H_DAYS = 2
_WITHIN_7D_DAYS  = 7

# Straddle price as % of stock price → implied move approximation
# Only used when live options chain is unavailable
_VOL_SCALING_DAYS = 1  # 1 trading day earnings window


def _days_until(target_date_str: str) -> Optional[int]:
    """Parse YYYY-MM-DD and return calendar days from today. None if unparseable."""
    try:
        target = date.fromisoformat(target_date_str[:10])
        delta = (target - date.today()).days
        return delta
    except (ValueError, TypeError):
        return None


def _implied_move_from_options(ticker: str) -> Optional[str]:
    """
    Attempt to compute implied move from the ATM straddle price.
    Returns e.g. '±4.2%' or None if unavailable.
    """
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        exps = tk.options
        if not exps:
            return None

        # Use nearest expiry
        chain = tk.option_chain(exps[0])
        spot = tk.fast_info.get('lastPrice') or tk.fast_info.get('regularMarketPrice')
        if not spot or spot <= 0:
            return None

        # Find ATM call and put
        calls = chain.calls
        puts  = chain.puts
        if calls.empty or puts.empty:
            return None

        calls = calls.assign(_dist=(calls['strike'] - spot).abs())
        puts  = puts.assign(_dist=(puts['strike'] - spot).abs())

        atm_call = calls.nsmallest(1, '_dist')
        atm_put  = puts.nsmallest(1, '_dist')

        call_mid = (atm_call['bid'].iloc[0] + atm_call['ask'].iloc[0]) / 2
        put_mid  = (atm_put['bid'].iloc[0]  + atm_put['ask'].iloc[0])  / 2
        straddle_cost = call_mid + put_mid

        if straddle_cost <= 0 or spot <= 0:
            return None

        move_pct = round((straddle_cost / spot) * 100, 1)
        return f'±{move_pct}%'

    except Exception as e:
        _logger.debug('Options implied move unavailable for %s: %s', ticker, e)
        return None


def _implied_move_from_vol(vol_30d_str: Optional[str]) -> Optional[str]:
    """
    Estimate implied move from realised 30d volatility as a fallback.
    vol_30d is annualised %, stored as a plain string like '32.5'.
    Approximation: daily_vol × sqrt(2) to account for earnings jump window.
    """
    if not vol_30d_str:
        return None
    try:
        vol_ann = float(vol_30d_str)
        daily_vol = vol_ann / (252 ** 0.5)
        implied = round(daily_vol * (2 ** 0.5), 1)
        return f'±{implied}%'
    except (ValueError, TypeError):
        return None


class EarningsCalendarAdapter(BaseIngestAdapter):
    """
    Earnings calendar enrichment adapter.

    Reads next_earnings_date atoms from the KB and produces earnings_risk,
    earnings_implied_move, and pre_earnings_flag atoms for tickers approaching
    their earnings date.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='earnings_calendar')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        # ── Read all next_earnings_date atoms from KB ────────────────────────
        upcoming = self._load_upcoming_earnings()
        if not upcoming:
            _logger.info('earnings_calendar: no earnings date atoms found in KB')
            return []

        _logger.info('earnings_calendar: checking %d tickers', len(upcoming))

        for ticker, earnings_date_str, vol_30d in upcoming:
            days = _days_until(earnings_date_str)
            if days is None:
                continue

            source = f'{_SOURCE_PREFIX}_{ticker.lower()}'
            meta = {'fetched_at': now_iso, 'earnings_date': earnings_date_str, 'days_to_earnings': days}

            # Normalise date to ISO
            atoms.append(RawAtom(
                subject=ticker, predicate='earnings_date',
                object=earnings_date_str[:10],
                confidence=0.90, source=source,
                metadata=meta, upsert=True,
            ))

            # Only enrich tickers within 7 days
            if days < 0 or days > _WITHIN_7D_DAYS:
                atoms.append(RawAtom(
                    subject=ticker, predicate='pre_earnings_flag',
                    object='clear',
                    confidence=0.90, source=source,
                    metadata=meta, upsert=True,
                ))
                continue

            # Days to earnings countdown
            atoms.append(RawAtom(
                subject=ticker, predicate='days_to_earnings',
                object=str(days),
                confidence=0.95, source=source,
                metadata=meta, upsert=True,
            ))

            # Earnings risk classification
            if days <= _WITHIN_48H_DAYS:
                risk = 'elevated'
                flag = 'within_48h'
            else:
                risk = 'moderate'
                flag = 'within_7d'

            atoms.append(RawAtom(
                subject=ticker, predicate='earnings_risk',
                object=risk,
                confidence=0.85, source=source,
                metadata=meta, upsert=True,
            ))
            atoms.append(RawAtom(
                subject=ticker, predicate='pre_earnings_flag',
                object=flag,
                confidence=0.90, source=source,
                metadata=meta, upsert=True,
            ))

            # Implied move — try options first, fall back to vol estimate
            implied_options = _implied_move_from_options(ticker)
            implied = implied_options if implied_options is not None else _implied_move_from_vol(vol_30d)

            if implied:
                atoms.append(RawAtom(
                    subject=ticker, predicate='earnings_implied_move',
                    object=implied,
                    confidence=0.75 if implied_options else 0.50,
                    source=source,
                    metadata={**meta, 'method': 'options_straddle' if implied_options else 'vol_proxy'},
                    upsert=True,
                ))

            _logger.info(
                'earnings_calendar: %s in %d days — risk=%s implied=%s',
                ticker, days, risk, implied or 'n/a',
            )

        _logger.info('earnings_calendar: produced %d atoms', len(atoms))
        return atoms

    def _load_upcoming_earnings(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Read next_earnings_date atoms from the KB.
        Also reads volatility_30d for the same ticker (fallback implied move).
        Returns list of (ticker, earnings_date_str, vol_30d_str_or_None).
        """
        try:
            conn = db_connect(self._db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT subject, object FROM facts
                WHERE predicate = 'next_earnings_date'
                  AND object NOT IN ('n/a', '', 'none', 'unknown')
                """
            ).fetchall()

            # Build vol lookup
            vol_rows = conn.execute(
                "SELECT subject, object FROM facts WHERE predicate = 'volatility_30d'"
            ).fetchall()
            vol_map: Dict[str, str] = {r['subject']: r['object'] for r in vol_rows}

            conn.close()

            result = []
            for row in rows:
                ticker = row['subject'].upper()
                result.append((ticker, row['object'], vol_map.get(row['subject'].lower())))
            return result

        except Exception as e:
            _logger.error('earnings_calendar: failed to load KB atoms: %s', e)
            return []
