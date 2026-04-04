"""
analytics/regime_history.py — Historical Market Regime Classification

Classifies each calendar month in the historical record into one of four
macro regimes using cross-asset proxy data from yfinance, then writes
regime-conditional performance atoms to the KB.

REGIMES
=======
  risk_on_expansion    SPY up + credit tight + rates not collapsing
  risk_off_contraction SPY down + credit selling off
  stagflation          SPY flat/down + commodities (GLD) bidding
  recovery             SPY up + bonds rallying (rates falling)

ATOMS PRODUCED
==============
  global_macro_regime | regime_history_YYYY_MM | risk_on_expansion
  {TICKER}            | return_in_{regime}     | "+3.2"  (monthly avg % return in that regime)
  {TICKER}            | regime_hit_rate_{regime}| "72.3"  (% months the ticker was up in regime)
  {TICKER}            | best_regime            | risk_on_expansion
  {TICKER}            | worst_regime           | risk_off_contraction

These atoms let the LLM answer: "How does HSBA.L typically behave in a
risk-off contraction?" with real historical numbers rather than generic statements.

They also feed regime-conditional tip formatting: when the current regime is
risk_off_contraction, the tip formatter can surface the regime-specific pattern
hit rate from signal_calibration rather than the blended average.

SOURCE
======
  macro_data_regime_history (authority 0.80, half-life 30d)
  Regime labels are derived from proxy price data — not directly observed,
  hence authority 0.80 (macro_data tier) rather than 1.0.

USAGE
=====
  python -m analytics.regime_history

  from analytics.regime_history import RegimeHistoryClassifier
  clf = RegimeHistoryClassifier(db_path='trading_knowledge.db')
  clf.run(lookback_years=5)

  POST /calibrate/regime-history
  POST /calibrate/regime-history  {"lookback_years": 5, "tickers": ["HSBA.L"]}
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False
    yf = pd = np = None  # type: ignore

# ── Proxy tickers ──────────────────────────────────────────────────────────────
_SPY  = 'SPY'     # broad US equity — global risk appetite proxy
_HYG  = 'HYG'     # high-yield credit — tight spreads = risk-on
_TLT  = 'TLT'     # long duration treasuries — up = rates falling = risk-off/recovery
_GLD  = 'GLD'     # gold — up = inflation/stagflation hedge
_VIX  = '^VIX'    # volatility index — up = fear

_PROXY_TICKERS = [_SPY, _HYG, _TLT, _GLD, _VIX]

# Source authority prefix
_SOURCE = 'macro_data_regime_history'


# ── Monthly return helper ──────────────────────────────────────────────────────

def _monthly_returns(series: 'pd.Series') -> 'pd.Series':
    """
    Resample daily close to month-end and compute monthly % returns.
    Returns a Series indexed by period (YYYY-MM strings).
    """
    monthly = series.resample('ME').last()
    pct     = monthly.pct_change() * 100.0
    pct.index = pct.index.strftime('%Y-%m')
    return pct.dropna()


# ── Regime classifier ──────────────────────────────────────────────────────────

def _classify_month(
    month:      str,
    spy_ret:    Optional[float],
    hyg_ret:    Optional[float],
    tlt_ret:    Optional[float],
    gld_ret:    Optional[float],
    vix_ret:    Optional[float],
) -> str:
    """
    Classify a calendar month into a macro regime label.

    DECISION MATRIX (priority order — first match wins):
    ─────────────────────────────────────────────────────
    RISK_OFF_CONTRACTION:
      SPY return < -3%  AND  (HYG < -1% OR VIX > +10%)
      "Equities falling with credit stress or volatility spike"

    STAGFLATION:
      SPY return < 1%  AND  GLD return > +2%
      "Gold bidding without equity support — inflation / stag regime"

    RECOVERY:
      SPY return > +2%  AND  TLT return > +1%
      "Equities up while rates rally — early easing / Fed pivot"

    RISK_ON_EXPANSION:
      SPY return > +1%
      "Normal equity up-trend — expansion baseline"

    NO_DATA:
      SPY data missing
    """
    if spy_ret is None:
        return 'no_data'

    hyg_weak   = hyg_ret is not None and hyg_ret < -1.0
    vix_spike  = vix_ret is not None and vix_ret > 10.0
    gld_bid    = gld_ret is not None and gld_ret >  2.0
    tlt_bid    = tlt_ret is not None and tlt_ret >  1.0

    if spy_ret < -3.0 and (hyg_weak or vix_spike):
        return 'risk_off_contraction'
    if spy_ret < 1.0 and gld_bid:
        return 'stagflation'
    if spy_ret > 2.0 and tlt_bid:
        return 'recovery'
    if spy_ret > 1.0:
        return 'risk_on_expansion'
    return 'no_data'


# ── Main classifier ────────────────────────────────────────────────────────────

class RegimeHistoryClassifier:
    """
    Downloads cross-asset proxy data, classifies each month's regime,
    then computes regime-conditional performance atoms for each ticker.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        self._db_path = db_path

    def run(
        self,
        tickers:        Optional[List[str]] = None,
        lookback_years: int = 5,
    ) -> Dict[str, int]:
        """
        Run full regime history classification.

        Returns
        -------
        {'regime_months_classified': N, 'atoms_written': M}
        """
        if not HAS_DEPS:
            _logger.error('regime_history: yfinance/pandas/numpy not available')
            return {'regime_months_classified': 0, 'atoms_written': 0}

        if tickers is None:
            try:
                from ingest.dynamic_watchlist import DynamicWatchlistManager
                tickers = DynamicWatchlistManager.get_active_tickers(self._db_path)
            except ImportError:
                try:
                    from ingest.yfinance_adapter import _DEFAULT_TICKERS
                    tickers = list(_DEFAULT_TICKERS)
                except ImportError:
                    tickers = []

        # Filter to equity tickers only (skip FX, indices, proxies)
        _skip = {_SPY, _HYG, _TLT, _GLD, _VIX, 'GBPUSD=X', 'EURGBP=X', '^FTSE', '^FTMC', '^GSPC', '^VIX'}
        equity_tickers = [t for t in tickers if t not in _skip]

        # Build full download list: proxies + equity tickers
        all_tickers = list(_PROXY_TICKERS) + equity_tickers
        period      = f'{lookback_years}y'

        _logger.info('regime_history: downloading %s daily OHLCV for %d tickers',
                     period, len(all_tickers))
        try:
            raw = yf.download(
                tickers     = all_tickers,
                period      = period,
                interval    = '1d',
                group_by    = 'ticker',
                auto_adjust = True,
                progress    = False,
                threads     = True,
            )
        except Exception as e:
            _logger.error('regime_history: download failed: %s', e)
            return {'regime_months_classified': 0, 'atoms_written': 0}

        if raw is None or raw.empty:
            return {'regime_months_classified': 0, 'atoms_written': 0}

        def _close(sym: str) -> Optional['pd.Series']:
            try:
                if len(all_tickers) == 1:
                    s = raw['Close']
                else:
                    s = raw[sym]['Close']
                return s.dropna() if not s.empty else None
            except (KeyError, TypeError):
                return None

        # ── Build monthly return series for proxies ────────────────────────────
        proxy_monthly: Dict[str, 'pd.Series'] = {}
        for sym in _PROXY_TICKERS:
            close = _close(sym)
            if close is not None:
                proxy_monthly[sym] = _monthly_returns(close)

        spy_m = proxy_monthly.get(_SPY)
        if spy_m is None or spy_m.empty:
            _logger.warning('regime_history: SPY data missing — cannot classify regimes')
            return {'regime_months_classified': 0, 'atoms_written': 0}

        # ── Classify each month ────────────────────────────────────────────────
        all_months = spy_m.index.tolist()
        month_regime: Dict[str, str] = {}

        for month in all_months:
            def _get(sym: str) -> Optional[float]:
                s = proxy_monthly.get(sym)
                return float(s[month]) if s is not None and month in s else None

            regime = _classify_month(
                month    = month,
                spy_ret  = _get(_SPY),
                hyg_ret  = _get(_HYG),
                tlt_ret  = _get(_TLT),
                gld_ret  = _get(_GLD),
                vix_ret  = _get(_VIX),
            )
            month_regime[month] = regime

        classified = [m for m, r in month_regime.items() if r != 'no_data']
        _logger.info('regime_history: classified %d months (%d no_data)',
                     len(classified), len(all_months) - len(classified))

        # ── Build atoms ────────────────────────────────────────────────────────
        from ingest.base import RawAtom
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms:  List[RawAtom] = []

        # 1. Global regime history atoms (one per month)
        for month, regime in month_regime.items():
            if regime == 'no_data':
                continue
            atoms.append(RawAtom(
                subject    = 'global_macro_regime',
                predicate  = f'regime_history_{month.replace("-", "_")}',
                object     = regime,
                confidence = 0.80,
                source     = _SOURCE,
                metadata   = {'month': month, 'as_of': now_iso},
                upsert     = True,
            ))

        # 2. Regime-conditional performance per equity ticker
        for ticker in equity_tickers:
            close = _close(ticker)
            if close is None:
                continue
            monthly_rets = _monthly_returns(close)
            if monthly_rets.empty:
                continue

            # Group monthly returns by regime
            regime_returns: Dict[str, List[float]] = defaultdict(list)
            for month, regime in month_regime.items():
                if regime == 'no_data':
                    continue
                if month in monthly_rets:
                    regime_returns[regime].append(float(monthly_rets[month]))

            if not regime_returns:
                continue

            src  = f'{_SOURCE}_{ticker.lower().replace(".", "_").replace("-", "_")}'
            meta = {'as_of': now_iso, 'lookback_years': lookback_years}

            avg_by_regime: Dict[str, float] = {}
            hr_by_regime:  Dict[str, float] = {}

            for regime, rets in regime_returns.items():
                if len(rets) < 2:
                    continue
                avg_ret = round(sum(rets) / len(rets), 2)
                hit_rate = round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
                avg_by_regime[regime] = avg_ret
                hr_by_regime[regime]  = hit_rate
                n = len(rets)

                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = f'return_in_{regime}',
                    object     = str(avg_ret),
                    confidence = min(0.85, 0.60 + n * 0.005),
                    source     = src,
                    metadata   = {**meta, 'regime': regime, 'n_months': n},
                    upsert     = True,
                ))
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = f'regime_hit_rate_{regime}',
                    object     = str(hit_rate),
                    confidence = min(0.80, 0.55 + n * 0.005),
                    source     = src,
                    metadata   = {**meta, 'regime': regime, 'n_months': n},
                    upsert     = True,
                ))

            # Best and worst regime by average monthly return
            if avg_by_regime:
                best  = max(avg_by_regime, key=avg_by_regime.get)
                worst = min(avg_by_regime, key=avg_by_regime.get)
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'best_regime',
                    object     = f'{best} ({avg_by_regime[best]:+.1f}%/mo)',
                    confidence = 0.75,
                    source     = src,
                    metadata   = meta,
                    upsert     = True,
                ))
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'worst_regime',
                    object     = f'{worst} ({avg_by_regime[worst]:+.1f}%/mo)',
                    confidence = 0.75,
                    source     = src,
                    metadata   = meta,
                    upsert     = True,
                ))

        # ── Push atoms to KB ───────────────────────────────────────────────────
        if not atoms:
            _logger.warning('regime_history: no atoms to write')
            return {'regime_months_classified': len(classified), 'atoms_written': 0}

        try:
            from knowledge.graph import TradingKnowledgeGraph
            kg = TradingKnowledgeGraph(self._db_path)
        except ImportError:
            _logger.error('regime_history: TradingKnowledgeGraph not available')
            return {'regime_months_classified': len(classified), 'atoms_written': 0}

        written = 0
        for atom in atoms:
            try:
                ok = kg.add_fact(
                    subject   = atom.subject,
                    predicate = atom.predicate,
                    object    = atom.object,
                    confidence= atom.confidence,
                    source    = atom.source,
                    metadata  = atom.metadata,
                    upsert    = atom.upsert,
                )
                if ok:
                    written += 1
            except Exception as e:
                _logger.debug('regime_history: atom write failed: %s', e)

        _logger.info('regime_history: wrote %d atoms for %d tickers across %d months',
                     written, len(equity_tickers), len(classified))

        return {
            'regime_months_classified': len(classified),
            'atoms_written':            written,
            'tickers_processed':        len(equity_tickers),
        }


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    import argparse
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s %(levelname)-8s %(name)s — %(message)s',
    )
    parser = argparse.ArgumentParser(description='Historical regime classification')
    parser.add_argument('--db',    default='trading_knowledge.db')
    parser.add_argument('--years', type=int, default=5)
    args = parser.parse_args()

    clf    = RegimeHistoryClassifier(db_path=args.db)
    result = clf.run(lookback_years=args.years)
    print(f'\nRegime history complete:')
    for k, v in result.items():
        print(f'  {k}: {v}')

    # Show regime breakdown
    try:
        conn  = sqlite3.connect(args.db, timeout=10)
        rows  = conn.execute(
            "SELECT object, COUNT(*) as n FROM facts "
            "WHERE subject='global_macro_regime' AND predicate LIKE 'regime_history_%' "
            "GROUP BY object ORDER BY n DESC"
        ).fetchall()
        conn.close()
        print('\nRegime distribution:')
        for regime, count in rows:
            bar = '█' * (count // 2)
            print(f'  {regime:25s} {count:3d} months  {bar}')
    except Exception:
        pass


if __name__ == '__main__':
    main()
