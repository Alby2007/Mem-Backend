"""
ingest/technical_adapter.py — Technical Indicator Adapter

Computes BB Squeeze, ATR-14, SMA Alignment, MACD Signal, and Volume POC
from ohlcv_cache daily candles and writes them as KB atoms (facts).

Runs every 600s (registered in api_v2.py).  All indicators are per-ticker
and stored as predicates in the facts table (already on PG).

Signal families produced:
  - bb_squeeze        : 'firing' | 'building' | 'neutral'
  - atr_14            : numeric (dollar value)
  - atr_regime        : 'expanding' | 'contracting' | 'stable'
  - sma_alignment     : 'bullish_stack' | 'bearish_stack' | 'mixed'
  - macd_signal       : 'bullish_cross' | 'bearish_cross' | 'neutral'
  - macd_histogram    : numeric
  - volume_poc_zone   : 'above' | 'below' | 'at_poc'  (price vs 20d VWAP proxy)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

try:
    from db import HAS_POSTGRES, get_pg
except ImportError:
    HAS_POSTGRES = False
    get_pg = None  # type: ignore


class TechnicalAdapter(BaseIngestAdapter):
    """Compute technical indicators from ohlcv_cache daily candles."""

    name = "technical_indicators"

    def __init__(self, db_path: str):
        super().__init__(self.name)
        self._db_path = db_path

    # ── fetch ─────────────────────────────────────────────────────────────────

    def fetch(self) -> List[RawAtom]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()
        rows = self._get_ohlcv(cutoff)
        if not rows:
            _logger.debug("technical_adapter: no OHLCV rows")
            return []

        # Group by ticker
        ticker_candles: Dict[str, List[Tuple[float, float, float, float, float]]] = {}
        for ticker, o, h, l, c, v in rows:
            t = ticker.upper()
            if t not in ticker_candles:
                ticker_candles[t] = []
            ticker_candles[t].append((float(o), float(h), float(l), float(c), float(v or 0)))

        now_iso = datetime.now(timezone.utc).isoformat()
        src = "technical_adapter"
        atoms: List[RawAtom] = []

        for ticker, candles in ticker_candles.items():
            if len(candles) < 26:
                continue
            closes = [c[3] for c in candles]
            highs  = [c[1] for c in candles]
            lows   = [c[2] for c in candles]
            volumes = [c[4] for c in candles]

            # ── ATR-14 ────────────────────────────────────────────────────
            atr = self._atr(highs, lows, closes, 14)
            if atr is not None:
                atoms.append(RawAtom(
                    subject=ticker, predicate='atr_14',
                    object=str(round(atr, 4)), confidence=0.95,
                    source=src, metadata={'as_of': now_iso}, upsert=True,
                ))
                # ATR regime: compare current ATR to 20-period ATR mean
                atr_20 = self._atr(highs, lows, closes, 20)
                if atr_20 and atr_20 > 0:
                    ratio = atr / atr_20
                    regime = 'expanding' if ratio > 1.15 else ('contracting' if ratio < 0.85 else 'stable')
                    atoms.append(RawAtom(
                        subject=ticker, predicate='atr_regime',
                        object=regime, confidence=0.90,
                        source=src, metadata={'ratio': round(ratio, 3), 'as_of': now_iso},
                        upsert=True,
                    ))

            # ── BB Squeeze ────────────────────────────────────────────────
            squeeze = self._bb_squeeze(closes, highs, lows)
            if squeeze:
                atoms.append(RawAtom(
                    subject=ticker, predicate='bb_squeeze',
                    object=squeeze, confidence=0.90,
                    source=src, metadata={'as_of': now_iso}, upsert=True,
                ))

            # ── SMA Alignment ─────────────────────────────────────────────
            alignment = self._sma_alignment(closes)
            if alignment:
                atoms.append(RawAtom(
                    subject=ticker, predicate='sma_alignment',
                    object=alignment, confidence=0.90,
                    source=src, metadata={'as_of': now_iso}, upsert=True,
                ))

            # ── MACD Signal ───────────────────────────────────────────────
            macd_sig, macd_hist = self._macd(closes)
            if macd_sig:
                atoms.append(RawAtom(
                    subject=ticker, predicate='macd_signal',
                    object=macd_sig, confidence=0.90,
                    source=src, metadata={'histogram': macd_hist, 'as_of': now_iso},
                    upsert=True,
                ))
                if macd_hist is not None:
                    atoms.append(RawAtom(
                        subject=ticker, predicate='macd_histogram',
                        object=str(round(macd_hist, 6)), confidence=0.95,
                        source=src, metadata={'as_of': now_iso}, upsert=True,
                    ))

            # ── Volume POC (price vs 20d VWAP proxy) ─────────────────────
            poc = self._volume_poc(closes, volumes)
            if poc:
                atoms.append(RawAtom(
                    subject=ticker, predicate='volume_poc_zone',
                    object=poc, confidence=0.85,
                    source=src, metadata={'as_of': now_iso}, upsert=True,
                ))

        _logger.info("technical_adapter: produced %d atoms for %d tickers",
                      len(atoms), len(ticker_candles))
        return atoms

    def transform(self, raw):
        return raw if isinstance(raw, list) else []

    # ── OHLCV fetch (PG-first) ────────────────────────────────────────────────

    def _get_ohlcv(self, cutoff: str):
        rows = []
        if HAS_POSTGRES and get_pg:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute(
                        "SELECT ticker, open, high, low, close, volume FROM ohlcv_cache "
                        "WHERE interval='1d' AND ts >= %s AND close IS NOT NULL "
                        "ORDER BY ticker, ts ASC", (cutoff,))
                    rows = [(r['ticker'], r['open'], r['high'], r['low'],
                             r['close'], r['volume']) for r in cur.fetchall()]
            except Exception as e:
                _logger.warning("technical_adapter: PG query failed: %s", e)
                rows = []
        if not rows:
            try:
                conn = sqlite3.connect(self._db_path, timeout=15)
                conn.execute('PRAGMA journal_mode=WAL')
                rows = conn.execute(
                    """SELECT ticker, open, high, low, close, volume
                       FROM ohlcv_cache
                       WHERE interval='1d' AND ts >= ? AND close IS NOT NULL
                       ORDER BY ticker, ts ASC""",
                    (cutoff,),
                ).fetchall()
                conn.close()
            except Exception as e:
                _logger.warning("technical_adapter: SQLite query failed: %s", e)
        return rows

    # ── Indicator computations ────────────────────────────────────────────────

    @staticmethod
    def _ema(values: List[float], period: int) -> List[float]:
        """Exponential moving average."""
        if len(values) < period:
            return []
        k = 2.0 / (period + 1)
        ema = [sum(values[:period]) / period]
        for v in values[period:]:
            ema.append(v * k + ema[-1] * (1 - k))
        return ema

    @staticmethod
    def _sma(values: List[float], period: int) -> Optional[float]:
        """Simple moving average of last `period` values."""
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    @staticmethod
    def _atr(highs: List[float], lows: List[float], closes: List[float],
             period: int) -> Optional[float]:
        """Average True Range."""
        if len(closes) < period + 1:
            return None
        trs = []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
            trs.append(tr)
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period

    @staticmethod
    def _bb_squeeze(
        closes: List[float],
        highs: List[float],
        lows: List[float],
        bb_period: int = 20,
        bb_std: float = 2.0,
        kc_period: int = 20,
        kc_mult: float = 1.5,
    ) -> Optional[str]:
        """
        Bollinger Band Squeeze detection using the Keltner Channel method.

        Squeeze is ON  when BB bands are inside KC bands:
            BB_upper - BB_lower  <  KC_upper - KC_lower

        This is instrument-independent (no static price threshold) and
        correctly fires for ~5-15% of tickers at any given time.

        Returns: 'firing' | 'building' | 'neutral' | None
          firing  = squeeze just released (momentum building COMPLETE)
          building = squeeze is currently ON (BB inside KC)
          neutral  = no squeeze
        """
        n = max(bb_period, kc_period) + 1
        if len(closes) < n or len(highs) < n or len(lows) < n:
            return None

        # ── Bollinger Bands ───────────────────────────────────────────────
        bb_c = closes[-bb_period:]
        bb_sma = sum(bb_c) / bb_period
        if bb_sma == 0:
            return None
        bb_std_val = (sum((c - bb_sma) ** 2 for c in bb_c) / bb_period) ** 0.5
        bb_upper = bb_sma + bb_std * bb_std_val
        bb_lower = bb_sma - bb_std * bb_std_val
        bb_width = bb_upper - bb_lower

        # ── Keltner Channel — SMA(close, 20) ± mult × ATR(20) ────────────
        kc_c = closes[-kc_period:]
        kc_sma = sum(kc_c) / kc_period
        # True Range using full series so ATR covers the period
        trs = []
        for i in range(len(closes) - kc_period, len(closes)):
            if i == 0:
                continue
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
            trs.append(tr)
        if not trs:
            return None
        kc_atr = sum(trs) / len(trs)
        kc_width = 2 * kc_mult * kc_atr

        # ── Classify ──────────────────────────────────────────────────────
        squeeze_on = bb_width < kc_width

        # Detect squeeze release: was squeezed one period ago, not now
        # (use slightly wider window to check previous state)
        if not squeeze_on:
            # Check if we were in squeeze last bar (quick re-check with n-1)
            if len(closes) >= n + 1:
                bb_c_prev = closes[-(bb_period + 1):-1]
                if len(bb_c_prev) == bb_period:
                    bb_sma_p = sum(bb_c_prev) / bb_period
                    bb_std_p = (sum((c - bb_sma_p) ** 2 for c in bb_c_prev) / bb_period) ** 0.5
                    bb_width_p = 2 * bb_std * bb_std_p
                    trs_p = []
                    for i in range(len(closes) - kc_period - 1, len(closes) - 1):
                        if i == 0:
                            continue
                        tr = max(highs[i] - lows[i],
                                 abs(highs[i] - closes[i - 1]),
                                 abs(lows[i] - closes[i - 1]))
                        trs_p.append(tr)
                    if trs_p:
                        kc_atr_p = sum(trs_p) / len(trs_p)
                        kc_width_p = 2 * kc_mult * kc_atr_p
                        if bb_width_p < kc_width_p:
                            return 'firing'  # just released
            return 'neutral'

        return 'building'  # squeeze currently ON

    @staticmethod
    def _sma_alignment(closes: List[float]) -> Optional[str]:
        """Check if SMA 10/20/50 are in bullish or bearish stack."""
        if len(closes) < 50:
            return None
        sma10 = sum(closes[-10:]) / 10
        sma20 = sum(closes[-20:]) / 20
        sma50 = sum(closes[-50:]) / 50
        if sma10 > sma20 > sma50:
            return 'bullish_stack'
        elif sma10 < sma20 < sma50:
            return 'bearish_stack'
        return 'mixed'

    def _macd(self, closes: List[float]) -> Tuple[Optional[str], Optional[float]]:
        """MACD (12,26,9) cross detection."""
        if len(closes) < 35:
            return None, None
        ema12 = self._ema(closes, 12)
        ema26 = self._ema(closes, 26)
        if not ema12 or not ema26:
            return None, None
        # Align lengths: ema26 starts later
        offset = len(ema12) - len(ema26)
        macd_line = [ema12[offset + i] - ema26[i] for i in range(len(ema26))]
        if len(macd_line) < 9:
            return None, None
        signal_line = self._ema(macd_line, 9)
        if not signal_line:
            return None, None
        # Compare last 2 histogram values for cross detection
        hist_offset = len(macd_line) - len(signal_line)
        hist_now  = macd_line[-1] - signal_line[-1]
        hist_prev = macd_line[-2] - signal_line[-2] if len(signal_line) >= 2 else hist_now

        if hist_prev <= 0 < hist_now:
            return 'bullish_cross', round(hist_now, 6)
        elif hist_prev >= 0 > hist_now:
            return 'bearish_cross', round(hist_now, 6)
        return 'neutral', round(hist_now, 6)

    @staticmethod
    def _volume_poc(closes: List[float], volumes: List[float],
                    period: int = 20) -> Optional[str]:
        """
        Volume Point of Control proxy: compare current price to 20-day VWAP.
        above/below/at_poc indicates whether price is trading above or below
        the volume-weighted average (proxy for institutional fair value).
        """
        if len(closes) < period or len(volumes) < period:
            return None
        recent_c = closes[-period:]
        recent_v = volumes[-period:]
        total_vol = sum(recent_v)
        if total_vol == 0:
            return None
        vwap = sum(c * v for c, v in zip(recent_c, recent_v)) / total_vol
        current = closes[-1]
        if vwap == 0:
            return None
        pct = (current - vwap) / vwap
        if pct > 0.01:
            return 'above'
        elif pct < -0.01:
            return 'below'
        return 'at_poc'
