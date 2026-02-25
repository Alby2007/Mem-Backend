"""
tests/test_overlay_builder.py — Overlay Builder Tests
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from llm.overlay_builder import (
    extract_tickers,
    build_overlay_cards,
    _build_signal_summary_card,
    _build_stress_flag_card,
    _load_kb_subjects,
    _UPPERCASE_STOPWORDS,
)


def _tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT DEFAULT 'test', confidence REAL DEFAULT 0.8,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    return path


def _ins(conn, subject, predicate, obj):
    conn.execute(
        "INSERT INTO facts (subject, predicate, object) VALUES (?, ?, ?)",
        (subject, predicate, obj),
    )
    conn.commit()


def _make_db_with_tickers() -> tuple:
    path = _tmp_db()
    conn = sqlite3.connect(path)
    for pred, val in [
        ('conviction_tier', 'high'),
        ('upside_pct', '25.0'),
        ('invalidation_distance', '-20.0'),
        ('position_size_pct', '4.5'),
        ('signal_quality', 'strong'),
        ('thesis_risk_level', 'moderate'),
        ('macro_confirmation', 'confirmed'),
        ('options_regime', 'normal'),
    ]:
        _ins(conn, 'nvda', pred, val)
    for pred, val in [
        ('conviction_tier', 'medium'),
        ('upside_pct', '12.0'),
        ('signal_quality', 'confirmed'),
    ]:
        _ins(conn, 'aapl', pred, val)
    return path, conn


# ── TestStopwords ─────────────────────────────────────────────────────────────

class TestStopwords:

    def test_rsi_in_stopwords(self):
        assert 'RSI' in _UPPERCASE_STOPWORDS

    def test_gmt_in_stopwords(self):
        assert 'GMT' in _UPPERCASE_STOPWORDS

    def test_etf_in_stopwords(self):
        assert 'ETF' in _UPPERCASE_STOPWORDS

    def test_ceo_in_stopwords(self):
        assert 'CEO' in _UPPERCASE_STOPWORDS

    def test_ipo_in_stopwords(self):
        assert 'IPO' in _UPPERCASE_STOPWORDS

    def test_usd_in_stopwords(self):
        assert 'USD' in _UPPERCASE_STOPWORDS

    def test_ema_in_stopwords(self):
        assert 'EMA' in _UPPERCASE_STOPWORDS

    def test_real_ticker_not_in_stopwords(self):
        # Common tickers should NOT be blocked by stopwords
        assert 'NVDA' not in _UPPERCASE_STOPWORDS
        assert 'AAPL' not in _UPPERCASE_STOPWORDS
        assert 'MSFT' not in _UPPERCASE_STOPWORDS


# ── TestExtractTickers ────────────────────────────────────────────────────────

class TestExtractTickers:

    def test_extracts_known_ticker(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('NVDA Daily Chart', conn)
        conn.close()
        assert 'NVDA' in result

    def test_filters_stopwords(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('RSI 67 — NVDA EMA cross', conn)
        conn.close()
        assert 'RSI' not in result
        assert 'EMA' not in result

    def test_filters_unknown_tickers(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('ZZZZ Unknown ticker', conn)
        conn.close()
        assert 'ZZZZ' not in result

    def test_explicit_entities_bypass_validation(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('', conn, screen_entities=['TSLA'])
        conn.close()
        assert 'TSLA' in result

    def test_dedup_across_screen_and_explicit(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('NVDA chart NVDA', conn, screen_entities=['NVDA'])
        conn.close()
        assert result.count('NVDA') == 1

    def test_empty_context_returns_empty(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('', conn)
        conn.close()
        assert result == []

    def test_multiple_tickers_extracted(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('NVDA vs AAPL comparison', conn)
        conn.close()
        assert 'NVDA' in result
        assert 'AAPL' in result

    def test_gmt_not_extracted_from_screen(self):
        path, conn = _make_db_with_tickers()
        result = extract_tickers('NVDA Price 08:00 GMT', conn)
        conn.close()
        assert 'GMT' not in result


# ── TestBuildSignalSummaryCard ────────────────────────────────────────────────

class TestBuildSignalSummaryCard:

    def _atoms(self, **kwargs):
        base = {
            'conviction_tier': 'high',
            'signal_quality': 'strong',
            'position_size_pct': '4.5',
            'upside_pct': '25.0',
            'invalidation_distance': '-20.0',
            'options_regime': 'normal',
            'thesis_risk_level': 'moderate',
            'macro_confirmation': 'confirmed',
        }
        base.update(kwargs)
        return base

    def test_card_type_is_signal_summary(self):
        card = _build_signal_summary_card('NVDA', self._atoms())
        assert card['type'] == 'signal_summary'

    def test_ticker_uppercased(self):
        card = _build_signal_summary_card('nvda', self._atoms())
        assert card['ticker'] == 'NVDA'

    def test_asymmetry_computed(self):
        card = _build_signal_summary_card('NVDA', self._atoms())
        assert card['asymmetry_ratio'] == pytest.approx(1.25, abs=0.01)

    def test_zero_invalidation_asymmetry_is_none(self):
        atoms = self._atoms(invalidation_distance='0')
        card = _build_signal_summary_card('NVDA', atoms)
        assert card['asymmetry_ratio'] is None

    def test_all_required_fields_present(self):
        card = _build_signal_summary_card('NVDA', self._atoms())
        required = {
            'type', 'ticker', 'conviction_tier', 'signal_quality',
            'position_size_pct', 'upside_pct', 'invalidation_distance',
            'asymmetry_ratio', 'options_regime', 'thesis_risk_level',
            'macro_confirmation',
        }
        assert required == set(card.keys())

    def test_missing_atoms_return_none_fields(self):
        card = _build_signal_summary_card('NVDA', {})
        assert card['conviction_tier'] is None
        assert card['upside_pct'] is None


# ── TestBuildStressFlagCard ───────────────────────────────────────────────────

class TestBuildStressFlagCard:

    def test_low_stress_no_flag(self):
        card = _build_stress_flag_card({'composite_stress': 0.2})
        assert card['flag'] is None
        assert card['type'] == 'stress_flag'

    def test_high_stress_flagged(self):
        card = _build_stress_flag_card({'composite_stress': 0.75})
        assert card['flag'] == 'high_stress'

    def test_none_stress_no_flag(self):
        card = _build_stress_flag_card(None)
        assert card['flag'] is None
        assert card['composite_stress'] == 0.0

    def test_threshold_boundary(self):
        card_below = _build_stress_flag_card({'composite_stress': 0.60})
        card_above = _build_stress_flag_card({'composite_stress': 0.61})
        assert card_below['flag'] is None
        assert card_above['flag'] == 'high_stress'


# ── TestBuildOverlayCards ─────────────────────────────────────────────────────

class TestBuildOverlayCards:

    def test_returns_list(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['NVDA'], conn)
        conn.close()
        assert isinstance(cards, list)

    def test_signal_summary_present_for_known_ticker(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['NVDA'], conn)
        conn.close()
        types = [c['type'] for c in cards]
        assert 'signal_summary' in types

    def test_stress_flag_always_present(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['NVDA'], conn)
        conn.close()
        types = [c['type'] for c in cards]
        assert 'stress_flag' in types

    def test_no_signal_card_for_unknown_ticker(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['ZZZZ'], conn)
        conn.close()
        signal_cards = [c for c in cards if c['type'] == 'signal_summary']
        assert len(signal_cards) == 0

    def test_multiple_tickers_multiple_signal_cards(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['NVDA', 'AAPL'], conn)
        conn.close()
        signal_cards = [c for c in cards if c['type'] == 'signal_summary']
        tickers = {c['ticker'] for c in signal_cards}
        assert 'NVDA' in tickers
        assert 'AAPL' in tickers

    def test_stress_dict_passed_through(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards(['NVDA'], conn, stress_dict={'composite_stress': 0.8})
        conn.close()
        stress_card = next(c for c in cards if c['type'] == 'stress_flag')
        assert stress_card['flag'] == 'high_stress'
        assert stress_card['composite_stress'] == pytest.approx(0.8, abs=0.01)

    def test_empty_tickers_still_returns_stress_flag(self):
        path, conn = _make_db_with_tickers()
        cards = build_overlay_cards([], conn)
        conn.close()
        types = [c['type'] for c in cards]
        assert 'stress_flag' in types
