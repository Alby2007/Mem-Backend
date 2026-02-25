"""
tests/test_signal_enrichment.py — Unit tests for new signal enrichment functions

Tests:
  - _classify_earnings_proximity: all date-boundary cases
  - _compute_news_sentiment: score thresholds, min-atom guard, confidence weighting
  - Integration: earnings_proximity + news_sentiment atoms appear in
    SignalEnrichmentAdapter.fetch() output when KB contains the right inputs

No live DB or API required — tests use temp SQLite DBs and in-process adapter.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingest.signal_enrichment_adapter import (
    _classify_earnings_proximity,
    _classify_market_regime,
    _compute_news_sentiment,
    _compute_skew_filter_atoms,
    _SENTIMENT_MIN_ATOMS,
    _SENTIMENT_BULLISH_PREDS,
    _SENTIMENT_BEARISH_PREDS,
    _REGIME_RISK_ON_EXPANSION,
    _REGIME_RISK_OFF_CONTRACTION,
    _REGIME_STAGFLATION,
    _REGIME_RECOVERY,
    _REGIME_NO_DATA,
    _SKEW_FILTER_ELEVATED_MULTIPLIER,
    _SKEW_FILTER_SPIKE_MULTIPLIER,
    _SKEW_FILTER_STOP_TIGHTEN_PCT,
    SignalEnrichmentAdapter,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_plus(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _make_facts_db(rows: list) -> str:
    """Return path to a temp DB with a facts table populated from (subj, pred, obj, conf, src) rows."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            confidence REAL DEFAULT 0.5,
            source TEXT DEFAULT 'test',
            timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00'
        )
    """)
    conn.executemany(
        "INSERT INTO facts (subject, predicate, object, confidence, source) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return path


# ── _classify_earnings_proximity ──────────────────────────────────────────────

class TestClassifyEarningsProximity:
    def test_none_returns_none(self):
        assert _classify_earnings_proximity(None) is None

    def test_empty_string_returns_none(self):
        assert _classify_earnings_proximity('') is None

    def test_unparseable_returns_none(self):
        assert _classify_earnings_proximity('not-a-date') is None
        assert _classify_earnings_proximity('2026/04/30') is None

    def test_past_date_is_post_earnings(self):
        past = _today_plus(-1)
        assert _classify_earnings_proximity(past) == 'post_earnings'

    def test_far_past_is_post_earnings(self):
        assert _classify_earnings_proximity('2020-01-01') == 'post_earnings'

    def test_today_is_pre_earnings_3d(self):
        # delta = 0 → <= 3
        assert _classify_earnings_proximity(_today_plus(0)) == 'pre_earnings_3d'

    def test_3_days_out_is_pre_earnings_3d(self):
        assert _classify_earnings_proximity(_today_plus(3)) == 'pre_earnings_3d'

    def test_4_days_out_is_pre_earnings_2w(self):
        assert _classify_earnings_proximity(_today_plus(4)) == 'pre_earnings_2w'

    def test_14_days_out_is_pre_earnings_2w(self):
        assert _classify_earnings_proximity(_today_plus(14)) == 'pre_earnings_2w'

    def test_15_days_out_is_pre_earnings_8w(self):
        assert _classify_earnings_proximity(_today_plus(15)) == 'pre_earnings_8w'

    def test_56_days_out_is_pre_earnings_8w(self):
        assert _classify_earnings_proximity(_today_plus(56)) == 'pre_earnings_8w'

    def test_57_days_out_is_no_catalyst(self):
        assert _classify_earnings_proximity(_today_plus(57)) == 'no_catalyst'

    def test_far_future_is_no_catalyst(self):
        assert _classify_earnings_proximity(_today_plus(180)) == 'no_catalyst'

    def test_strips_whitespace(self):
        past = '  ' + _today_plus(-5) + '  '
        assert _classify_earnings_proximity(past) == 'post_earnings'


# ── _compute_news_sentiment ───────────────────────────────────────────────────

class TestComputeNewsSentiment:
    def setup_method(self):
        self.db_path = None

    def teardown_method(self):
        if self.db_path and os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def _make_db_with_llm(self, rows):
        self.db_path = _make_facts_db(rows)
        return self.db_path

    def test_no_atoms_returns_none(self):
        db = self._make_db_with_llm([])
        assert _compute_news_sentiment('aapl', db) is None

    def test_fewer_than_min_atoms_returns_none(self):
        rows = [
            ('aapl', 'catalyst', 'product_launch', 0.8, 'llm_extracted_aapl'),
            ('aapl', 'catalyst', 'partnership', 0.7, 'llm_extracted_aapl'),
        ]
        # _SENTIMENT_MIN_ATOMS = 3, only 2 rows
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('aapl', db) is None

    def test_exactly_min_atoms_fires(self):
        rows = [
            ('aapl', 'catalyst', 'product_launch', 0.8, 'llm_extracted_aapl'),
            ('aapl', 'catalyst', 'partnership', 0.7, 'llm_extracted_aapl'),
            ('aapl', 'catalyst', 'deal', 0.6, 'llm_extracted_aapl'),
        ]
        db = self._make_db_with_llm(rows)
        result = _compute_news_sentiment('aapl', db)
        assert result is not None

    def test_all_bullish_returns_bullish(self):
        rows = [
            ('aapl', 'catalyst',       'event', 0.9, 'llm_extracted_aapl'),
            ('aapl', 'earnings_beat',  'true',  0.9, 'llm_extracted_aapl'),
            ('aapl', 'revenue_beat',   'true',  0.8, 'llm_extracted_aapl'),
        ]
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('aapl', db) == 'bullish'

    def test_all_bearish_returns_bearish(self):
        rows = [
            ('xom', 'risk_factor',      'climate',   0.8, 'llm_extracted_xom'),
            ('xom', 'earnings_miss',    'q3',        0.9, 'llm_extracted_xom'),
            ('xom', 'guidance_lowered', '2026',      0.7, 'llm_extracted_xom'),
        ]
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('xom', db) == 'bearish'

    def test_mixed_near_zero_returns_neutral(self):
        rows = [
            ('msft', 'catalyst',    'ai_deal',  0.8, 'llm_extracted_msft'),
            ('msft', 'risk_factor', 'antitrust', 0.8, 'llm_extracted_msft'),
            ('msft', 'catalyst',    'cloud',    0.3, 'llm_extracted_msft'),
        ]
        # score = +0.8 - 0.8 + 0.3 = +0.3 < 0.5 → neutral
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('msft', db) == 'neutral'

    def test_non_llm_source_ignored(self):
        rows = [
            ('nvda', 'catalyst', 'gpu',   0.9, 'exchange_feed_yahoo'),  # not llm
            ('nvda', 'catalyst', 'ai',    0.9, 'rss_news'),             # not llm
            ('nvda', 'catalyst', 'other', 0.9, 'edgar_filing'),         # not llm
        ]
        db = self._make_db_with_llm(rows)
        # No llm_extracted_ rows → below min threshold
        assert _compute_news_sentiment('nvda', db) is None

    def test_ticker_isolation(self):
        rows = [
            ('aapl', 'catalyst',    'event', 0.9, 'llm_extracted_aapl'),
            ('aapl', 'earnings_beat','true',  0.9, 'llm_extracted_aapl'),
            ('aapl', 'revenue_beat', 'true',  0.8, 'llm_extracted_aapl'),
            ('msft', 'risk_factor', 'lawsuit', 0.9, 'llm_extracted_msft'),
            ('msft', 'earnings_miss','q2',     0.9, 'llm_extracted_msft'),
            ('msft', 'guidance_lowered','fy26', 0.8, 'llm_extracted_msft'),
        ]
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('aapl', db) == 'bullish'
        assert _compute_news_sentiment('msft', db) == 'bearish'

    def test_neutral_predicate_no_contribution(self):
        rows = [
            ('ko', 'signal_direction', 'long',  0.9, 'llm_extracted_ko'),
            ('ko', 'signal_direction', 'long',  0.9, 'llm_extracted_ko'),
            ('ko', 'signal_direction', 'long',  0.8, 'llm_extracted_ko'),
        ]
        # signal_direction is in neither set → score stays 0.0 → neutral
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('ko', db) == 'neutral'

    def test_high_confidence_bullish_dominates(self):
        rows = [
            ('amzn', 'earnings_beat',  'q3',   1.0, 'llm_extracted_amzn'),
            ('amzn', 'revenue_beat',   'q3',   1.0, 'llm_extracted_amzn'),
            ('amzn', 'risk_factor',    'trade', 0.3, 'llm_extracted_amzn'),
        ]
        # score = +1.0 + 1.0 - 0.3 = +1.7 > 0.5 → bullish
        db = self._make_db_with_llm(rows)
        assert _compute_news_sentiment('amzn', db) == 'bullish'

    def test_missing_confidence_defaults_to_0_5(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                confidence REAL,
                source TEXT DEFAULT 'test',
                timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00'
            )
        """)
        conn.executemany(
            "INSERT INTO facts (subject, predicate, object, confidence, source) VALUES (?, ?, ?, ?, ?)",
            [
                ('jnj', 'catalyst',    'drug', None, 'llm_extracted_jnj'),
                ('jnj', 'catalyst',    'fda',  None, 'llm_extracted_jnj'),
                ('jnj', 'catalyst',    'new',  None, 'llm_extracted_jnj'),
            ],
        )
        conn.commit()
        conn.close()
        self.db_path = path
        # Each bullish atom defaults to 0.5, total score = 1.5 > 0.5 → bullish
        assert _compute_news_sentiment('jnj', path) == 'bullish'


# ── Integration: adapter emits earnings_proximity + news_sentiment atoms ──────

class TestSignalEnrichmentAdapterIntegration:
    def setup_method(self):
        self.db_path = None

    def teardown_method(self):
        if self.db_path and os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def _make_equity_db(self, earnings_date: str = None, llm_rows: list = None):
        rows = [
            ('aapl', 'last_price',       '220.0', 0.9, 'exchange_feed_yahoo'),
            ('aapl', 'price_target',     '260.0', 0.8, 'broker_research_yahoo'),
            ('aapl', 'signal_direction', 'long',  0.9, 'exchange_feed_yahoo'),
            ('aapl', 'volatility_regime','medium_volatility', 0.7, 'exchange_feed_yahoo'),
            ('aapl', 'volatility_30d',   '28.0',  0.8, 'derived_signal_historical_aapl'),
            ('aapl', 'low_52w',          '165.0', 0.85, 'derived_signal_historical_aapl'),
            # Macro proxies for macro_confirmation
            ('spy', 'signal_direction',  'near_high', 0.9, 'exchange_feed_yahoo'),
            ('hyg', 'signal_direction',  'near_high', 0.8, 'exchange_feed_yahoo'),
            ('tlt', 'signal_direction',  'mid_range', 0.8, 'exchange_feed_yahoo'),
        ]
        if earnings_date:
            rows.append(('aapl', 'earnings_quality', earnings_date, 0.8, 'exchange_feed_yahoo'))
        if llm_rows:
            rows.extend(llm_rows)
        self.db_path = _make_facts_db(rows)
        return self.db_path

    def _get_atoms(self, db_path: str) -> dict:
        """Run adapter and return {predicate: object} for aapl atoms."""
        adapter = SignalEnrichmentAdapter(tickers=['aapl'], db_path=db_path)
        raw_atoms = adapter.fetch()
        return {a.predicate: a.object for a in raw_atoms if a.subject == 'aapl'}

    def test_earnings_proximity_emitted_when_date_present(self):
        future_date = _today_plus(10)  # pre_earnings_2w
        db = self._make_equity_db(earnings_date=future_date)
        atoms = self._get_atoms(db)
        assert 'earnings_proximity' in atoms
        assert atoms['earnings_proximity'] == 'pre_earnings_2w'

    def test_earnings_proximity_not_emitted_when_no_date(self):
        db = self._make_equity_db()
        atoms = self._get_atoms(db)
        assert 'earnings_proximity' not in atoms

    def test_earnings_proximity_post_earnings(self):
        db = self._make_equity_db(earnings_date=_today_plus(-5))
        atoms = self._get_atoms(db)
        assert atoms.get('earnings_proximity') == 'post_earnings'

    def test_earnings_proximity_3d(self):
        db = self._make_equity_db(earnings_date=_today_plus(2))
        atoms = self._get_atoms(db)
        assert atoms.get('earnings_proximity') == 'pre_earnings_3d'

    def test_news_sentiment_emitted_when_sufficient_llm_atoms(self):
        llm_rows = [
            ('aapl', 'catalyst',    'ai_chip', 0.9, 'llm_extracted_aapl'),
            ('aapl', 'earnings_beat','q3',      0.9, 'llm_extracted_aapl'),
            ('aapl', 'revenue_beat', 'q3',      0.8, 'llm_extracted_aapl'),
        ]
        db = self._make_equity_db(llm_rows=llm_rows)
        atoms = self._get_atoms(db)
        assert 'news_sentiment' in atoms
        assert atoms['news_sentiment'] == 'bullish'

    def test_news_sentiment_not_emitted_when_insufficient_llm_atoms(self):
        llm_rows = [
            ('aapl', 'catalyst', 'deal', 0.9, 'llm_extracted_aapl'),
        ]
        db = self._make_equity_db(llm_rows=llm_rows)
        atoms = self._get_atoms(db)
        assert 'news_sentiment' not in atoms

    def test_news_sentiment_bearish(self):
        llm_rows = [
            ('aapl', 'risk_factor',     'antitrust', 0.9, 'llm_extracted_aapl'),
            ('aapl', 'earnings_miss',   'q2',        0.9, 'llm_extracted_aapl'),
            ('aapl', 'guidance_lowered','fy26',      0.8, 'llm_extracted_aapl'),
        ]
        db = self._make_equity_db(llm_rows=llm_rows)
        atoms = self._get_atoms(db)
        assert atoms.get('news_sentiment') == 'bearish'

    def test_both_atoms_emitted_together(self):
        llm_rows = [
            ('aapl', 'catalyst',    'launch', 0.8, 'llm_extracted_aapl'),
            ('aapl', 'earnings_beat','q1',    0.9, 'llm_extracted_aapl'),
            ('aapl', 'revenue_beat', 'q1',    0.8, 'llm_extracted_aapl'),
        ]
        db = self._make_equity_db(
            earnings_date=_today_plus(20),
            llm_rows=llm_rows,
        )
        atoms = self._get_atoms(db)
        assert 'earnings_proximity' in atoms
        assert 'news_sentiment' in atoms
        assert atoms['earnings_proximity'] == 'pre_earnings_8w'
        assert atoms['news_sentiment'] == 'bullish'

    def test_existing_atoms_still_emitted(self):
        db = self._make_equity_db()
        atoms = self._get_atoms(db)
        for pred in ('price_regime', 'signal_quality', 'macro_confirmation',
                     'conviction_tier', 'thesis_risk_level'):
            assert pred in atoms, f"Missing existing atom: {pred}"

    def test_market_regime_atom_emitted(self):
        db = self._make_equity_db()
        adapter = SignalEnrichmentAdapter(tickers=['aapl'], db_path=db)
        raw_atoms = adapter.fetch()
        market_atoms = {a.predicate: a.object for a in raw_atoms if a.subject == 'market'}
        assert 'market_regime' in market_atoms
        assert market_atoms['market_regime'] in (
            'risk_on_expansion', 'risk_off_contraction',
            'stagflation', 'recovery', 'no_data',
        )


# ── _classify_market_regime ───────────────────────────────────────────────────

def _ms(spy='', hyg='', tlt=''):
    """Build macro_signals dict."""
    return {'spy': spy, 'hyg': hyg, 'tlt': tlt}

def _ta(gld='', uup=''):
    """Build ticker_atoms dict for regime function."""
    result = {}
    if gld:
        result['gld'] = {'signal_direction': gld}
    if uup:
        result['uup'] = {'signal_direction': uup}
    return result


class TestClassifyMarketRegime:
    def test_no_data_when_spy_and_hyg_missing(self):
        assert _classify_market_regime(_ms(), _ta()) == _REGIME_NO_DATA

    def test_no_data_when_only_tlt_present(self):
        assert _classify_market_regime(_ms(tlt='near_high'), _ta()) == _REGIME_NO_DATA

    def test_risk_on_expansion(self):
        # SPY up + HYG up + TLT not bullish
        result = _classify_market_regime(_ms(spy='near_high', hyg='near_high', tlt='mid_range'), _ta())
        assert result == _REGIME_RISK_ON_EXPANSION

    def test_risk_on_expansion_tlt_bearish(self):
        result = _classify_market_regime(_ms(spy='near_high', hyg='near_high', tlt='near_low'), _ta())
        assert result == _REGIME_RISK_ON_EXPANSION

    def test_recovery_all_three_bullish(self):
        # SPY up + HYG up + TLT up (rates falling = early cycle)
        result = _classify_market_regime(_ms(spy='near_high', hyg='near_high', tlt='near_high'), _ta())
        assert result == _REGIME_RECOVERY

    def test_recovery_checked_before_risk_on(self):
        # Both conditions could match — recovery should win (checked first)
        result = _classify_market_regime(_ms(spy='long', hyg='near_high', tlt='near_high'), _ta())
        assert result == _REGIME_RECOVERY

    def test_risk_off_contraction(self):
        result = _classify_market_regime(_ms(spy='near_low', hyg='near_low', tlt='near_high'), _ta())
        assert result == _REGIME_RISK_OFF_CONTRACTION

    def test_risk_off_spy_short_hyg_bearish(self):
        result = _classify_market_regime(_ms(spy='short', hyg='near_low'), _ta())
        assert result == _REGIME_RISK_OFF_CONTRACTION

    def test_stagflation_gold_up_spy_not(self):
        result = _classify_market_regime(_ms(spy='mid_range', hyg='mid_range'), _ta(gld='near_high'))
        assert result == _REGIME_STAGFLATION

    def test_stagflation_gold_up_spy_missing(self):
        # hyg present so not no_data, but spy missing (empty) and gld bullish
        result = _classify_market_regime(_ms(hyg='mid_range'), _ta(gld='near_high'))
        assert result == _REGIME_STAGFLATION

    def test_stagflation_not_when_spy_also_bullish(self):
        # Gold up AND SPY up → not stagflation → risk_on_expansion (if HYG also up)
        result = _classify_market_regime(_ms(spy='near_high', hyg='near_high'), _ta(gld='near_high'))
        assert result == _REGIME_RISK_ON_EXPANSION

    def test_residual_spy_bullish_mixed_credit(self):
        # SPY up but HYG not bullish → residual → risk_on_expansion
        result = _classify_market_regime(_ms(spy='near_high', hyg='mid_range'), _ta())
        assert result == _REGIME_RISK_ON_EXPANSION

    def test_residual_spy_bearish_no_hyg_bear(self):
        # SPY down, HYG mid → no strong rule → recovery (default residual)
        result = _classify_market_regime(_ms(spy='near_low', hyg='mid_range'), _ta())
        assert result == _REGIME_RECOVERY

    def test_uses_long_as_bullish_signal(self):
        # 'long' is in _BULLISH_SIGNALS
        result = _classify_market_regime(_ms(spy='long', hyg='near_high', tlt='mid_range'), _ta())
        assert result == _REGIME_RISK_ON_EXPANSION

    def test_valid_regime_values(self):
        valid = {_REGIME_RISK_ON_EXPANSION, _REGIME_RISK_OFF_CONTRACTION,
                 _REGIME_STAGFLATION, _REGIME_RECOVERY, _REGIME_NO_DATA}
        combos = [
            (_ms('near_high', 'near_high', 'mid_range'), _ta()),
            (_ms('near_high', 'near_high', 'near_high'), _ta()),
            (_ms('near_low', 'near_low', 'near_high'), _ta()),
            (_ms('mid_range', 'mid_range', ''), _ta(gld='near_high')),
            (_ms(), _ta()),
        ]
        for ms, ta in combos:
            r = _classify_market_regime(ms, ta)
            assert r in valid, f"Unexpected regime: {r}"


# ── _compute_skew_filter_atoms ────────────────────────────────────────────────

_SKEW_META = {'as_of': '2026-01-01T00:00:00+00:00'}


def _skew_filter(ticker='nvda', preds=None, market=None, direction='long'):
    """Helper to call _compute_skew_filter_atoms with minimal boilerplate."""
    return _compute_skew_filter_atoms(
        ticker       = ticker,
        preds        = preds or {},
        market_atoms = market or {},
        signal_direction = direction,
        src_base     = f'derived_signal_{ticker}',
        meta         = _SKEW_META,
    )


class TestComputeSkewFilterAtoms:
    def test_spike_skew_blocks_long(self):
        atoms = _skew_filter(preds={'skew_regime': 'spike'}, direction='long')
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert float(parts[0]) == _SKEW_FILTER_SPIKE_MULTIPLIER   # 0.0
        assert parts[2] == 'spike_skew'

    def test_elevated_skew_reduces_long(self):
        atoms = _skew_filter(preds={'skew_regime': 'elevated'}, direction='long')
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert float(parts[0]) == _SKEW_FILTER_ELEVATED_MULTIPLIER  # 0.5
        assert float(parts[1]) == _SKEW_FILTER_STOP_TIGHTEN_PCT     # 20.0
        assert parts[2] == 'elevated_skew'

    def test_spike_skew_does_not_affect_short(self):
        atoms = _skew_filter(preds={'skew_regime': 'spike'}, direction='short')
        assert atoms == []

    def test_normal_skew_no_filter(self):
        atoms = _skew_filter(preds={'skew_regime': 'normal'}, direction='long')
        assert atoms == []

    def test_no_skew_data_no_filter(self):
        # Neither ticker skew nor market tail_risk present — options_adapter
        # hasn't run yet; must not emit any filter atom
        atoms = _skew_filter(preds={}, market={}, direction='long')
        assert atoms == []

    def test_market_tail_risk_extreme_blocks_long(self):
        atoms = _skew_filter(
            preds={'skew_regime': 'normal'},
            market={'tail_risk': 'extreme'},
            direction='long',
        )
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert float(parts[0]) == _SKEW_FILTER_SPIKE_MULTIPLIER   # 0.0
        assert parts[2] == 'market_tail_risk_extreme'

    def test_market_tail_risk_elevated_reduces_long(self):
        atoms = _skew_filter(
            preds={},
            market={'tail_risk': 'elevated'},
            direction='long',
        )
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert float(parts[0]) == _SKEW_FILTER_ELEVATED_MULTIPLIER  # 0.5
        assert parts[2] == 'market_tail_risk_elevated'

    def test_ticker_spike_beats_market_elevated(self):
        # Worst case wins: spike takes priority over market elevated
        atoms = _skew_filter(
            preds={'skew_regime': 'spike'},
            market={'tail_risk': 'elevated'},
            direction='long',
        )
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert float(parts[0]) == _SKEW_FILTER_SPIKE_MULTIPLIER   # 0.0

    def test_encoded_value_is_pipe_delimited(self):
        atoms = _skew_filter(preds={'skew_regime': 'elevated'}, direction='long')
        assert len(atoms) == 1
        parts = atoms[0].object.split('|')
        assert len(parts) == 3
        float(parts[0])   # multiplier parseable as float
        float(parts[1])   # stop_tighten_pct parseable as float
        assert parts[2]   # reason non-empty string

    def test_predicate_is_skew_filter(self):
        atoms = _skew_filter(preds={'skew_regime': 'spike'}, direction='long')
        assert atoms[0].predicate == 'skew_filter'

    def test_upsert_true(self):
        atoms = _skew_filter(preds={'skew_regime': 'elevated'}, direction='long')
        assert atoms[0].upsert is True
