"""
tests/test_user_store.py — User Store Tests

Covers: ensure_user_tables, create_user, get_user, update_preferences,
upsert_portfolio, get_portfolio, upsert_user_model, get_user_model,
log_delivery, get_delivery_history, already_delivered_today
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile

import pytest

from users.user_store import (
    ensure_user_tables,
    create_user,
    get_user,
    update_preferences,
    upsert_portfolio,
    get_portfolio,
    upsert_user_model,
    get_user_model,
    log_delivery,
    get_delivery_history,
    already_delivered_today,
)


def _tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    return path


# ── ensure_user_tables ────────────────────────────────────────────────────────

class TestEnsureUserTables:
    def test_creates_all_four_tables(self):
        path = _tmp_db()
        conn = sqlite3.connect(path)
        ensure_user_tables(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert 'user_portfolios' in tables
        assert 'user_models' in tables
        assert 'user_preferences' in tables
        assert 'snapshot_delivery_log' in tables

    def test_idempotent(self):
        path = _tmp_db()
        conn = sqlite3.connect(path)
        ensure_user_tables(conn)
        ensure_user_tables(conn)  # second call must not raise
        conn.close()


# ── create_user / get_user ────────────────────────────────────────────────────

class TestCreateGetUser:
    def test_creates_user(self):
        path = _tmp_db()
        result = create_user(path, 'u1', telegram_chat_id='123')
        assert result['user_id'] == 'u1'
        assert result['telegram_chat_id'] == '123'

    def test_default_delivery_time(self):
        path = _tmp_db()
        create_user(path, 'u2')
        u = get_user(path, 'u2')
        assert u['delivery_time'] == '08:00'

    def test_default_timezone(self):
        path = _tmp_db()
        create_user(path, 'u3')
        u = get_user(path, 'u3')
        assert u['timezone'] == 'UTC'

    def test_get_nonexistent_returns_none(self):
        path = _tmp_db()
        assert get_user(path, 'nobody') is None

    def test_selected_sectors_deserialized(self):
        path = _tmp_db()
        create_user(path, 'u4')
        u = get_user(path, 'u4')
        assert isinstance(u['selected_sectors'], list)

    def test_duplicate_create_is_ignored(self):
        path = _tmp_db()
        create_user(path, 'u5', telegram_chat_id='aaa')
        create_user(path, 'u5', telegram_chat_id='bbb')  # should not overwrite
        u = get_user(path, 'u5')
        assert u['telegram_chat_id'] == 'aaa'

    def test_onboarding_complete_default_zero(self):
        path = _tmp_db()
        create_user(path, 'u6')
        u = get_user(path, 'u6')
        assert u['onboarding_complete'] == 0


# ── update_preferences ────────────────────────────────────────────────────────

class TestUpdatePreferences:
    def test_updates_telegram_chat_id(self):
        path = _tmp_db()
        create_user(path, 'u10')
        update_preferences(path, 'u10', telegram_chat_id='999')
        u = get_user(path, 'u10')
        assert u['telegram_chat_id'] == '999'

    def test_updates_delivery_time(self):
        path = _tmp_db()
        create_user(path, 'u11')
        update_preferences(path, 'u11', delivery_time='09:30')
        u = get_user(path, 'u11')
        assert u['delivery_time'] == '09:30'

    def test_updates_timezone(self):
        path = _tmp_db()
        create_user(path, 'u12')
        update_preferences(path, 'u12', timezone_str='Europe/London')
        u = get_user(path, 'u12')
        assert u['timezone'] == 'Europe/London'

    def test_updates_selected_sectors(self):
        path = _tmp_db()
        create_user(path, 'u13')
        update_preferences(path, 'u13', selected_sectors=['technology', 'financials'])
        u = get_user(path, 'u13')
        assert 'technology' in u['selected_sectors']
        assert 'financials' in u['selected_sectors']

    def test_updates_selected_risk(self):
        path = _tmp_db()
        create_user(path, 'u14')
        update_preferences(path, 'u14', selected_risk='aggressive')
        u = get_user(path, 'u14')
        assert u['selected_risk'] == 'aggressive'

    def test_sets_onboarding_complete(self):
        path = _tmp_db()
        create_user(path, 'u15')
        update_preferences(path, 'u15', onboarding_complete=1)
        u = get_user(path, 'u15')
        assert u['onboarding_complete'] == 1

    def test_creates_user_if_not_exists(self):
        path = _tmp_db()
        update_preferences(path, 'new_user', delivery_time='07:00')
        u = get_user(path, 'new_user')
        assert u is not None
        assert u['delivery_time'] == '07:00'

    def test_partial_update_leaves_other_fields(self):
        path = _tmp_db()
        create_user(path, 'u16', telegram_chat_id='456')
        update_preferences(path, 'u16', delivery_time='10:00')
        u = get_user(path, 'u16')
        assert u['telegram_chat_id'] == '456'
        assert u['delivery_time'] == '10:00'


# ── upsert_portfolio / get_portfolio ──────────────────────────────────────────

class TestPortfolio:
    def test_upsert_and_get(self):
        path = _tmp_db()
        holdings = [
            {'ticker': 'AAPL', 'quantity': 10, 'avg_cost': 150.0, 'sector': 'Technology'},
            {'ticker': 'MS', 'quantity': 5, 'avg_cost': 90.0},
        ]
        result = upsert_portfolio(path, 'u20', holdings)
        assert result['count'] == 2
        portfolio = get_portfolio(path, 'u20')
        tickers = [h['ticker'] for h in portfolio]
        assert 'AAPL' in tickers
        assert 'MS' in tickers

    def test_replace_replaces_all(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u21', [{'ticker': 'AAPL'}, {'ticker': 'GOOG'}])
        upsert_portfolio(path, 'u21', [{'ticker': 'MSFT'}])
        portfolio = get_portfolio(path, 'u21')
        tickers = [h['ticker'] for h in portfolio]
        assert tickers == ['MSFT']

    def test_empty_holdings_clears_portfolio(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u22', [{'ticker': 'AAPL'}])
        upsert_portfolio(path, 'u22', [])
        assert get_portfolio(path, 'u22') == []

    def test_ticker_normalized_to_upper(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u23', [{'ticker': 'aapl', 'quantity': 1}])
        portfolio = get_portfolio(path, 'u23')
        assert portfolio[0]['ticker'] == 'AAPL'

    def test_skips_empty_ticker(self):
        path = _tmp_db()
        result = upsert_portfolio(path, 'u24', [{'ticker': '', 'quantity': 1}])
        assert get_portfolio(path, 'u24') == []

    def test_get_empty_returns_empty_list(self):
        path = _tmp_db()
        assert get_portfolio(path, 'nobody') == []


# ── upsert_user_model / get_user_model ────────────────────────────────────────

class TestUserModel:
    def test_upsert_and_get(self):
        path = _tmp_db()
        upsert_user_model(
            path, 'u30',
            risk_tolerance='moderate',
            sector_affinity=['technology', 'financials'],
            avg_conviction_threshold=0.7,
            holding_style='momentum',
            portfolio_beta=1.1,
            concentration_risk='concentrated',
        )
        m = get_user_model(path, 'u30')
        assert m['risk_tolerance'] == 'moderate'
        assert 'technology' in m['sector_affinity']
        assert m['holding_style'] == 'momentum'

    def test_get_nonexistent_returns_none(self):
        path = _tmp_db()
        assert get_user_model(path, 'nobody') is None

    def test_sector_affinity_deserialized(self):
        path = _tmp_db()
        upsert_user_model(path, 'u31', 'aggressive', ['energy'], 0.8, 'value', 0.9, 'diversified')
        m = get_user_model(path, 'u31')
        assert isinstance(m['sector_affinity'], list)

    def test_replace_updates_existing(self):
        path = _tmp_db()
        upsert_user_model(path, 'u32', 'conservative', [], None, 'value', None, 'diversified')
        upsert_user_model(path, 'u32', 'aggressive', ['tech'], 0.9, 'momentum', 1.3, 'concentrated')
        m = get_user_model(path, 'u32')
        assert m['risk_tolerance'] == 'aggressive'
        assert m['holding_style'] == 'momentum'


# ── log_delivery / get_delivery_history / already_delivered_today ─────────────

class TestDeliveryLog:
    def test_log_and_retrieve(self):
        path = _tmp_db()
        log_delivery(path, 'u40', True, message_length=500,
                     regime_at_delivery='risk_on_expansion', opportunities_count=3)
        history = get_delivery_history(path, 'u40')
        assert len(history) == 1
        assert history[0]['success'] == 1
        assert history[0]['regime_at_delivery'] == 'risk_on_expansion'
        assert 'delivered_at_local_date' in history[0]

    def test_failed_delivery_logged(self):
        path = _tmp_db()
        log_delivery(path, 'u41', False)
        history = get_delivery_history(path, 'u41')
        assert history[0]['success'] == 0

    def test_history_newest_first(self):
        path = _tmp_db()
        log_delivery(path, 'u42', True)
        log_delivery(path, 'u42', True)
        history = get_delivery_history(path, 'u42')
        assert history[0]['delivered_at'] >= history[1]['delivered_at']

    def test_limit_respected(self):
        path = _tmp_db()
        for _ in range(10):
            log_delivery(path, 'u43', True)
        history = get_delivery_history(path, 'u43', limit=3)
        assert len(history) == 3

    def test_already_delivered_today_true(self):
        path = _tmp_db()
        entry = log_delivery(path, 'u44', True, local_date='2026-02-24')
        assert already_delivered_today(path, 'u44', '2026-02-24') is True

    def test_already_delivered_today_false_different_date(self):
        path = _tmp_db()
        log_delivery(path, 'u45', True, local_date='2026-02-24')
        assert already_delivered_today(path, 'u45', '2000-01-01') is False

    def test_already_delivered_today_false_no_log(self):
        path = _tmp_db()
        assert already_delivered_today(path, 'nobody', '2026-02-24') is False

    def test_already_delivered_today_ignores_failures(self):
        path = _tmp_db()
        log_delivery(path, 'u46', False, local_date='2026-02-24')
        # Failed delivery should not count
        assert already_delivered_today(path, 'u46', '2026-02-24') is False

    def test_local_date_stored_in_log(self):
        path = _tmp_db()
        log_delivery(path, 'u47', True, local_date='2026-02-25')
        history = get_delivery_history(path, 'u47')
        assert history[0]['delivered_at_local_date'] == '2026-02-25'

    def test_local_date_defaults_to_utc_date(self):
        path = _tmp_db()
        entry = log_delivery(path, 'u48', True)  # no local_date
        assert entry['delivered_at_local_date'] is not None
        assert len(entry['delivered_at_local_date']) == 10  # YYYY-MM-DD
