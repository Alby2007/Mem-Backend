"""
tests/test_tip_scheduler.py — Unit tests for notifications/tip_scheduler.py
"""

from __future__ import annotations
import json
import tempfile
import os
import pytest
from unittest.mock import patch, MagicMock

from notifications.tip_scheduler import (
    TipScheduler, _should_tip, _pick_best_pattern, _get_local_now,
)
from users.user_store import (
    ensure_user_tables, create_user, update_preferences,
    upsert_pattern_signal, get_tip_history, already_tipped_today,
    log_tip_delivery, update_tip_config,
)


def _tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    import sqlite3
    conn = sqlite3.connect(path)
    ensure_user_tables(conn)
    conn.close()
    return path


def _open_pattern(
    ticker='NVDA', pattern_type='fvg', direction='bullish',
    zone_high=192.0, zone_low=189.0, quality_score=0.85, timeframe='1h',
) -> dict:
    return {
        'ticker': ticker, 'pattern_type': pattern_type, 'direction': direction,
        'zone_high': zone_high, 'zone_low': zone_low,
        'zone_size_pct': round((zone_high - zone_low) / zone_low * 100, 4),
        'timeframe': timeframe, 'formed_at': '2026-02-25T08:00:00',
        'status': 'open', 'quality_score': quality_score,
        'kb_conviction': 'high', 'kb_regime': 'risk_on_expansion',
        'kb_signal_dir': 'long',
    }


class TestGetLocalNow:
    def test_returns_datetime(self):
        from datetime import datetime
        result = _get_local_now('UTC')
        assert isinstance(result, datetime)

    def test_invalid_tz_falls_back_to_utc(self):
        from datetime import datetime, timezone
        result = _get_local_now('Not/ATimezone')
        assert result.tzinfo is not None

    def test_empty_string_falls_back(self):
        result = _get_local_now('')
        assert result is not None


class TestShouldTip:
    def test_wrong_time_returns_false(self):
        path = _tmp_db()
        create_user(path, 'u1')
        assert _should_tip(path, 'u1', '23:59', 'UTC') is False

    def test_already_tipped_today_returns_false(self):
        path = _tmp_db()
        create_user(path, 'u2')
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_tip_delivery(path, 'u2', success=True, local_date=today)
        delivery_time = datetime.now(timezone.utc).strftime('%H:%M')
        assert _should_tip(path, 'u2', delivery_time, 'UTC') is False

    def test_no_prior_tip_correct_time_returns_true(self):
        path = _tmp_db()
        create_user(path, 'u3')
        from datetime import datetime, timezone
        delivery_time = datetime.now(timezone.utc).strftime('%H:%M')
        assert _should_tip(path, 'u3', delivery_time, 'UTC') is True

    def test_failed_prior_tip_not_counted(self):
        path = _tmp_db()
        create_user(path, 'u4')
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_tip_delivery(path, 'u4', success=False, local_date=today)
        delivery_time = datetime.now(timezone.utc).strftime('%H:%M')
        assert _should_tip(path, 'u4', delivery_time, 'UTC') is True


class TestPickBestPattern:
    def test_returns_none_when_no_patterns(self):
        path = _tmp_db()
        result = _pick_best_pattern(path, 'u1', 'basic', ['1h'], None)
        assert result is None

    def test_returns_highest_quality(self):
        path = _tmp_db()
        upsert_pattern_signal(path, _open_pattern(quality_score=0.6))
        upsert_pattern_signal(path, _open_pattern(quality_score=0.9, ticker='AAPL'))
        result = _pick_best_pattern(path, 'u1', 'basic', ['1h'], None)
        assert result is not None
        assert result['quality_score'] == 0.9

    def test_filters_by_tier_pattern_type(self):
        path = _tmp_db()
        # order_block not in basic tier
        upsert_pattern_signal(path, _open_pattern(pattern_type='order_block', quality_score=0.95))
        result = _pick_best_pattern(path, 'u1', 'basic', ['1h'], None)
        assert result is None

    def test_pro_tier_allows_order_block(self):
        path = _tmp_db()
        upsert_pattern_signal(path, _open_pattern(pattern_type='order_block', quality_score=0.95))
        result = _pick_best_pattern(path, 'u1', 'pro', ['1h'], None)
        assert result is not None
        assert result['pattern_type'] == 'order_block'

    def test_filters_by_timeframe(self):
        path = _tmp_db()
        # Only 15m pattern, user configured for 1h only
        upsert_pattern_signal(path, _open_pattern(timeframe='15m', quality_score=0.9))
        result = _pick_best_pattern(path, 'u1', 'pro', ['1h'], None)
        assert result is None

    def test_skips_already_alerted_user(self):
        path = _tmp_db()
        pid = upsert_pattern_signal(path, _open_pattern(quality_score=0.9))
        from users.user_store import mark_pattern_alerted
        mark_pattern_alerted(path, pid, 'u1')
        result = _pick_best_pattern(path, 'u1', 'basic', ['1h'], None)
        assert result is None

    def test_different_user_can_receive_same_pattern(self):
        path = _tmp_db()
        pid = upsert_pattern_signal(path, _open_pattern(quality_score=0.9))
        from users.user_store import mark_pattern_alerted
        mark_pattern_alerted(path, pid, 'u1')
        # u2 has not been alerted
        result = _pick_best_pattern(path, 'u2', 'basic', ['1h'], None)
        assert result is not None

    def test_custom_pattern_type_filter(self):
        path = _tmp_db()
        upsert_pattern_signal(path, _open_pattern(pattern_type='fvg', quality_score=0.9))
        upsert_pattern_signal(path, _open_pattern(pattern_type='ifvg', quality_score=0.8, ticker='AAPL'))
        # User wants only ifvg
        result = _pick_best_pattern(path, 'u1', 'basic', ['1h'], ['ifvg'])
        assert result is not None
        assert result['pattern_type'] == 'ifvg'


class TestTipSchedulerLifecycle:
    def test_start_creates_thread(self):
        path = _tmp_db()
        sched = TipScheduler(path, interval_sec=3600)
        sched.start()
        assert sched._thread is not None
        assert sched._thread.is_alive()
        sched.stop()

    def test_double_start_no_duplicate_thread(self):
        path = _tmp_db()
        sched = TipScheduler(path, interval_sec=3600)
        sched.start()
        thread1 = sched._thread
        sched.start()  # second call should be no-op
        assert sched._thread is thread1
        sched.stop()

    def test_stop_signals_event(self):
        path = _tmp_db()
        sched = TipScheduler(path, interval_sec=3600)
        sched.start()
        sched.stop()
        assert sched._stop_event.is_set()


class TestDeliverTipToUser:
    def test_no_chat_id_skips_silently(self):
        path = _tmp_db()
        upsert_pattern_signal(path, _open_pattern())
        from notifications.tip_scheduler import _deliver_tip_to_user
        prefs = {
            'telegram_chat_id': None,
            'tier': 'basic',
            'tip_timeframes': ['1h'],
            'tip_pattern_types': None,
            'account_size': 10000.0,
            'max_risk_per_trade_pct': 1.0,
            'account_currency': 'GBP',
            'tip_delivery_timezone': 'UTC',
        }
        # Should not raise, should log and return
        _deliver_tip_to_user(path, 'u1', prefs)
        history = get_tip_history(path, 'u1')
        assert len(history) == 0  # nothing logged — skipped before send

    def test_no_eligible_patterns_skips(self):
        path = _tmp_db()
        # Only an order_block which basic tier can't access
        upsert_pattern_signal(path, _open_pattern(pattern_type='order_block'))
        from notifications.tip_scheduler import _deliver_tip_to_user
        prefs = {
            'telegram_chat_id': 'chat123',
            'tier': 'basic',
            'tip_timeframes': ['1h'],
            'tip_pattern_types': None,
            'account_size': 10000.0,
            'max_risk_per_trade_pct': 1.0,
            'account_currency': 'GBP',
            'tip_delivery_timezone': 'UTC',
        }
        _deliver_tip_to_user(path, 'u1', prefs)
        assert len(get_tip_history(path, 'u1')) == 0

    @patch('requests.post')
    def test_successful_send_logs_tip(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        path = _tmp_db()
        pid = upsert_pattern_signal(path, _open_pattern())
        from notifications.tip_scheduler import _deliver_tip_to_user
        import os; os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
        prefs = {
            'telegram_chat_id': 'chat123',
            'tier': 'basic',
            'tip_timeframes': ['1h'],
            'tip_pattern_types': None,
            'account_size': 10000.0,
            'max_risk_per_trade_pct': 1.0,
            'account_currency': 'GBP',
            'tip_delivery_timezone': 'UTC',
        }
        _deliver_tip_to_user(path, 'u1', prefs)
        history = get_tip_history(path, 'u1')
        assert len(history) == 1
        assert history[0]['success'] == 1
        assert history[0]['pattern_signal_id'] == pid

    @patch('requests.post')
    def test_successful_send_marks_user_alerted(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        path = _tmp_db()
        pid = upsert_pattern_signal(path, _open_pattern())
        from notifications.tip_scheduler import _deliver_tip_to_user
        from users.user_store import get_open_patterns
        import os; os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
        prefs = {
            'telegram_chat_id': 'chat123',
            'tier': 'basic',
            'tip_timeframes': ['1h'],
            'tip_pattern_types': None,
            'account_size': 10000.0,
            'max_risk_per_trade_pct': 1.0,
            'account_currency': 'GBP',
            'tip_delivery_timezone': 'UTC',
        }
        _deliver_tip_to_user(path, 'u1', prefs)
        patterns = get_open_patterns(path)
        row = next((p for p in patterns if p['id'] == pid), None)
        assert row is not None
        assert 'u1' in row['alerted_users']
