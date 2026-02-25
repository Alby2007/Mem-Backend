"""
tests/test_telegram_notifier.py — Telegram Notifier Tests (mocked requests)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from notifications.telegram_notifier import TelegramNotifier


def _mock_response(status_code: int, text: str = '{"ok":true}'):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


# ── TestIsConfigured ──────────────────────────────────────────────────────────

class TestIsConfigured:

    def test_configured_with_token(self):
        n = TelegramNotifier(bot_token='abc123')
        assert n.is_configured is True

    def test_not_configured_without_token(self, monkeypatch):
        monkeypatch.delenv('TELEGRAM_BOT_TOKEN', raising=False)
        n = TelegramNotifier()
        assert n.is_configured is False

    def test_configured_via_env(self, monkeypatch):
        monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'env_token')
        n = TelegramNotifier()
        assert n.is_configured is True


# ── TestSend ──────────────────────────────────────────────────────────────────

class TestSend:

    def test_returns_true_on_200(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)):
            result = n.send('123', 'hello')
        assert result is True

    def test_returns_false_on_400(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(400, '{"ok":false}')):
            result = n.send('123', 'hello')
        assert result is False

    def test_returns_false_no_token(self, monkeypatch):
        monkeypatch.delenv('TELEGRAM_BOT_TOKEN', raising=False)
        n = TelegramNotifier()
        result = n.send('123', 'hello')
        assert result is False

    def test_returns_false_empty_chat_id(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)):
            result = n.send('', 'hello')
        assert result is False

    def test_returns_false_empty_message(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)):
            result = n.send('123', '')
        assert result is False

    def test_returns_false_on_network_exception(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', side_effect=ConnectionError('timeout')):
            result = n.send('123', 'hello')
        assert result is False

    def test_posts_to_correct_url(self):
        n = TelegramNotifier(bot_token='mytoken')
        with patch('requests.post', return_value=_mock_response(200)) as mock_post:
            n.send('chat123', 'msg')
        url = mock_post.call_args[0][0]
        assert 'mytoken' in url
        assert 'sendMessage' in url

    def test_sends_parse_mode_markdownv2(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)) as mock_post:
            n.send('123', 'msg')
        payload = mock_post.call_args[1]['json']
        assert payload['parse_mode'] == 'MarkdownV2'

    def test_sends_chat_id_as_string(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)) as mock_post:
            n.send(999, 'msg')
        payload = mock_post.call_args[1]['json']
        assert payload['chat_id'] == '999'


# ── TestSendTest ──────────────────────────────────────────────────────────────

class TestSendTest:

    def test_send_test_returns_true_on_200(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)):
            result = n.send_test('chat123')
        assert result is True

    def test_send_test_returns_false_no_token(self, monkeypatch):
        monkeypatch.delenv('TELEGRAM_BOT_TOKEN', raising=False)
        n = TelegramNotifier()
        result = n.send_test('chat123')
        assert result is False

    def test_send_test_message_contains_trading_galaxy(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)) as mock_post:
            n.send_test('chat123')
        payload = mock_post.call_args[1]['json']
        assert 'Trading Galaxy' in payload['text']


# ── TestSendPlain ─────────────────────────────────────────────────────────────

class TestSendPlain:

    def test_send_plain_uses_empty_parse_mode(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)) as mock_post:
            n.send_plain('123', 'plain text')
        payload = mock_post.call_args[1]['json']
        assert payload['parse_mode'] == ''

    def test_send_plain_returns_true_on_200(self):
        n = TelegramNotifier(bot_token='tok')
        with patch('requests.post', return_value=_mock_response(200)):
            result = n.send_plain('123', 'hello')
        assert result is True
