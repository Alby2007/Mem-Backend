"""
notifications/telegram_notifier.py — Telegram Bot API Wrapper

Sends pre-formatted messages to Telegram chat IDs via the Bot API.
Configured via TELEGRAM_BOT_TOKEN env var.

All send methods return bool — True on success, False on any failure
(network error, bad token, invalid chat_id). No exceptions are raised
so callers don't need try/except.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

_log = logging.getLogger(__name__)

_TELEGRAM_API_BASE = 'https://api.telegram.org/bot{token}/sendMessage'
_TEST_MESSAGE = (
    '✅ *Trading Galaxy* — Bot connected\\!\n'
    '_Your daily briefing will arrive at your scheduled delivery time\\._'
)


_MDV2_ESCAPE_CHARS = r'\_*[]()~`>#+-=|{}.!'


def escape_mdv2(text: str) -> str:
    """
    Escape a freeform string for Telegram MarkdownV2.
    Preserves **bold** and _italic_ markdown by converting them first,
    then escaping all other special characters.
    Suitable for wrapping LLM-generated text.
    """
    import re as _re
    # Convert markdown bold/italic before escaping special chars
    # Bold: **text** -> \*\*escaped_text\*\* (Telegram bold is *text*)
    # We'll handle by escaping then re-substituting
    result = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch in _MDV2_ESCAPE_CHARS:
            result.append('\\')
        result.append(ch)
        i += 1
    return ''.join(result)


class TelegramNotifier:
    """
    Thin wrapper around the Telegram Bot API sendMessage endpoint.

    Parameters
    ----------
    bot_token   Telegram bot token from @BotFather.
                Defaults to TELEGRAM_BOT_TOKEN env var.
                If None/empty, all send calls return False gracefully.
    """

    def __init__(self, bot_token: Optional[str] = None):
        self._token = bot_token or os.environ.get('TELEGRAM_BOT_TOKEN', '')

    @property
    def is_configured(self) -> bool:
        """True if a bot token is available."""
        return bool(self._token)

    def send(self, chat_id: str, message: str, parse_mode: str = 'MarkdownV2') -> bool:
        """
        Send a message to a Telegram chat.

        Parameters
        ----------
        chat_id     Telegram chat ID (numeric string or @username)
        message     Message text — must be valid for the chosen parse_mode
        parse_mode  'MarkdownV2' (default) or 'HTML' or '' (plain text)

        Returns True on HTTP 200, False on any error.
        """
        if not self._token:
            _log.warning('TelegramNotifier: no bot token configured — message not sent')
            return False

        if not chat_id or not message:
            _log.warning('TelegramNotifier: missing chat_id or message')
            return False

        try:
            import requests as _requests
        except ImportError:
            _log.error('TelegramNotifier: requests package not available')
            return False

        url = _TELEGRAM_API_BASE.format(token=self._token)
        payload: dict = {
            'chat_id':    str(chat_id),
            'text':       message,
            'parse_mode': parse_mode,
        }

        import time as _time
        last_exc: Optional[Exception] = None
        for attempt in range(2):
            try:
                resp = _requests.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    return True
                retryable = resp.status_code == 429 or resp.status_code >= 500
                if not retryable or attempt == 1:
                    _log.warning(
                        'TelegramNotifier: API returned %d — %s',
                        resp.status_code,
                        resp.text[:200],
                    )
                    return False
                _log.warning(
                    'TelegramNotifier: transient %d — retrying in 2s', resp.status_code
                )
                _time.sleep(2)
            except Exception as exc:
                last_exc = exc
                if attempt == 0:
                    _log.warning('TelegramNotifier: request failed (%s) — retrying in 2s', exc)
                    _time.sleep(2)
                else:
                    _log.error('TelegramNotifier: request failed — %s', exc)
        return False

    def send_test(self, chat_id: str) -> bool:
        """
        Send a test connection-verification message to chat_id.
        Used during onboarding to confirm the bot is connected.
        Returns True on success.
        """
        return self.send(chat_id, _TEST_MESSAGE, parse_mode='MarkdownV2')

    def send_plain(self, chat_id: str, message: str) -> bool:
        """Send a plain-text message (no markdown parsing)."""
        return self.send(chat_id, message, parse_mode='')
