"""
notifications/operator_bot.py

Operator Telegram bot — sends system reports to the configured operator chat.
Configure via OPERATOR_TELEGRAM_CHAT_ID env var (falls back to
TELEGRAM_BOT_TOKEN for the actual send).
"""

from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)

_OPERATOR_CHAT_ID = os.environ.get('OPERATOR_TELEGRAM_CHAT_ID', '')


def _e(text: str) -> str:
    """Escape a string for Telegram MarkdownV2."""
    _SPECIAL = r'\_*[]()~`>#+-=|{}.!'
    result = []
    for ch in str(text):
        if ch in _SPECIAL:
            result.append('\\')
        result.append(ch)
    return ''.join(result)


def _send(message: str) -> bool:
    """Send a MarkdownV2 message to the operator chat. Returns True on success."""
    chat_id = _OPERATOR_CHAT_ID or os.environ.get('OPERATOR_TELEGRAM_CHAT_ID', '')
    if not chat_id:
        _log.warning('operator_bot: OPERATOR_TELEGRAM_CHAT_ID not set — message not sent')
        return False
    try:
        from notifications.telegram_notifier import TelegramNotifier
        notifier = TelegramNotifier()
        if not notifier.is_configured:
            _log.warning('operator_bot: TelegramNotifier not configured')
            return False
        return notifier.send(chat_id, message, parse_mode='MarkdownV2')
    except Exception as e:
        _log.warning('operator_bot: send failed: %s', e)
        return False


def send_observatory_report(
    findings: list,
    executed: list,
    queued: list,
    tokens_used: int,
    duration_sec: float,
    budget_today: int,
    budget_limit: int,
) -> None:
    """Send observatory run summary to the operator Telegram chat."""
    if not findings and not executed and not queued:
        _send('🔭 Observatory — all clear, no findings')
        return

    severity_icons = {'critical': '🚨', 'high': '⚠️', 'medium': 'ℹ️', 'info': '💬'}

    lines = ['*🔭 Observatory Run*\n']

    for f in findings:
        icon = severity_icons.get(f.get('severity', 'info'), '•')
        lines.append(f"{icon} *{_e(f.get('severity','').upper())}*: {_e(f.get('description',''))}")

    if executed:
        lines.append(f"\n*✅ Auto\\-actioned:* {_e(str(len(executed)))}")
        for a in executed:
            lines.append(f"  • {_e(a.get('rationale', a.get('action_type', '')))}")

    if queued:
        lines.append(f"\n*⏳ Queued for approval:* {_e(str(len(queued)))}")
        for q in queued:
            lines.append(f"  • {_e(q.get('subject', q.get('action_type', '')))}")

    lines.append(
        f"\n_Tokens: {_e(str(tokens_used))} \\| "
        f"Budget: {_e(str(budget_today))}/{_e(str(budget_limit))} \\| "
        f"{_e(f'{duration_sec:.1f}')}s_"
    )

    _send('\n'.join(lines))
