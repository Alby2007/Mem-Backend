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
OPERATOR_BOT_TOKEN = os.environ.get('OPERATOR_BOT_TOKEN', '')


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


def handle_callback(data: str, db_path: str) -> None:
    """
    Called by OperatorBotPoller when operator taps Approve/Reject on a write gate message.
    data format: 'approve:QUEUE_ID' or 'reject:QUEUE_ID'
    """
    import sqlite3
    from datetime import datetime, timezone

    try:
        action, queue_id_str = data.split(':', 1)
        queue_id = int(queue_id_str)
    except (ValueError, AttributeError):
        _log.warning('handle_callback: malformed data: %s', data)
        return

    approved = action == 'approve'

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        row = conn.execute(
            'SELECT tool_name, path, old_str, new_str, full_content, status FROM mcp_write_queue WHERE id = ?',
            (queue_id,)
        ).fetchone()

        if not row:
            _send(f"⚠️ Write queue ID {queue_id} not found")
            return

        tool_name, path, old_str, new_str, full_content, status = row

        if status != 'pending':
            _send(f"ℹ️ Queue ID {queue_id} already {_e(status)} — ignoring")
            return

        now = datetime.now(timezone.utc).isoformat()

        if not approved:
            conn.execute(
                'UPDATE mcp_write_queue SET status=?, resolved_at=? WHERE id=?',
                ('rejected', now, queue_id)
            )
            conn.commit()
            _send(f"❌ *Rejected* — {_e(tool_name)} on `{_e(path or '')}`")
            return

        result_msg = _execute_write(tool_name, path, old_str, new_str, full_content)

        conn.execute(
            'UPDATE mcp_write_queue SET status=?, resolved_at=? WHERE id=?',
            ('executed', now, queue_id)
        )
        conn.commit()

        _send(result_msg)

    except Exception as e:
        _log.error('handle_callback: error: %s', e)
        _send(f"🚨 Write gate error: {_e(str(e))}")
    finally:
        conn.close()


def _execute_write(tool_name: str, path: str, old_str: str, new_str: str, full_content: str) -> str:
    """
    Execute an approved write operation. Returns a Telegram-formatted result message.
    """
    import os as _os

    try:
        if tool_name == 'file_patch':
            if not old_str:
                return f"🚨 *file\\_patch failed* — old\\_str is empty for `{_e(path or '')}`"
            with open(path, 'r') as f:
                content = f.read()
            if old_str not in content:
                return f"🚨 *file\\_patch failed* — string not found in `{_e(path or '')}`"
            if content.count(old_str) > 1:
                return f"🚨 *file\\_patch failed* — string appears multiple times in `{_e(path or '')}`"
            patched = content.replace(old_str, new_str or '', 1)
            with open(path, 'w') as f:
                f.write(patched)
            lines_changed = abs((new_str or '').count('\n') - (old_str or '').count('\n'))
            return (
                f"✅ *file\\_patch applied*\n"
                f"`{_e(path or '')}`\n"
                f"Lines changed: {lines_changed}\n"
                f"Preview: `{_e((new_str or '')[:120])}`"
            )

        elif tool_name == 'file_write':
            if not full_content:
                return "🚨 *file\\_write failed* — no content provided"
            _os.makedirs(_os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                f.write(full_content)
            lines = full_content.count('\n') + 1
            size_kb = len(full_content.encode()) / 1024
            return (
                f"✅ *file\\_write applied*\n"
                f"`{_e(path or '')}`\n"
                f"{lines} lines, {size_kb:.1f}KB written"
            )

        elif tool_name == 'git_pull_deploy':
            import subprocess
            r1 = subprocess.run(
                ['git', '-C', '/home/ubuntu/trading-galaxy', 'pull'],
                capture_output=True, text=True, timeout=60
            )
            pull_out = r1.stdout.strip().splitlines()[-1] if r1.stdout.strip() else 'no output'
            r2 = subprocess.run(
                ['sudo', 'systemctl', 'restart', 'trading-galaxy'],
                capture_output=True, text=True, timeout=30
            )
            if r2.returncode != 0:
                return f"⚠️ *git pull ok, restart failed*\n`{_e(r2.stderr[:200])}`"
            return (
                f"✅ *git\\_pull\\_deploy complete*\n"
                f"Pull: {_e(pull_out)}\n"
                f"Service restarted ✓"
            )

        elif tool_name == 'service_restart':
            import subprocess
            r = subprocess.run(
                ['sudo', 'systemctl', 'restart', 'trading-galaxy'],
                capture_output=True, text=True, timeout=30
            )
            if r.returncode != 0:
                return f"🚨 *service\\_restart failed*\n`{_e(r.stderr[:200])}`"
            return "✅ *service\\_restart complete* — trading\\-galaxy restarted ✓"

        else:
            return f"⚠️ Unknown tool in write queue: {_e(tool_name or '')}"

    except Exception as e:
        return f"🚨 *{_e(tool_name or '')} failed*: {_e(str(e))}"
