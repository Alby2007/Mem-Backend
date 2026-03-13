"""
services/workflow_engine.py — Multi-turn chat workflow engine

Handles slash-command triggered guided conversations (e.g. /setup to log a trade).
State is persisted in chat_workflow_state table (one row per user, deleted on
completion or cancellation).
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

_logger = logging.getLogger(__name__)

# ── DB helpers ────────────────────────────────────────────────────────────────

def ensure_workflow_table(db_path: str) -> None:
    """Create chat_workflow_state table if it doesn't exist."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_workflow_state (
                user_id      TEXT PRIMARY KEY,
                workflow     TEXT NOT NULL,
                step         INTEGER NOT NULL DEFAULT 0,
                data         TEXT NOT NULL DEFAULT '{}',
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _get_state(db_path: str, user_id: str) -> Optional[dict]:
    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM chat_workflow_state WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)
    finally:
        conn.close()


def _set_state(db_path: str, user_id: str, workflow: str, step: int, data: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute(
            """INSERT INTO chat_workflow_state (user_id, workflow, step, data, created_at, updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 workflow=excluded.workflow, step=excluded.step,
                 data=excluded.data, updated_at=excluded.updated_at""",
            (user_id, workflow, step, json.dumps(data), now, now),
        )
        conn.commit()
    finally:
        conn.close()


def _delete_state(db_path: str, user_id: str) -> None:
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute("DELETE FROM chat_workflow_state WHERE user_id=?", (user_id,))
        conn.commit()
    finally:
        conn.close()


# ── Step / Workflow definitions ───────────────────────────────────────────────

@dataclass
class Step:
    field: str
    prompt: str
    optional: bool = False
    validator: Optional[object] = None  # callable(str) -> str | None  (None = valid, str = error msg)


@dataclass
class WorkflowResult:
    done: bool
    answer: str
    next_step: Optional[int] = None
    workflow_field: Optional[str] = None


# ── Validators ────────────────────────────────────────────────────────────────

_VALID_TIMEFRAMES = {'1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w'}
_VALID_PATTERNS = {
    'fvg', 'ifvg', 'breaker', 'mitigation', 'order_block',
    'liquidity_void', 'other', 'manual',
}


def _validate_ticker(val: str) -> Optional[str]:
    val = val.upper().strip()
    if not val:
        return 'Ticker cannot be empty.'
    if not re.match(r'^[A-Z0-9.\-\^/]+$', val):
        return f'"{val[:30]}" doesn\'t look like a ticker — please enter a symbol (e.g. BARC.L, NVDA, 0700.HK).'
    if len(val) > 20:
        return 'Ticker must be 20 characters or fewer.'
    return None


def _validate_direction(val: str) -> Optional[str]:
    if val.lower().strip() not in ('bullish', 'bearish'):
        return 'Please enter **bullish** or **bearish**.'
    return None


def _validate_price(val: str) -> Optional[str]:
    try:
        f = float(val.replace(',', ''))
        if f <= 0:
            return 'Price must be a positive number.'
        return None
    except ValueError:
        return f'"{val}" doesn\'t look like a valid price — please enter a number (e.g. 215.50).'


def _validate_timeframe(val: str) -> Optional[str]:
    if val.lower().strip() not in _VALID_TIMEFRAMES:
        return f'Please choose one of: {", ".join(sorted(_VALID_TIMEFRAMES))}.'
    return None


def _validate_pattern(val: str) -> Optional[str]:
    if val.lower().strip() not in _VALID_PATTERNS:
        return f'Please choose one of: {", ".join(sorted(_VALID_PATTERNS))}.'
    return None


# ── SetupTradeWorkflow steps ──────────────────────────────────────────────────

_SETUP_STEPS: List[Step] = [
    Step('ticker',       'What\'s the ticker?  (e.g. BARC.L, NVDA, 0700.HK)',
         validator=_validate_ticker),
    Step('direction',    'Direction — **bullish** or **bearish**?',
         validator=_validate_direction),
    Step('entry_price',  'Entry price?',
         validator=_validate_price),
    Step('stop_loss',    'Stop loss?',
         validator=_validate_price),
    Step('target_1',     'Target 1 (T1)?',
         validator=_validate_price),
    Step('target_2',     'Target 2 (T2)?  Type **skip** to leave blank.',
         optional=True, validator=_validate_price),
    Step('timeframe',    'Timeframe?  (1m / 5m / 15m / 30m / 1h / 2h / 4h / 1d / 1w)\nType **skip** to leave blank.',
         optional=True, validator=_validate_timeframe),
    Step('pattern_type', 'Pattern type?  (fvg / ifvg / breaker / mitigation / order_block / liquidity_void / other)\nType **skip** for manual.',
         optional=True, validator=_validate_pattern),
    Step('user_note',    'Any notes?  Type **skip** to leave blank.',
         optional=True),
]


# ── R:R helpers ───────────────────────────────────────────────────────────────

def _rr_label(entry: float, stop: float, target: float, direction: str) -> str:
    risk = abs(entry - stop)
    reward = abs(target - entry)
    if risk == 0:
        return 'N/A'
    return f'{reward / risk:.1f}R'


def _stop_side_warning(entry: float, stop: float, direction: str) -> Optional[str]:
    """Return a warning string if stop is on the wrong side of entry."""
    if direction == 'bullish' and stop > entry:
        return '⚠️ Stop appears above entry for a bullish trade — double-check direction.'
    if direction == 'bearish' and stop < entry:
        return '⚠️ Stop appears below entry for a bearish trade — double-check direction.'
    return None


# ── Confirmation card ─────────────────────────────────────────────────────────

def _build_confirmation(data: dict) -> str:
    entry  = float(data['entry_price'])
    stop   = float(data['stop_loss'])
    t1     = float(data['target_1'])
    t2     = data.get('target_2')
    dirn   = data['direction'].capitalize()

    rr_t1 = _rr_label(entry, stop, t1, data['direction'])
    rr_t2 = _rr_label(entry, stop, float(t2), data['direction']) if t2 else None

    rows = [
        ('Ticker',     data['ticker'].upper()),
        ('Direction',  dirn),
        ('Entry',      f"{entry:g}"),
        ('Stop',       f"{stop:g}"),
        ('T1',         f"{t1:g}"),
    ]
    if t2:
        rows.append(('T2', f"{float(t2):g}"))
    rows.append(('R:R (T1)', rr_t1))
    if rr_t2:
        rows.append(('R:R (T2)', rr_t2))
    if data.get('timeframe'):
        rows.append(('Timeframe', data['timeframe']))
    if data.get('pattern_type') and data['pattern_type'] != 'manual':
        rows.append(('Pattern', data['pattern_type'].upper()))
    if data.get('user_note'):
        note = data['user_note']
        rows.append(('Note', note[:60] + ('…' if len(note) > 60 else '')))

    table = '| Field | Value |\n|-------|-------|\n'
    table += '\n'.join(f'| {k} | {v} |' for k, v in rows)

    warnings = []
    w = _stop_side_warning(entry, stop, data['direction'])
    if w:
        warnings.append(w)
    if t2:
        t2f = float(t2)
        if data['direction'] == 'bullish' and t2f < t1:
            warnings.append('⚠️ T2 is below T1 for a bullish trade — double-check targets.')
        if data['direction'] == 'bearish' and t2f > t1:
            warnings.append('⚠️ T2 is above T1 for a bearish trade — double-check targets.')

    warning_block = ('\n\n' + '\n'.join(warnings)) if warnings else ''

    return f'✅ **Trade logged**\n\n{table}{warning_block}\n\nAdded to your journal.'


# ── Workflow registry ─────────────────────────────────────────────────────────

_WORKFLOW_STEPS: Dict[str, List[Step]] = {
    'setup_trade': _SETUP_STEPS,
}

_WORKFLOW_INTROS: Dict[str, str] = {
    'setup_trade': '📋 **Log a trade** — I\'ll ask a few quick questions.\n\n',
}


# ── Public API ────────────────────────────────────────────────────────────────

def detect_workflow_trigger(message: str) -> Optional[str]:
    """Return workflow key if message is a slash trigger, else None."""
    cmd = message.strip().lower()
    triggers = {'/setup': 'setup_trade', '/log': 'setup_trade'}
    return triggers.get(cmd)


def get_active_workflow(db_path: str, user_id: str) -> Optional[dict]:
    """Return active workflow state dict or None."""
    if not user_id:
        return None
    state = _get_state(db_path, user_id)
    if state is None:
        return None
    steps = _WORKFLOW_STEPS.get(state['workflow'], [])
    step_idx = state['step']
    current_prompt = steps[step_idx].prompt if step_idx < len(steps) else ''
    state['current_prompt'] = current_prompt
    return state


def start_workflow(db_path: str, user_id: str, workflow_key: str) -> str:
    """Create state row and return first prompt."""
    steps = _WORKFLOW_STEPS[workflow_key]
    _set_state(db_path, user_id, workflow_key, 0, {})
    intro = _WORKFLOW_INTROS.get(workflow_key, '')
    return intro + steps[0].prompt


def advance_workflow(db_path: str, user_id: str, message: str) -> WorkflowResult:
    """Process user's answer for the current step. Returns WorkflowResult."""
    state = _get_state(db_path, user_id)
    if state is None:
        return WorkflowResult(done=True, answer='No active workflow.')

    workflow_key = state['workflow']
    steps = _WORKFLOW_STEPS.get(workflow_key, [])
    step_idx = state['step']
    data = json.loads(state['data'])

    if step_idx >= len(steps):
        _delete_state(db_path, user_id)
        return WorkflowResult(done=True, answer='Workflow already complete.')

    current_step = steps[step_idx]
    raw = message.strip()

    # Blank message — re-ask same step
    if not raw:
        return WorkflowResult(
            done=False,
            answer=current_step.prompt,
            next_step=step_idx,
            workflow_field=current_step.field,
        )

    # Skip for optional fields
    is_skip = raw.lower() == 'skip'
    if is_skip and current_step.optional:
        # Don't store value — advance
        next_idx = step_idx + 1
        if next_idx >= len(steps):
            return _complete_workflow(db_path, user_id, workflow_key, data)
        _set_state(db_path, user_id, workflow_key, next_idx, data)
        next_step = steps[next_idx]
        return WorkflowResult(
            done=False,
            answer=next_step.prompt,
            next_step=next_idx,
            workflow_field=next_step.field,
        )

    # Validate
    if current_step.validator and not is_skip:
        error = current_step.validator(raw)
        if error:
            return WorkflowResult(
                done=False,
                answer=f'⚠️ {error}\n\n{current_step.prompt}',
                next_step=step_idx,
                workflow_field=current_step.field,
            )

    # Normalise value
    if current_step.field == 'ticker':
        value = raw.upper().strip()
    elif current_step.field == 'direction':
        value = raw.lower().strip()
    elif current_step.field in ('entry_price', 'stop_loss', 'target_1', 'target_2'):
        value = str(float(raw.replace(',', '')))
    elif current_step.field == 'timeframe':
        value = raw.lower().strip()
    elif current_step.field == 'pattern_type':
        value = raw.lower().strip()
    else:
        value = raw[:500]  # user_note — cap length

    data[current_step.field] = value
    next_idx = step_idx + 1

    if next_idx >= len(steps):
        return _complete_workflow(db_path, user_id, workflow_key, data)

    _set_state(db_path, user_id, workflow_key, next_idx, data)
    next_step = steps[next_idx]
    return WorkflowResult(
        done=False,
        answer=next_step.prompt,
        next_step=next_idx,
        workflow_field=next_step.field,
    )


def _complete_workflow(db_path: str, user_id: str, workflow_key: str, data: dict) -> WorkflowResult:
    """Commit the collected data to the journal and return confirmation card."""
    if workflow_key == 'setup_trade':
        try:
            from users.user_store import add_manual_journal_position
            add_manual_journal_position(db_path, user_id, data)
            _delete_state(db_path, user_id)
            card = _build_confirmation(data)
            return WorkflowResult(done=True, answer=card)
        except ValueError as e:
            # Validation error from journal function — preserve state so user can retry
            _logger.warning('workflow journal commit error: %s', e)
            return WorkflowResult(
                done=False,
                answer=f'⚠️ Could not save trade: {e}\n\nPlease check your entries and try again.',
                next_step=None,
                workflow_field=None,
            )
        except Exception as e:
            _logger.error('workflow journal commit unexpected error: %s', e)
            _delete_state(db_path, user_id)
            return WorkflowResult(
                done=True,
                answer=f'⚠️ An error occurred saving your trade. Please try again via the journal form.',
            )
    # Unknown workflow — clean up
    _delete_state(db_path, user_id)
    return WorkflowResult(done=True, answer='Workflow complete.')


def cancel_workflow(db_path: str, user_id: str) -> None:
    """Delete active workflow state."""
    _delete_state(db_path, user_id)
