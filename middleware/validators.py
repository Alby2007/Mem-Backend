"""
middleware/validators.py — Input validation for API boundary

All validators return a ValidationResult(valid, errors).
Call validate_*() at the top of write endpoints and return 400 if not valid.

USAGE
=====
    from middleware.validators import validate_portfolio_submission

    result = validate_portfolio_submission(holdings)
    if not result.valid:
        return jsonify({'error': 'validation_failed', 'details': result.errors}), 400
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, List, Optional

# ── Constants ──────────────────────────────────────────────────────────────────

_TICKER_RE = re.compile(r'^[A-Z]{1,5}$')
_TIME_RE    = re.compile(r'^\d{2}:\d{2}$')

_VALID_TIMEFRAMES = frozenset({'15m', '1h', '4h', '1d'})
_VALID_TIERS      = frozenset({'basic', 'pro'})
_VALID_RISKS      = frozenset({'conservative', 'moderate', 'aggressive'})
_VALID_DIRECTIONS = frozenset({'bullish', 'bearish'})
_VALID_OUTCOMES   = frozenset({'hit_t1', 'hit_t2', 'hit_t3', 'stopped_out', 'pending', 'skipped'})

_MAX_HOLDINGS       = 50
_MAX_SECTORS        = 20
_MAX_TICKER_LEN     = 5
_MAX_PATTERN_TYPES  = 7

try:
    import zoneinfo as _zoneinfo
    _VALID_TIMEZONES = _zoneinfo.available_timezones()
except Exception:
    _VALID_TIMEZONES = frozenset({'UTC', 'Europe/London', 'America/New_York', 'Asia/Tokyo'})


# ── Result type ────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    valid: bool
    errors: List[str] = field(default_factory=list)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_positive_number(v: Any) -> bool:
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


def _is_non_negative_number(v: Any) -> bool:
    try:
        return float(v) >= 0
    except (TypeError, ValueError):
        return False


# ── Validators ─────────────────────────────────────────────────────────────────

def validate_portfolio_submission(holdings: Any) -> ValidationResult:
    """
    Validate a list of portfolio holding dicts.

    Each holding must have:
      ticker   — 1–5 uppercase letters
      quantity — positive number
      avg_cost — positive number
    """
    errors: List[str] = []

    if not isinstance(holdings, list):
        return ValidationResult(valid=False, errors=['holdings must be a list'])

    if len(holdings) == 0:
        return ValidationResult(valid=False, errors=['holdings list is empty'])

    if len(holdings) > _MAX_HOLDINGS:
        errors.append(f'too many holdings: max {_MAX_HOLDINGS}, got {len(holdings)}')

    for i, h in enumerate(holdings):
        if not isinstance(h, dict):
            errors.append(f'holding[{i}] must be an object')
            continue

        ticker = str(h.get('ticker', '')).strip().upper()
        if not _TICKER_RE.match(ticker):
            errors.append(f'holding[{i}]: invalid ticker "{h.get("ticker")}" — must be 1–5 uppercase letters')

        qty = h.get('quantity')
        if qty is not None and not _is_positive_number(qty):
            errors.append(f'holding[{i}] ({ticker}): quantity must be a positive number, got {qty!r}')

        cost = h.get('avg_cost')
        if cost is not None and not _is_positive_number(cost):
            errors.append(f'holding[{i}] ({ticker}): avg_cost must be a positive number, got {cost!r}')

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def validate_onboarding(data: dict) -> ValidationResult:
    """Validate POST /users/{id}/onboarding body."""
    errors: List[str] = []

    delivery_time = data.get('delivery_time')
    if delivery_time is not None:
        if not isinstance(delivery_time, str) or not _TIME_RE.match(delivery_time):
            errors.append(f'delivery_time must be HH:MM format, got {delivery_time!r}')
        else:
            h, m = delivery_time.split(':')
            if not (0 <= int(h) <= 23 and 0 <= int(m) <= 59):
                errors.append(f'delivery_time out of range: {delivery_time}')

    tz = data.get('timezone')
    if tz is not None and tz not in _VALID_TIMEZONES:
        errors.append(f'timezone "{tz}" is not a valid IANA timezone')

    risk = data.get('risk_tolerance') or data.get('selected_risk')
    if risk is not None and risk not in _VALID_RISKS:
        errors.append(f'risk_tolerance must be one of: {", ".join(sorted(_VALID_RISKS))}')

    sectors = data.get('selected_sectors')
    if sectors is not None:
        if not isinstance(sectors, list):
            errors.append('selected_sectors must be a list')
        elif len(sectors) > _MAX_SECTORS:
            errors.append(f'selected_sectors: too many entries (max {_MAX_SECTORS})')

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def validate_tip_config(data: dict) -> ValidationResult:
    """Validate POST /users/{id}/tip-config body."""
    errors: List[str] = []

    tip_time = data.get('tip_delivery_time')
    if tip_time is not None:
        if not isinstance(tip_time, str) or not _TIME_RE.match(tip_time):
            errors.append(f'tip_delivery_time must be HH:MM format, got {tip_time!r}')

    tip_tz = data.get('tip_delivery_timezone')
    if tip_tz is not None and tip_tz not in _VALID_TIMEZONES:
        errors.append(f'tip_delivery_timezone "{tip_tz}" is not a valid IANA timezone')

    timeframes = data.get('tip_timeframes')
    if timeframes is not None:
        if not isinstance(timeframes, list):
            errors.append('tip_timeframes must be a list')
        else:
            bad = [t for t in timeframes if t not in _VALID_TIMEFRAMES]
            if bad:
                errors.append(f'invalid timeframes: {bad} — valid: {sorted(_VALID_TIMEFRAMES)}')

    tier = data.get('tier')
    if tier is not None and tier not in _VALID_TIERS:
        errors.append(f'tier must be one of: {", ".join(sorted(_VALID_TIERS))}')

    account_size = data.get('account_size')
    if account_size is not None and not _is_positive_number(account_size):
        errors.append(f'account_size must be a positive number, got {account_size!r}')

    risk_pct = data.get('max_risk_per_trade_pct')
    if risk_pct is not None:
        try:
            v = float(risk_pct)
            if not (0 < v <= 100):
                errors.append(f'max_risk_per_trade_pct must be between 0 and 100, got {v}')
        except (TypeError, ValueError):
            errors.append(f'max_risk_per_trade_pct must be a number, got {risk_pct!r}')

    pattern_types = data.get('tip_pattern_types')
    if pattern_types is not None:
        if not isinstance(pattern_types, list):
            errors.append('tip_pattern_types must be a list')
        elif len(pattern_types) > _MAX_PATTERN_TYPES:
            errors.append(f'tip_pattern_types: too many entries (max {_MAX_PATTERN_TYPES})')

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def validate_ingest_atom(atom: dict) -> ValidationResult:
    """Validate a single KB atom for POST /ingest."""
    errors: List[str] = []

    subject = str(atom.get('subject', '')).strip()
    if not subject:
        errors.append('subject is required and must be non-empty')

    predicate = str(atom.get('predicate', '')).strip()
    if not predicate:
        errors.append('predicate is required and must be non-empty')

    obj = atom.get('object')
    if obj is None or str(obj).strip() == '':
        errors.append('object is required and must be non-empty')

    confidence = atom.get('confidence')
    if confidence is not None:
        try:
            c = float(confidence)
            if not (0.0 <= c <= 1.0):
                errors.append(f'confidence must be between 0.0 and 1.0, got {c}')
        except (TypeError, ValueError):
            errors.append(f'confidence must be a number, got {confidence!r}')

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def validate_feedback(data: dict) -> ValidationResult:
    """Validate POST /feedback body."""
    errors: List[str] = []

    user_id = str(data.get('user_id', '')).strip()
    if not user_id:
        errors.append('user_id is required')

    outcome = str(data.get('outcome', '')).strip()
    if outcome not in _VALID_OUTCOMES:
        errors.append(f'outcome must be one of: {", ".join(sorted(_VALID_OUTCOMES))}')

    for key in ('tip_id', 'pattern_id'):
        v = data.get(key)
        if v is not None:
            try:
                if int(v) <= 0:
                    errors.append(f'{key} must be a positive integer')
            except (TypeError, ValueError):
                errors.append(f'{key} must be a positive integer, got {v!r}')

    return ValidationResult(valid=len(errors) == 0, errors=errors)


def validate_register(data: dict) -> ValidationResult:
    """Validate POST /auth/register body."""
    errors: List[str] = []

    email = str(data.get('email', '')).strip()
    if not email or '@' not in email or '.' not in email.split('@')[-1]:
        errors.append('a valid email address is required')

    password = str(data.get('password', ''))
    if len(password) < 8:
        errors.append('password must be at least 8 characters')

    user_id = str(data.get('user_id', '')).strip()
    if not user_id:
        errors.append('user_id is required')
    elif len(user_id) > 64:
        errors.append('user_id must be 64 characters or fewer')

    return ValidationResult(valid=len(errors) == 0, errors=errors)
