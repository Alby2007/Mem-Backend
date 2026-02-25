"""
middleware/encryption.py — Field-level encryption helpers

Uses Fernet symmetric encryption (AES-128-CBC + HMAC-SHA256).
Key is loaded from the DB_ENCRYPTION_KEY environment variable.

If DB_ENCRYPTION_KEY is not set, encrypt/decrypt are no-ops so the system
degrades gracefully in development without raising hard errors.

Lazy migration: if decrypt_field receives a value that is not valid Fernet
ciphertext (i.e. a legacy plaintext value), it returns the value unchanged.
The caller is responsible for re-encrypting on next write.
"""

from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)

_RAW_KEY: bytes | None = None

try:
    from cryptography.fernet import Fernet, InvalidToken
    _raw = os.environ.get('DB_ENCRYPTION_KEY', '').strip()
    if _raw:
        _RAW_KEY = _raw.encode()
        _FERNET = Fernet(_RAW_KEY)
    else:
        _FERNET = None
except ImportError:
    Fernet = None       # type: ignore
    InvalidToken = None # type: ignore
    _FERNET = None


def encrypt_field(value: str | None) -> str | None:
    """
    Encrypt a string field value.
    Returns None if value is None.
    Returns plaintext unchanged if encryption is not configured.
    """
    if value is None:
        return None
    if _FERNET is None:
        return value
    try:
        return _FERNET.encrypt(str(value).encode()).decode()
    except Exception as exc:
        _log.warning('encrypt_field failed: %s', exc)
        return value


def decrypt_field(value: str | None) -> str | None:
    """
    Decrypt a Fernet-encrypted field value.
    Returns None if value is None.
    Returns value unchanged if:
      - encryption is not configured
      - value is not valid ciphertext (lazy-migration: legacy plaintext)
    """
    if value is None:
        return None
    if _FERNET is None:
        return value
    try:
        return _FERNET.decrypt(value.encode()).decode()
    except Exception:
        return value


def encryption_enabled() -> bool:
    return _FERNET is not None
