"""
llm/groq_client.py — Thin wrapper around the Groq REST API.

Groq provides ultra-fast LLM inference (llama3, mixtral, gemma).
Set GROQ_API_KEY env var to enable. Falls back gracefully if not set.

Public API:
    chat(messages, model, timeout) -> str | None
    is_available() -> bool
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

import requests as _requests

_logger = logging.getLogger(__name__)

GROQ_BASE_URL  = "https://api.groq.com/openai/v1"
DEFAULT_MODEL  = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
DEFAULT_TIMEOUT = int(os.environ.get("GROQ_TIMEOUT", "15"))


def _api_key() -> str:
    return os.environ.get("GROQ_API_KEY", "")


def chat(
    messages: List[dict],
    model: str = DEFAULT_MODEL,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[str]:
    """
    Send messages to Groq and return the assistant reply as a string.
    Returns None if GROQ_API_KEY is not set or on any error.
    """
    key = _api_key()
    if not key:
        return None

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 2048,
    }
    try:
        r = _requests.post(
            f"{GROQ_BASE_URL}/chat/completions",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except _requests.exceptions.ConnectionError:
        _logger.warning("Groq API not reachable")
        return None
    except _requests.exceptions.Timeout:
        _logger.warning("Groq request timed out after %ds", timeout)
        return None
    except _requests.exceptions.HTTPError as e:
        body = ""
        try:
            body = e.response.text[:200]
        except Exception:
            pass
        _logger.error("Groq HTTP error: %s — %s", e, body)
        return None
    except Exception as e:
        _logger.error("Groq chat error: %s", e)
        return None


def is_available() -> bool:
    """True if GROQ_API_KEY is set and Groq is reachable."""
    return bool(_api_key())
