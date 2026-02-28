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

GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
GROQ_BASE_URL  = "https://api.groq.com/openai/v1"
DEFAULT_MODEL  = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant")
DEFAULT_TIMEOUT = int(os.environ.get("GROQ_TIMEOUT", "30"))


def chat(
    messages: List[dict],
    model: str = DEFAULT_MODEL,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[str]:
    """
    Send messages to Groq and return the assistant reply as a string.
    Returns None if GROQ_API_KEY is not set or on any error.
    """
    if not GROQ_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1024,
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
    except Exception as e:
        _logger.error("Groq chat error: %s", e)
        return None


def is_available() -> bool:
    """True if GROQ_API_KEY is set and Groq is reachable."""
    return bool(GROQ_API_KEY)
