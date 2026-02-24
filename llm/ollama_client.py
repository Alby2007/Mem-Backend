"""
llm/ollama_client.py — Thin wrapper around the local Ollama REST API.

Ollama must be running at http://localhost:11434 (default).
Override with env var OLLAMA_BASE_URL.

Public API:
    chat(model, messages, stream=False, timeout=60) -> str | None
    list_models() -> list[str]
"""

from __future__ import annotations

import json
import logging
import os
from typing import List, Optional

import requests as _requests

_logger = logging.getLogger(__name__)

_BASE_URL   = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")

_CHAT_URL  = f"{_BASE_URL}/api/chat"
_TAGS_URL  = f"{_BASE_URL}/api/tags"


def chat(
    messages: List[dict],
    model: str = DEFAULT_MODEL,
    stream: bool = False,
    timeout: int = 120,
) -> Optional[str]:
    """
    Send a list of chat messages to Ollama and return the assistant reply as a string.

    messages format: [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}]

    Returns None on any connection or HTTP error — callers should degrade gracefully.
    """
    payload = {
        "model":    model,
        "messages": messages,
        "stream":   stream,
    }
    try:
        r = _requests.post(_CHAT_URL, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        if stream:
            # Stream mode: newline-delimited JSON — collect all content chunks
            content_parts = []
            for line in r.text.strip().splitlines():
                try:
                    chunk = json.loads(line)
                    content_parts.append(
                        chunk.get("message", {}).get("content", "")
                    )
                    if chunk.get("done"):
                        break
                except json.JSONDecodeError:
                    continue
            return "".join(content_parts)

        # Non-stream: single JSON response
        return data.get("message", {}).get("content", "")

    except _requests.exceptions.ConnectionError:
        _logger.warning("Ollama not reachable at %s", _BASE_URL)
        return None
    except _requests.exceptions.Timeout:
        _logger.warning("Ollama request timed out after %ds", timeout)
        return None
    except Exception as e:
        _logger.error("Ollama chat error: %s", e)
        return None


def list_models() -> List[str]:
    """
    Return the names of locally available Ollama models.
    Returns [] if Ollama is unreachable.
    """
    try:
        r = _requests.get(_TAGS_URL, timeout=10)
        r.raise_for_status()
        models = r.json().get("models", [])
        return [m.get("name", "") for m in models if m.get("name")]
    except Exception as e:
        _logger.warning("Ollama list_models error: %s", e)
        return []


def is_available() -> bool:
    """Quick liveness check — True if Ollama is reachable."""
    try:
        r = _requests.get(f"{_BASE_URL}/api/tags", timeout=5)
        return r.status_code == 200
    except Exception:
        return False
