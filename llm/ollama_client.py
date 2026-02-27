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

_BASE_URL          = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
DEFAULT_MODEL      = os.environ.get("OLLAMA_MODEL", "llama3.2")
EXTRACTION_MODEL   = os.environ.get("OLLAMA_EXTRACTION_MODEL", "phi3")
VISION_MODEL       = os.environ.get("OLLAMA_VISION_MODEL", "llava")
DEFAULT_TIMEOUT    = int(os.environ.get("OLLAMA_CHAT_TIMEOUT", "180"))

_CHAT_URL  = f"{_BASE_URL}/api/chat"
_TAGS_URL  = f"{_BASE_URL}/api/tags"


def chat(
    messages: List[dict],
    model: str = DEFAULT_MODEL,
    stream: bool = False,
    timeout: int = DEFAULT_TIMEOUT,
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


def warmup(model: str = DEFAULT_MODEL, timeout: int = DEFAULT_TIMEOUT) -> bool:
    """
    Send a minimal prompt to Ollama to force the model to load into GPU/CPU memory.
    Call once at startup so the first real user request does not time out on cold start.
    Returns True if warmup succeeded, False if Ollama is unreachable or timed out.
    """
    _logger.info("[Ollama] warming up model '%s' ...", model)
    result = chat(
        messages=[{"role": "user", "content": "hi"}],
        model=model,
        timeout=timeout,
    )
    if result is not None:
        _logger.info("[Ollama] warmup complete for model '%s'", model)
        return True
    _logger.warning("[Ollama] warmup failed for model '%s' — model may not be pulled yet", model)
    return False


def chat_vision(
    image_b64: str,
    prompt: str,
    model: str = VISION_MODEL,
    timeout: int = 120,
) -> Optional[str]:
    """
    Send an image (base64-encoded) + text prompt to an Ollama vision model.

    Uses the Ollama multimodal message format:
        {"role": "user", "content": prompt, "images": [base64_string]}

    Returns the assistant reply string, or None on any error.
    Callers should check None and degrade gracefully.
    """
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
                "images": [image_b64],
            }
        ],
        "stream": False,
    }
    try:
        r = _requests.post(_CHAT_URL, json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json().get("message", {}).get("content", "")
    except _requests.exceptions.ConnectionError:
        _logger.warning("Ollama not reachable at %s (vision request)", _BASE_URL)
        return None
    except _requests.exceptions.Timeout:
        _logger.warning("Ollama vision request timed out after %ds", timeout)
        return None
    except Exception as e:
        _logger.error("Ollama chat_vision error: %s", e)
        return None


def is_available() -> bool:
    """Quick liveness check — True if Ollama is reachable."""
    try:
        r = _requests.get(f"{_BASE_URL}/api/tags", timeout=10)
        return r.status_code == 200
    except Exception:
        return False
