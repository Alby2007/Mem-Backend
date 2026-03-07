"""
ingest/llm_extraction_adapter.py — LLM-Based Atom Extraction Adapter

Drains the extraction_queue table (populated by RSSAdapter and EDGARAdapter)
and uses Ollama to convert raw article text into structured KB atoms.

Design:
  - Reads up to BATCH_SIZE unprocessed rows per run (default 20)
  - Sends each row to Ollama with a structured extraction prompt
  - Parses the JSON array response into RawAtom objects
  - Marks processed=1 on success; increments failed_attempts on parse failure
  - Items with failed_attempts >= MAX_FAILURES are permanently skipped
  - Falls back to [] gracefully if Ollama is unreachable

Model: OLLAMA_EXTRACTION_MODEL env var (default: phi3)
Source prefix: llm_extracted_  (authority 0.70, half-life 12h)
Interval: recommended 300s (5 min) — stays ahead of RSS queue at 20 items/run
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom
from ingest.rss_adapter import _ensure_extraction_queue

_logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BATCH_SIZE   = int(os.environ.get('LLM_EXTRACTION_BATCH', os.environ.get('INGEST_BATCH_SIZE', '20')))
MAX_FAILURES = 3

_VALID_PREDICATES = {
    'catalyst', 'risk_factor', 'signal_direction', 'forward_guidance',
    'earnings_result', 'rating_change', 'price_target', 'insider_transaction',
    'key_finding', 'regime_label', 'central_bank_stance', 'inflation_environment',
}

_VALID_DIRECTIONS = {'long', 'short', 'neutral'}

# Exact lowercase macro entity names the parser accepts as valid subjects
_VALID_MACRO_ENTITIES = {'fed', 'ecb', 'treasury', 'us_macro', 'us_labor', 'us_yields', 'us_credit'}

# Watchlist for ticker context hint in the prompt
_WATCHLIST_HINT = (
    'AAPL MSFT GOOGL AMZN NVDA META TSLA AVGO JPM V MA BAC GS '
    'AMD INTC QCOM CRM ADBE NOW NFLX DIS UNH LLY ABBV XOM CVX'
)

# Confidence hedge-word calibration
_HIGH_CERTAINTY = {'confirmed', 'announced', 'reported', 'filed', 'disclosed',
                   'approved', 'completed', 'signed', 'launched', 'beats', 'beat'}
_MED_CERTAINTY  = {'expects', 'signals', 'plans', 'projects', 'guides', 'raises',
                   'lowers', 'upgrades', 'downgrades', 'says', 'notes', 'sees'}
_LOW_CERTAINTY  = {'may', 'might', 'could', 'considering', 'exploring',
                   'potential', 'rumoured', 'reportedly', 'sources say'}


# ── Prompt construction ────────────────────────────────────────────────────────

_EXTRACTION_SYSTEM = """\
You are a financial knowledge extraction system. Your only job is to extract \
structured facts from financial news and return them as a JSON array.

Each fact in the array must have exactly these keys:
  "subject"    — MUST be a 2-5 character uppercase ticker symbol (e.g. AAPL, NVDA, JPM)
                 OR one of these exact macro entities: fed, ecb, treasury, us_macro,
                 us_labor, us_yields, us_credit
                 If no valid subject exists, return []
  "predicate"  — one of: catalyst, risk_factor, signal_direction, forward_guidance,
                  earnings_result, rating_change, price_target, insider_transaction,
                  key_finding, regime_label, central_bank_stance, inflation_environment
  "object"     — concise snake_case fact, max 80 characters
  "reasoning"  — one sentence explaining why this fact was extracted

Rules:
- Return ONLY a valid JSON array. No prose, no markdown, no explanation.
- Return [] if no valid ticker or macro entity subject exists in the text.
- object must be snake_case, no spaces, max 80 chars.
- Do not invent facts not present in the text.\
"""

_EXTRACTION_USER_TMPL = """\
Watchlist (prefer these tickers if ambiguous): {watchlist}

Text: {text}

JSON array:\
"""


def _build_prompt(text: str) -> List[dict]:
    return [
        {'role': 'system', 'content': _EXTRACTION_SYSTEM},
        {'role': 'user',   'content': _EXTRACTION_USER_TMPL.format(
            watchlist=_WATCHLIST_HINT,
            text=text[:800],
        )},
    ]


# ── Confidence from language ───────────────────────────────────────────────────

def _confidence_from_language(text: str) -> float:
    """Fallback confidence scalar when LLM doesn't calibrate well."""
    lower = text.lower()
    words = set(re.findall(r'\b\w+\b', lower))
    if words & _HIGH_CERTAINTY:
        return 0.80
    if words & _MED_CERTAINTY:
        return 0.70
    if words & _LOW_CERTAINTY:
        return 0.58
    return 0.65


# ── LLM response parsing ──────────────────────────────────────────────────────

def _parse_llm_atoms(
    response_text: str,
    source_prefix: str,
    fallback_confidence: float,
    now_iso: str,
) -> Tuple[List[RawAtom], bool]:
    """
    Parse Ollama response into RawAtom list.

    Returns (atoms, success) — success=False means malformed JSON.
    """
    if not response_text:
        return [], False

    # Strip markdown fences if model wraps the JSON
    clean = response_text.strip()
    clean = re.sub(r'^```(?:json)?\s*', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\s*```$', '', clean)
    clean = clean.strip()

    # Some models prefix with prose — try to extract the first [...] block
    if not clean.startswith('['):
        bracket_match = re.search(r'\[.*\]', clean, re.DOTALL)
        if bracket_match:
            clean = bracket_match.group(0)
        else:
            return [], False

    try:
        data = json.loads(clean)
    except json.JSONDecodeError:
        return [], False

    if not isinstance(data, list):
        return [], False

    atoms: List[RawAtom] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        subject   = str(item.get('subject', '')).strip()
        predicate = str(item.get('predicate', '')).strip().lower()
        obj       = str(item.get('object', '')).strip()

        if not (subject and predicate and obj):
            continue

        # Enforce subject constraint: valid ticker (2-5 uppercase chars)
        # or known macro entity (lowercase exact match)
        subj_lower = subject.lower()
        is_valid_ticker = bool(re.match(r'^[A-Z]{2,5}$', subject))
        is_valid_macro  = subj_lower in _VALID_MACRO_ENTITIES
        if not (is_valid_ticker or is_valid_macro):
            continue
        subject = subj_lower  # normalise to lowercase for KB consistency

        # Validate predicate is in our known vocab
        if predicate not in _VALID_PREDICATES:
            continue

        # Validate signal_direction values
        if predicate == 'signal_direction' and obj.lower() not in _VALID_DIRECTIONS:
            continue

        # Confidence: computed purely from language heuristic — LLM field ignored
        conf = fallback_confidence

        # Truncate object to 250 chars
        obj = obj[:250]

        atoms.append(RawAtom(
            subject=subject,
            predicate=predicate,
            object=obj,
            confidence=conf,
            source=source_prefix,
            metadata={'extracted_at': now_iso},
        ))

    return atoms, True


# ── Adapter ───────────────────────────────────────────────────────────────────

class LLMExtractionAdapter(BaseIngestAdapter):
    """
    LLM-based atom extraction adapter.

    Drains the extraction_queue table and uses Ollama to produce structured
    atoms from raw news/filing text. Runs after RSSAdapter and EDGARAdapter
    in the scheduler so the queue is always populated before extraction starts.

    Falls back gracefully to [] if Ollama is unreachable.
    """

    def __init__(self, db_path: Optional[str] = None):
        super().__init__(name='llm_extraction')
        self._db_path = db_path or os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')

    def _llm_call(self, messages: List[dict]) -> Optional[str]:
        """Try Ollama first, fall back to Groq if unavailable."""
        try:
            from llm.ollama_client import chat as ollama_chat, is_available, EXTRACTION_MODEL
            if is_available(model=EXTRACTION_MODEL):
                return ollama_chat(messages, model=EXTRACTION_MODEL, timeout=60)
        except Exception as e:
            self._logger.debug('Ollama unavailable: %s', e)

        try:
            from llm.groq_client import chat as groq_chat, is_available as groq_available
            if groq_available():
                self._logger.debug('Falling back to Groq for LLM extraction')
                return groq_chat(messages)
        except Exception as e:
            self._logger.debug('Groq unavailable: %s', e)

        return None

    def _any_llm_available(self) -> bool:
        """Quick check — no network call needed."""
        try:
            from llm.ollama_client import is_available
            if is_available():
                return True
        except Exception:
            pass
        try:
            from llm.groq_client import is_available as groq_available
            if groq_available():
                return True
        except Exception:
            pass
        return False

    def fetch(self) -> List[RawAtom]:

        now_iso = datetime.now(timezone.utc).isoformat()

        if not self._any_llm_available():
            self._logger.info('No LLM backend reachable (Ollama + Groq) — skipping extraction run')
            return []

        try:
            conn = sqlite3.connect(self._db_path)
            _ensure_extraction_queue(conn)
        except Exception as e:
            self._logger.error('Cannot open extraction_queue: %s', e)
            return []

        # Fetch a batch of unprocessed, non-exhausted rows
        try:
            rows = conn.execute(
                """
                SELECT id, text, source, url
                FROM extraction_queue
                WHERE processed = 0
                  AND failed_attempts < ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (MAX_FAILURES, BATCH_SIZE),
            ).fetchall()
        except Exception as e:
            self._logger.error('Failed to read extraction_queue: %s', e)
            conn.close()
            return []

        if not rows:
            conn.close()
            return []

        self._logger.info('LLM extraction: processing %d items from queue', len(rows))

        all_atoms: List[RawAtom] = []

        for row_id, text, original_source, url in rows:
            # Derive source prefix from original_source (news_wire_x → llm_extracted_news_wire_x)
            source_prefix = f'llm_extracted_{original_source}' if original_source else 'llm_extracted_rss'
            fallback_conf = _confidence_from_language(text or '')

            try:
                messages = _build_prompt(text or '')
                response = self._llm_call(messages)
            except Exception as e:
                self._logger.warning('LLM call failed for row %d: %s', row_id, e)
                conn.execute(
                    'UPDATE extraction_queue SET failed_attempts = failed_attempts + 1 WHERE id = ?',
                    (row_id,),
                )
                conn.commit()
                continue

            if response is None:
                self._logger.warning('Row %d: no LLM response (both backends failed)', row_id)
            else:
                self._logger.info('Row %d: LLM response preview: %.120s', row_id, response)

            atoms, success = _parse_llm_atoms(response or '', source_prefix, fallback_conf, now_iso)

            if success:
                all_atoms.extend(atoms)
                conn.execute(
                    """UPDATE extraction_queue
                       SET processed = 1, processed_at = ?, atoms_extracted = ?
                       WHERE id = ?""",
                    (now_iso, len(atoms), row_id),
                )
            else:
                conn.execute(
                    'UPDATE extraction_queue SET failed_attempts = failed_attempts + 1 WHERE id = ?',
                    (row_id,),
                )
                # Permanently skip items that have exhausted retries
                conn.execute(
                    """UPDATE extraction_queue
                       SET processed = 1, processed_at = ?
                       WHERE id = ? AND failed_attempts >= ?""",
                    (now_iso, row_id, MAX_FAILURES),
                )

            conn.commit()

        conn.close()

        self._logger.info(
            'LLM extraction: produced %d atoms from %d items', len(all_atoms), len(rows)
        )
        return all_atoms
