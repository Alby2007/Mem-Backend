"""
llm/prompt_builder.py — System prompt and user-turn assembly for the KB-grounded chat layer.

Produces the two-message list [system, user] that is passed directly to ollama_client.chat().

Design:
  - System prompt is fixed and tells the LLM to reason only from provided context.
  - User turn injects: stress header, optional KB-thin warning, optional prior-session
    state, the structured KB snippet, and then the user's question.
  - stress threshold for explicit "thin coverage" warning: 0.6
"""

from __future__ import annotations

from typing import Optional

_STRESS_WARN_THRESHOLD = 0.6

_SYSTEM_PROMPT_BASE = """\
You are a trading analyst assistant powered by a live knowledge base.
The KNOWLEDGE CONTEXT block below contains ranked, authority-weighted market facts \
sourced from price feeds, macro indicators, SEC filings, and financial news.

Rules:
1. Reason strictly from the facts in KNOWLEDGE CONTEXT. Do not introduce external facts.
2. If a fact has a low confidence score, treat it as tentative and say so.
3. Do not make specific buy/sell recommendations or price predictions beyond what the \
context directly states.
4. When the context contains conflicting signals, surface the conflict rather than \
picking a side.
5. Be concise. Lead with the most actionable insight from the context.
6. Do NOT reproduce metadata tags, stress scores, or diagnostic labels in your answer — \
they are instructions for you, not content for the user.
7. If an atom contains a date or event without associated names or details, cite only \
what the atom states. Do not infer or generate names, people, or specific details \
not present in the context.\
"""

_SYSTEM_THIN_COVERAGE = (
    "\n8. IMPORTANT: KB coverage is thin for this topic. "
    "Say so explicitly at the start of your answer and qualify every claim accordingly. "
    "Do not speculate beyond what the context states."
)

_SYSTEM_DIAGNOSIS_SUFFIX = (
    "\n9. The knowledge base has a structural gap ({primary_type}) for this topic. "
    "Acknowledge the gap and indicate what additional data would improve the answer."
)


def build(
    user_message: str,
    snippet: str,
    stress: Optional[dict] = None,
    kb_diagnosis: Optional[dict] = None,
    prior_context: Optional[str] = None,
) -> list[dict]:
    """
    Build the [system, user] message list for Ollama.

    Args:
        user_message:  The user's natural-language question.
        snippet:       The formatted KB context string from retrieve().
        stress:        Dict with composite_stress, decay_pressure, etc.
        kb_diagnosis:  Optional kb_diagnosis block from /retrieve (fires when stressed).
        prior_context: Optional prior-session state string from working_state.

    Returns:
        [{"role": "system", "content": ...}, {"role": "user", "content": ...}]
    """
    # ── Build dynamic system prompt ────────────────────────────────────────────
    system_text = _SYSTEM_PROMPT_BASE

    composite = 0.0
    if stress:
        composite = stress.get("composite_stress", 0.0)
        if composite >= _STRESS_WARN_THRESHOLD:
            system_text += _SYSTEM_THIN_COVERAGE

    if kb_diagnosis:
        primary = kb_diagnosis.get("primary_type", "unknown")
        conf    = kb_diagnosis.get("confidence", 0.0)
        if primary not in ("unknown", "") and conf > 0.3:
            system_text += _SYSTEM_DIAGNOSIS_SUFFIX.format(primary_type=primary)

    # ── User turn ─────────────────────────────────────────────────────────────
    user_parts: list[str] = []

    # Prior session state (cross-session continuity)
    if prior_context:
        user_parts.append(prior_context)

    # KB context block
    user_parts.append(snippet if snippet.strip() else "(No KB context available for this query.)")

    # User question — always last so the LLM sees context then question
    user_parts.append(f"Question: {user_message}")

    return [
        {"role": "system", "content": system_text},
        {"role": "user",   "content": "\n\n".join(user_parts)},
    ]
