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

_SYSTEM_PROMPT = """\
You are a trading analyst assistant powered by a live knowledge base.
The KNOWLEDGE CONTEXT block below contains ranked, authority-weighted market facts \
sourced from price feeds, macro indicators, SEC filings, and financial news.

Rules:
1. Reason strictly from the facts in KNOWLEDGE CONTEXT. Do not introduce external facts.
2. If a fact has a low confidence score, treat it as tentative and say so.
3. If the KB stress score is above 0.60, explicitly flag that KB coverage may be thin \
for this topic and qualify your answer accordingly.
4. Do not make specific buy/sell recommendations or price predictions beyond what the \
context directly states.
5. When the context contains conflicting signals, surface the conflict rather than \
picking a side.
6. Be concise. Lead with the most actionable insight from the context.\
"""


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
    user_parts: list[str] = []

    # ── Stress header ──────────────────────────────────────────────────────────
    if stress:
        composite  = stress.get("composite_stress", 0.0)
        decay      = stress.get("decay_pressure", 0.0)
        conflict   = stress.get("authority_conflict", 0.0)
        entropy    = stress.get("domain_entropy", 1.0)
        stress_line = (
            f"[KB STRESS: {composite:.2f} | "
            f"decay:{decay:.2f} | conflict:{conflict:.2f} | entropy:{entropy:.2f}]"
        )
        user_parts.append(stress_line)

        if composite >= _STRESS_WARN_THRESHOLD:
            user_parts.append(
                "⚠  KB coverage is thin for this topic "
                f"(composite stress {composite:.2f} ≥ {_STRESS_WARN_THRESHOLD}). "
                "Answer with caution and flag any gaps explicitly."
            )

    # ── KB insufficiency diagnosis (if fired) ──────────────────────────────────
    if kb_diagnosis:
        primary = kb_diagnosis.get("primary_type", "unknown")
        conf    = kb_diagnosis.get("confidence", 0.0)
        if primary != "unknown" and conf > 0.0:
            user_parts.append(
                f"[KB DIAGNOSIS: {primary} (confidence {conf:.2f}) — "
                "the knowledge base may have structural gaps for this topic.]"
            )

    # ── Prior session state (first turn of new session) ───────────────────────
    if prior_context:
        user_parts.append(prior_context)

    # ── KB context block ───────────────────────────────────────────────────────
    user_parts.append(snippet if snippet.strip() else "(No KB context available for this query.)")

    # ── User question ──────────────────────────────────────────────────────────
    user_parts.append(f"Question: {user_message}")

    return [
        {"role": "system",  "content": _SYSTEM_PROMPT},
        {"role": "user",    "content": "\n\n".join(user_parts)},
    ]
