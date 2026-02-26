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
You are Trading Galaxy, an epistemically-governed trading intelligence system.
The KNOWLEDGE CONTEXT block below contains ranked, authority-weighted market facts \
sourced from live price feeds, macro indicators, SEC filings, and financial news.

CRITICAL: You must ONLY answer from the facts in KNOWLEDGE CONTEXT. \
Your training data is stale — never use it for prices, signals, or market conditions. \
ALIAS RULE: If the context contains a line beginning with \
'INSTRUCTION: ' or 'ALIAS RESOLVED: ', that is a hard directive. \
It means the user's ticker has been resolved to a KB ticker. \
The atoms listed below THAT line ARE the user's data. \
You MUST answer from those atoms. You MUST NOT say you have no data. \
NO-DATA RULE: Only use the no-data response below if the KNOWLEDGE CONTEXT \
contains ZERO atoms AND contains NO 'INSTRUCTION:' or 'ALIAS RESOLVED:' lines. \
No-data response: "I don't have current KB data for [ticker/topic]. \
The KB currently covers [mention a related ticker or sector if visible in context, \
otherwise say 'check back after the next ingest cycle']. \
I cannot answer from my training knowledge as it may be significantly out of date."

Rules:
1. Reason strictly from the facts in KNOWLEDGE CONTEXT. Never introduce external facts \
or prices from your training data.
2. If a fact has a low confidence score, treat it as tentative and say so.
3. You are an educational tool, NOT a financial adviser. Never make personal \
recommendations to buy, sell, or hold any security. Do not use phrases like \
"you should buy", "I recommend", "consider buying", or "Trading Recommendations". \
If the user asks for a recommendation, redirect by explaining what the KB data shows \
and frame any position discussion as an educational sizing example (see rule 11).
4. When the context contains conflicting signals, surface the conflict rather than \
picking a side.
5. Match response depth to the question. For single-ticker questions, be concise and lead with the most actionable insight. For portfolio-wide questions (when USER PORTFOLIO block is present and the user asks about their portfolio, holdings, or multiple tickers), provide a comprehensive per-holding analysis — cover every holding that has KB data, including price regime, signal direction, returns, and sizing context.
6. Do NOT reproduce metadata tags, stress scores, diagnostic labels, or the \
comment-style section markers (lines beginning with #) in your answer — \
they are internal structure for you to navigate, not content to show the user. \
Write your answer in plain prose using the facts, not the labels.
7. If an atom contains a date or event without associated names or details, cite only \
what the atom states. Do not infer or generate names, people, or specific details \
not present in the context.\
8. NEVER add a currency symbol (£, $, €, ¥ etc.) to a price unless the KB explicitly \
provides a 'currency' atom for that ticker. If currency is USD say 'USD' or '$', \
if GBP say '£'. If the currency atom is absent, quote the number with no symbol \
and note the currency is unconfirmed.\
"""

_SYSTEM_NO_HALLUCINATION = (
    "\n9. Do NOT introduce company names, executive names, news headlines, or any "
    "narrative not present verbatim in the KB atoms. If the KB has no text atoms "
    "for this topic, say so — do not fill the gap with training-data knowledge."
)

_SYSTEM_THIN_COVERAGE = (
    "\n8. IMPORTANT: KB coverage is thin for this topic. "
    "Say so explicitly at the start of your answer and qualify every claim accordingly. "
    "Do not speculate beyond what the context states."
)

_SYSTEM_DIAGNOSIS_SUFFIX = (
    "\n9. The knowledge base has a structural gap ({primary_type}) for this topic. "
    "Acknowledge the gap and indicate what additional data would improve the answer."
)

_SYSTEM_PORTFOLIO_RULE = (
    "\n10. You have access to the user's portfolio in the USER PORTFOLIO block below. "
    "Use it to personalise answers. When the user asks about their portfolio or holdings: "
    "(a) analyse EVERY holding that has KB data — do not skip any; "
    "(b) for each holding: state the current price and price regime, then EXPLAIN IN PLAIN ENGLISH "
    "what the signal direction means for that holding and WHY — e.g. if signal_direction=short and "
    "price_regime=near_52w_high, explain the stock is near its yearly peak and the KB signal suggests "
    "downside pressure; if signal_direction=long and upside_pct is high, explain why that represents "
    "a potential opportunity given the current positioning; "
    "(c) cross-reference macro_confirmation — if unconfirmed, explain that macro conditions do not "
    "yet support the signal and what that means for confidence; if partial or confirmed, say so and why it matters; "
    "(d) for each holding, state what the conviction_tier and signal_quality mean together — "
    "e.g. low conviction + weak quality = high uncertainty, medium conviction + confirmed = higher reliability; "
    "(e) flag concentration risk or sector overlap if visible — all holdings being financials is a risk; "
    "(f) close each holding analysis with a one-sentence plain-English summary of what the KB data suggests. "
    "Do not reveal exact average costs unless the user explicitly asks."
)

_SYSTEM_LIVE_DATA_RULE = (
    "\n12. LIVE DATA block (when present) contains atoms fetched on-demand this session "
    "from live market feeds. These are MORE CURRENT than the KB atoms above. "
    "Prefer live data for price, regime, and direction. "
    "State 'Live data fetched this session' at the start of your answer when using it."
)

_SYSTEM_SIZING_RULE = (
    "\n11. EDUCATIONAL POSITION SIZING: When any instrument is discussed, use the "
    "user's total invested figure from USER PORTFOLIO to illustrate position-sizing "
    "maths as an educational example. Format it as a clearly labelled block:\n"
    "  Educational Example — if you were to allocate X% of your portfolio (£Y) to "
    "<TICKER> at a price of £Z, that would be approximately N shares. With a "
    "stop-loss at £S, the maximum risk would be £R (P% of your portfolio).\n"
    "Always close with: 'This is not financial advice. "
    "Past performance is not indicative of future results.'"
)


def build(
    user_message: str,
    snippet: str,
    stress: Optional[dict] = None,
    kb_diagnosis: Optional[dict] = None,
    prior_context: Optional[str] = None,
    portfolio_context: Optional[str] = None,
    atom_count: int = 0,
    live_context: Optional[str] = None,
    resolved_aliases: Optional[dict] = None,
) -> list[dict]:
    """
    Build the [system, user] message list for Ollama.

    Args:
        user_message:      The user's natural-language question.
        snippet:           The formatted KB context string from retrieve().
        stress:            Dict with composite_stress, decay_pressure, etc.
        kb_diagnosis:      Optional kb_diagnosis block from /retrieve (fires when stressed).
        prior_context:     Optional prior-session state string from working_state.
        portfolio_context: Optional formatted string of user holdings + model.
                           When None the prompt is identical to the no-portfolio case.

    Returns:
        [{"role": "system", "content": ...}, {"role": "user", "content": ...}]
    """
    # ── Build dynamic system prompt ────────────────────────────────────────────
    system_text = _SYSTEM_PROMPT_BASE + _SYSTEM_NO_HALLUCINATION

    composite = 0.0
    if stress:
        composite = stress.get("composite_stress", 0.0)
        if composite >= _STRESS_WARN_THRESHOLD:
            system_text += _SYSTEM_THIN_COVERAGE

    if kb_diagnosis and not resolved_aliases:
        primary = kb_diagnosis.get("primary_type", "unknown")
        conf    = kb_diagnosis.get("confidence", 0.0)
        if primary not in ("unknown", "") and conf > 0.3:
            system_text += _SYSTEM_DIAGNOSIS_SUFFIX.format(primary_type=primary)

    if resolved_aliases:
        for raw, canonical in resolved_aliases.items():
            system_text += (
                f"\nALIAS RESOLVED: The user said '{raw}'. This KB tracks it as '{canonical}'. "
                f"All atoms with subject='{canonical.lower()}' below ARE the '{raw}' data. "
                f"You MUST answer from those atoms. "
                f"DO NOT say you have no data for '{raw}'."
            )

    if live_context:
        system_text += _SYSTEM_LIVE_DATA_RULE

    if portfolio_context:
        system_text += _SYSTEM_PORTFOLIO_RULE
        system_text += _SYSTEM_SIZING_RULE

    # ── User turn ─────────────────────────────────────────────────────────────
    user_parts: list[str] = []

    # Prior session state (cross-session continuity)
    if prior_context:
        user_parts.append(prior_context)

    # KB atom count header — tells LLM explicitly how much context was retrieved
    if atom_count == 0:
        user_parts.append("⚠ KB ATOMS RETRIEVED: 0 — No knowledge context found for this query. You must respond with the no-data message from your CRITICAL rule above.")
    elif atom_count < 5:
        user_parts.append(f"⚠ KB ATOMS RETRIEVED: {atom_count} (thin coverage — qualify all claims)")

    # KB context block
    user_parts.append(snippet if snippet.strip() else "(No KB context available for this query.)")

    # Live on-demand data — injected after KB, before portfolio
    if live_context:
        user_parts.append(live_context)

    # Portfolio context — injected after live data, before the question
    if portfolio_context:
        user_parts.append(portfolio_context)

    # If snippet contains alias instructions, echo them right before the question
    # so they are the last thing the LLM reads before answering
    if snippet and 'is an alias' in snippet:
        import re as _re
        for m in _re.finditer(r"INSTRUCTION: '(\S+)' is an alias.*?Do NOT say you have no data for \S+\.", snippet):
            user_parts.append(m.group(0))

    # User question — always last so the LLM sees context then question
    user_parts.append(f"Question: {user_message}")

    return [
        {"role": "system", "content": system_text},
        {"role": "user",   "content": "\n\n".join(user_parts)},
    ]
