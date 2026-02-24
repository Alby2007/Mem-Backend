"""
Smoke test: LLM extraction adapter — unit tests only, no Ollama required.

Tests:
  - _parse_llm_atoms: valid JSON, markdown-fenced JSON, prose-wrapped JSON,
    invalid JSON, empty array, bad predicate filtering, subject constraint
    (invalid subjects filtered, valid ticker+macro pass),
    confidence always equals fallback_confidence (LLM field ignored),
    signal_direction value validation
  - _confidence_from_language: hedge-word calibration
  - _build_prompt: structure, content, no confidence field in schema
  - _ensure_extraction_queue: table creation idempotency
  - Adapter fetch() graceful degradation when Ollama unreachable

Run: python tests/smoke_llm_extraction.py
Expected: ALL CHECKS PASSED
"""
import sys, sqlite3, os, json, tempfile; sys.path.insert(0, '.')

from ingest.llm_extraction_adapter import (
    _parse_llm_atoms, _confidence_from_language, _build_prompt, MAX_FAILURES,
    _VALID_MACRO_ENTITIES,
)
from ingest.rss_adapter import _ensure_extraction_queue

ERRORS = []

def ok(label):
    print(f"  OK  [{label}]")

def fail(label, msg):
    ERRORS.append(f"FAIL [{label}] {msg}")
    print(f"  FAIL [{label}] {msg}")

def check(label, got, expected):
    if got != expected:
        fail(label, f"got={got!r} expected={expected!r}")
    else:
        ok(label)

def check_approx(label, got, expected, tol=0.01):
    try:
        if abs(float(got) - float(expected)) <= tol:
            ok(label)
        else:
            fail(label, f"got={got} expected≈{expected}")
    except (TypeError, ValueError) as e:
        fail(label, str(e))

NOW = "2026-02-24T00:00:00+00:00"
SRC = "llm_extracted_news_wire_cnbc"

print("\n=== LLM Extraction Smoke Test ===\n")

# ── 1. _parse_llm_atoms: valid JSON ──────────────────────────────────────────
print("[1] Valid JSON response")
FALLBACK = 0.65
resp = json.dumps([
    {"subject": "NVDA", "predicate": "catalyst", "object": "q4_earnings_beat", "reasoning": "beat Q4"},
    {"subject": "fed", "predicate": "forward_guidance", "object": "two_cuts_2026", "reasoning": "signals"},
])
atoms, success = _parse_llm_atoms(resp, SRC, FALLBACK, NOW)
check("valid_json.success", success, True)
check("valid_json.count", len(atoms), 2)
check("valid_json.subject_0", atoms[0].subject, "nvda")
check("valid_json.predicate_0", atoms[0].predicate, "catalyst")
check("valid_json.object_0", atoms[0].object, "q4_earnings_beat")
# Confidence is always fallback_confidence — LLM field is ignored
check_approx("valid_json.confidence_uses_fallback", atoms[0].confidence, FALLBACK)
check("valid_json.subject_1", atoms[1].subject, "fed")
check("valid_json.predicate_1", atoms[1].predicate, "forward_guidance")

# ── 2. Markdown-fenced JSON ───────────────────────────────────────────────────
print("\n[2] Markdown-fenced JSON (model wraps in ```json)")
resp = '```json\n[{"subject": "AAPL", "predicate": "rating_change", "object": "upgraded_to_buy", "reasoning": "analyst upgrade"}]\n```'
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("fenced.success", success, True)
check("fenced.count", len(atoms), 1)
check("fenced.predicate", atoms[0].predicate, "rating_change")

# ── 3. Prose-wrapped JSON ─────────────────────────────────────────────────────
print("\n[3] Prose-wrapped JSON (model adds explanation before the array)")
resp = 'Here are the extracted facts:\n[{"subject": "MSFT", "predicate": "key_finding", "object": "azure_growth_beat", "reasoning": "beat"}]'
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("prose_wrap.success", success, True)
check("prose_wrap.count", len(atoms), 1)

# ── 4. Empty array ────────────────────────────────────────────────────────────
print("\n[4] Empty array []")
atoms, success = _parse_llm_atoms("[]", SRC, 0.65, NOW)
check("empty.success", success, True)
check("empty.count", len(atoms), 0)

# ── 5. Invalid JSON ───────────────────────────────────────────────────────────
print("\n[5] Malformed JSON")
atoms, success = _parse_llm_atoms("not json at all", SRC, 0.65, NOW)
check("invalid_json.success", success, False)
check("invalid_json.count", len(atoms), 0)

# ── 6. Empty/None response ────────────────────────────────────────────────────
print("\n[6] None / empty response")
atoms, success = _parse_llm_atoms(None, SRC, 0.65, NOW)
check("none_resp.success", success, False)
atoms, success = _parse_llm_atoms("", SRC, 0.65, NOW)
check("empty_str_resp.success", success, False)

# ── 7. Invalid predicate filtered out ─────────────────────────────────────────
print("\n[7] Invalid predicate filtered out")
resp = json.dumps([
    {"subject": "TSLA", "predicate": "invalid_predicate", "object": "some_fact", "confidence": 0.70},
    {"subject": "TSLA", "predicate": "catalyst", "object": "robo_taxi_launch", "confidence": 0.70},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("bad_pred.success", success, True)
check("bad_pred.count", len(atoms), 1)
check("bad_pred.predicate", atoms[0].predicate, "catalyst")

# ── 8. signal_direction value validation ─────────────────────────────────────
print("\n[8] signal_direction value validation")
resp = json.dumps([
    {"subject": "AMD", "predicate": "signal_direction", "object": "long", "confidence": 0.72},
    {"subject": "AMD", "predicate": "signal_direction", "object": "definitely_buy_now", "confidence": 0.72},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("sig_dir.success", success, True)
check("sig_dir.count", len(atoms), 1)
check("sig_dir.object", atoms[0].object, "long")

# ── 9. Confidence always uses fallback (LLM confidence field ignored) ─────────
print("\n[9] Confidence always equals fallback_confidence regardless of LLM field")
FB = 0.72
resp = json.dumps([
    {"subject": "GS", "predicate": "key_finding", "object": "deal_announced", "confidence": 0.99},
    {"subject": "GS", "predicate": "key_finding", "object": "deal_rumoured", "confidence": 0.10},
])
atoms, success = _parse_llm_atoms(resp, SRC, FB, NOW)
check("conf_fallback.count", len(atoms), 2)
check_approx("conf_fallback.atom0", atoms[0].confidence, FB)
check_approx("conf_fallback.atom1", atoms[1].confidence, FB)

# ── 10. Object truncated at 250 chars ─────────────────────────────────────────
print("\n[10] Object truncated at 250 chars")
long_obj = "x" * 300
resp = json.dumps([{"subject": "JPM", "predicate": "key_finding", "object": long_obj, "confidence": 0.65}])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("trunc.len", len(atoms[0].object), 250)

# ── 11. Missing required fields skipped ───────────────────────────────────────
print("\n[11] Missing subject/predicate/object skipped")
resp = json.dumps([
    {"subject": "", "predicate": "catalyst", "object": "something", "confidence": 0.70},
    {"subject": "AAPL", "predicate": "", "object": "something", "confidence": 0.70},
    {"subject": "AAPL", "predicate": "catalyst", "object": "", "confidence": 0.70},
    {"subject": "AAPL", "predicate": "catalyst", "object": "valid_fact", "confidence": 0.70},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("missing_fields.count", len(atoms), 1)

# ── 12. _confidence_from_language ─────────────────────────────────────────────
print("\n[12] _confidence_from_language")
check_approx("lang.high",   _confidence_from_language("NVDA announced Q4 earnings beat"), 0.80)
check_approx("lang.medium", _confidence_from_language("Fed signals two rate cuts in 2026"), 0.70)
check_approx("lang.low",    _confidence_from_language("Apple may consider acquisition"), 0.58)
check_approx("lang.default",_confidence_from_language("Market update for today"), 0.65)

# ── 13. _build_prompt structure ───────────────────────────────────────────────
print("\n[13] _build_prompt structure")
msgs = _build_prompt("Test article about NVDA earnings")
check("prompt.count", len(msgs), 2)
check("prompt.roles", [m['role'] for m in msgs], ['system', 'user'])
assert 'catalyst' in msgs[0]['content'], "system prompt missing predicate list"
assert 'NVDA' in msgs[1]['content'], "user prompt missing article text"
ok("prompt.system_has_predicates")
ok("prompt.user_has_text")
# confidence must NOT appear in the schema — we don't want LLM hallucinating it
assert 'confidence' not in msgs[0]['content'], "system prompt should not request confidence field"
ok("prompt.no_confidence_field")
# subject constraint must be explicit in the prompt
assert 'fed' in msgs[0]['content'], "system prompt missing macro entity list"
ok("prompt.has_macro_entity_list")

# Text truncated to 800 chars
long_text = "x" * 1000
msgs = _build_prompt(long_text)
assert len(msgs[1]['content']) < 1200, "prompt not truncating long text"
ok("prompt.truncates_long_text")

# ── 14. _ensure_extraction_queue idempotency ──────────────────────────────────
print("\n[14] _ensure_extraction_queue idempotency")
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    tmp_db = f.name
try:
    conn = sqlite3.connect(tmp_db)
    _ensure_extraction_queue(conn)
    _ensure_extraction_queue(conn)  # must not raise
    c = conn.execute("PRAGMA table_info(extraction_queue)")
    cols = {row[1] for row in c.fetchall()}
    conn.close()
    for col in ('id', 'text', 'url', 'source', 'fetched_at', 'processed',
                'processed_at', 'atoms_extracted', 'failed_attempts'):
        if col not in cols:
            fail(f"queue_schema.{col}", f"column missing from extraction_queue")
        else:
            ok(f"queue_schema.{col}")
finally:
    os.unlink(tmp_db)

# ── 15. Adapter graceful degradation when Ollama unreachable ─────────────────
print("\n[15] Adapter graceful degradation (Ollama unreachable)")
with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    tmp_db2 = f.name
try:
    from unittest.mock import patch
    from ingest.llm_extraction_adapter import LLMExtractionAdapter
    adapter = LLMExtractionAdapter(db_path=tmp_db2)
    with patch('ingest.llm_extraction_adapter.LLMExtractionAdapter.fetch',
               wraps=adapter.fetch) as _:
        # Patch is_available to return False
        import ingest.llm_extraction_adapter as _mod
        orig = None
        try:
            from llm import ollama_client as _oc
            orig = _oc.is_available
            _oc.is_available = lambda: False
            result = adapter.fetch()
            check("degradation.returns_empty_list", result, [])
        except ImportError:
            ok("degradation.no_llm_module_import_skipped")
        finally:
            if orig is not None:
                _oc.is_available = orig
finally:
    os.unlink(tmp_db2)

# ── 16. Subject constraint — invalid subjects filtered ───────────────────────
print("\n[16] Subject constraint — invalid subjects filtered")
resp = json.dumps([
    {"subject": "none",     "predicate": "key_finding", "object": "some_fact"},
    {"subject": "tariffs",  "predicate": "risk_factor",  "object": "trade_war"},
    {"subject": "global",   "predicate": "regime_label", "object": "uncertainty"},
    {"subject": "individual companies/sectors", "predicate": "key_finding", "object": "impacted"},
    {"subject": "NVDA",     "predicate": "catalyst",    "object": "valid_ticker"},
    {"subject": "fed",      "predicate": "forward_guidance", "object": "valid_macro"},
    {"subject": "us_macro", "predicate": "regime_label", "object": "valid_macro2"},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("subj_constraint.success", success, True)
check("subj_constraint.count", len(atoms), 3)  # only NVDA, fed, us_macro survive
check("subj_constraint.ticker",  atoms[0].subject, "nvda")
check("subj_constraint.macro1",  atoms[1].subject, "fed")
check("subj_constraint.macro2",  atoms[2].subject, "us_macro")

# ── 17. _VALID_MACRO_ENTITIES set is correct ──────────────────────────────────
print("\n[17] _VALID_MACRO_ENTITIES content")
for entity in ('fed', 'ecb', 'treasury', 'us_macro', 'us_labor', 'us_yields', 'us_credit'):
    if entity not in _VALID_MACRO_ENTITIES:
        fail(f"macro_entities.{entity}", "missing from _VALID_MACRO_ENTITIES")
    else:
        ok(f"macro_entities.{entity}")

# ── Summary ───────────────────────────────────────────────────────────────────
print()
if ERRORS:
    print("=" * 60)
    print(f"FAILED — {len(ERRORS)} error(s):")
    for e in ERRORS:
        print(f"  {e}")
    sys.exit(1)
else:
    print("=" * 60)
    print("ALL CHECKS PASSED")
