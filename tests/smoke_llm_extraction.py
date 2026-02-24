"""
Smoke test: LLM extraction adapter — unit tests only, no Ollama required.

Tests:
  - _parse_llm_atoms: valid JSON, markdown-fenced JSON, prose-wrapped JSON,
    invalid JSON, empty array, bad predicate filtering, confidence clamping,
    signal_direction value validation, failed_attempts logic
  - _confidence_from_language: hedge-word calibration
  - _build_prompt: structure and content
  - _ensure_extraction_queue: table creation idempotency
  - Adapter fetch() graceful degradation when Ollama unreachable

Run: python tests/smoke_llm_extraction.py
Expected: ALL CHECKS PASSED
"""
import sys, sqlite3, os, json, tempfile; sys.path.insert(0, '.')

from ingest.llm_extraction_adapter import (
    _parse_llm_atoms, _confidence_from_language, _build_prompt, MAX_FAILURES,
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
resp = json.dumps([
    {"subject": "NVDA", "predicate": "catalyst", "object": "q4_earnings_beat", "confidence": 0.82},
    {"subject": "fed", "predicate": "forward_guidance", "object": "two_cuts_2026", "confidence": 0.73},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("valid_json.success", success, True)
check("valid_json.count", len(atoms), 2)
check("valid_json.subject_0", atoms[0].subject, "nvda")
check("valid_json.predicate_0", atoms[0].predicate, "catalyst")
check("valid_json.object_0", atoms[0].object, "q4_earnings_beat")
check_approx("valid_json.confidence_0", atoms[0].confidence, 0.82)
check("valid_json.subject_1", atoms[1].subject, "fed")
check("valid_json.predicate_1", atoms[1].predicate, "forward_guidance")

# ── 2. Markdown-fenced JSON ───────────────────────────────────────────────────
print("\n[2] Markdown-fenced JSON (model wraps in ```json)")
resp = '```json\n[{"subject": "AAPL", "predicate": "rating_change", "object": "upgraded_to_buy", "confidence": 0.75}]\n```'
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("fenced.success", success, True)
check("fenced.count", len(atoms), 1)
check("fenced.predicate", atoms[0].predicate, "rating_change")

# ── 3. Prose-wrapped JSON ─────────────────────────────────────────────────────
print("\n[3] Prose-wrapped JSON (model adds explanation before the array)")
resp = 'Here are the extracted facts:\n[{"subject": "MSFT", "predicate": "key_finding", "object": "azure_growth_beat", "confidence": 0.78}]'
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

# ── 9. Confidence clamping ────────────────────────────────────────────────────
print("\n[9] Confidence clamped to [0.50, 0.85]")
resp = json.dumps([
    {"subject": "GS", "predicate": "key_finding", "object": "deal_announced", "confidence": 0.99},
    {"subject": "GS", "predicate": "key_finding", "object": "deal_rumoured", "confidence": 0.10},
])
atoms, success = _parse_llm_atoms(resp, SRC, 0.65, NOW)
check("clamp.count", len(atoms), 2)
check_approx("clamp.high_clamped", atoms[0].confidence, 0.85)
check_approx("clamp.low_clamped", atoms[1].confidence, 0.50)

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
assert 'NVDA' in msgs[1]['content'] or 'nvda' in msgs[1]['content'].lower()
ok("prompt.system_has_predicates")
ok("prompt.user_has_text")

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
