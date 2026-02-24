"""
tests/test_full_stack.py — Comprehensive full-stack test suite
Run: python -m pytest tests/test_full_stack.py -v --tb=short
Requires: pip install requests pytest
Server:   http://127.0.0.1:5050
"""

import uuid
import pytest
import requests

BASE = "http://127.0.0.1:5050"

def get(path, **kw):    return requests.get(f"{BASE}{path}", **kw)
def post(path, body=None, **kw): return requests.post(f"{BASE}{path}", json=body, **kw)

def check(r, status=200, keys=(), label=""):
    tag = f"[{label}] " if label else ""
    assert r.status_code == status, f"{tag}HTTP {r.status_code}: {r.text[:300]}"
    d = r.json()
    for k in keys:
        assert k in d, f"{tag}Missing '{k}' in {list(d.keys())}"
    return d


# ─── 1. INFRASTRUCTURE ───────────────────────────────────────────────────────

class TestInfrastructure:
    def test_health(self):
        d = check(get("/health"), keys=["status","db"], label="health")
        assert d["status"] == "ok"

    def test_stats_all_keys(self):
        d = check(get("/stats"), keys=[
            "total_facts","unique_subjects","unique_predicates",
            "total_conflicts_detected","pending_repair_proposals",
            "domain_refresh_queue_depth","adaptation_sessions_active",
            "adaptation_sessions_total","kb_insufficient_events_7d",
        ], label="stats")
        assert d["total_facts"] >= 0

    def test_stats_top_retrieved_atoms(self):
        d = check(get("/stats"), label="stats-top")
        for a in d.get("top_retrieved_atoms", []):
            assert "subject" in a and "predicate" in a and "hits" in a

    def test_ingest_status_all_adapters(self):
        d = check(get("/ingest/status"), keys=["scheduler","adapters"], label="ingest-status")
        assert d["scheduler"] == "running"
        for name in ("yfinance","rss_news","fred","edgar"):
            assert name in d["adapters"], f"Adapter '{name}' missing"
            s = d["adapters"][name]
            assert "total_runs" in s and "is_running" in s


# ─── 2. INGEST ───────────────────────────────────────────────────────────────

class TestIngest:
    SUBJ = f"ts_{uuid.uuid4().hex[:8]}"

    def test_single_atom(self):
        d = check(post("/ingest", {"subject": self.SUBJ, "predicate": "signal_direction",
                                    "object": "long", "confidence": 0.85,
                                    "source": "test_suite"}),
                  keys=["ingested"], label="ingest-single")
        assert d["ingested"] >= 1

    def test_batch_atoms(self):
        d = check(post("/ingest", {"atoms": [
            {"subject": self.SUBJ, "predicate": "price_target",  "object": "999", "confidence": 0.9,  "source": "test_suite"},
            {"subject": self.SUBJ, "predicate": "sector",        "object": "tech","confidence": 0.95, "source": "test_suite"},
            {"subject": self.SUBJ, "predicate": "market_cap_tier","object": "mega","confidence": 0.8,  "source": "test_suite"},
        ]}), keys=["ingested"], label="ingest-batch")
        assert d["ingested"] >= 1

    def test_duplicate_is_no_op(self):
        atom = {"subject": self.SUBJ, "predicate": "signal_direction",
                "object": "long", "confidence": 0.85, "source": "test_suite"}
        post("/ingest", atom)
        r = post("/ingest", atom)
        assert r.status_code == 200

    def test_missing_predicate_returns_400(self):
        r = post("/ingest", {"subject": "x", "object": "y"})
        assert r.status_code == 400

    def test_empty_body_returns_400(self):
        r = requests.post(f"{BASE}/ingest", data="", headers={"Content-Type":"application/json"})
        assert r.status_code == 400

    def test_verify_via_query(self):
        subj = f"v_{uuid.uuid4().hex[:6]}"
        post("/ingest", {"subject": subj, "predicate": "test_pred",
                         "object": "val", "confidence": 0.7, "source": "test_suite"})
        d = check(get("/query", params={"subject": subj}), label="verify-query")
        assert any(r["predicate"] == "test_pred" for r in d["results"]), \
            "Ingested atom not found via /query"


# ─── 3. RETRIEVAL ────────────────────────────────────────────────────────────

class TestRetrieval:
    SID = "test_retr_session"

    def _r(self, msg, **kw):
        return check(post("/retrieve", {"message": msg, "session_id": self.SID, **kw}),
                     keys=["snippet","atoms","stress"], label=f"retr:{msg[:35]}")

    def test_price_target_boost(self):
        d = self._r("NVDA META GOOGL upside price target analyst consensus")
        preds = [a["predicate"] for a in d["atoms"]]
        assert "price_target" in preds or "price_target" in d["snippet"]

    def test_graph_snippet_for_relational(self):
        d = self._r("How does Fed policy affect tech stocks through yield sensitivity")
        assert "KNOWLEDGE GRAPH" in d["snippet"] or "CENTRAL CONCEPTS" in d["snippet"]

    def test_cross_asset_query(self):
        d = self._r("Compare sector ETFs XLF XLE XLK XLV XLI")
        assert len(d["atoms"]) >= 1

    def test_sector_predicate_surfaced(self):
        d = self._r("XLF XLE XLK XLV XLI sector classification volatility")
        preds = [a["predicate"] for a in d["atoms"]]
        assert "sector" in preds or "signal_direction" in preds

    def test_macro_predicates_surfaced(self):
        d = self._r("What is the current macro regime and Fed stance?")
        preds = set(a["predicate"] for a in d["atoms"])
        macro = {"regime_label","central_bank_stance","dominant_driver",
                 "inflation_environment","growth_environment"}
        assert macro & preds, f"No macro predicates in: {preds}"

    def test_long_signals_query(self):
        d = self._r("Top long signals across the watchlist")
        preds = [a["predicate"] for a in d["atoms"]]
        assert "signal_direction" in preds

    def test_direct_ticker_nvda(self):
        d = self._r("NVDA latest price and volatility regime")
        subjects = {a["subject"].upper() for a in d["atoms"]}
        assert any("NVDA" in s for s in subjects), f"NVDA missing from {subjects}"

    def test_macro_proxies_gld_slv_uup(self):
        d = self._r("Gold silver dollar inflation signal GLD SLV UUP")
        subjects = {a["subject"].upper() for a in d["atoms"]}
        assert subjects & {"GLD","SLV","UUP"}, f"Macro proxies missing: {subjects}"

    def test_tlt_hyg_rates(self):
        d = self._r("Treasury yields TLT HYG credit duration signal")
        subjects = {a["subject"].upper() for a in d["atoms"]}
        assert subjects & {"TLT","HYG"}, f"TLT/HYG missing: {subjects}"

    def test_stress_block_all_fields(self):
        d = self._r("AAPL MSFT sector signals")
        for k in ("composite_stress","decay_pressure","authority_conflict",
                  "supersession_density","conflict_cluster","domain_entropy"):
            assert k in d["stress"], f"stress.{k} missing"
        assert 0.0 <= d["stress"]["composite_stress"] <= 1.0

    def test_stress_values_bounded(self):
        d = self._r("macro regime inflation")
        for k, v in d["stress"].items():
            assert 0.0 <= float(v) <= 1.0, f"stress.{k}={v} out of [0,1]"

    def test_snippet_has_headers(self):
        d = self._r("NVDA META long upside price target")
        assert "===" in d["snippet"] or "[" in d["snippet"]

    def test_limit_param(self):
        d = check(post("/retrieve", {"message":"tech stocks","session_id":self.SID,"limit":5}),
                  keys=["atoms"], label="retr-limit")
        assert len(d["atoms"]) <= 5

    def test_topic_param(self):
        d = check(post("/retrieve", {"message":"AAPL signals","session_id":self.SID,"topic":"AAPL"}),
                  keys=["snippet","atoms"], label="retr-topic")
        assert d["snippet"] != ""

    def test_kb_diagnosis_shape_when_present(self):
        d = self._r("vanadium redox battery exotic arbitrage obscure")
        if "kb_diagnosis" in d:
            kb = d["kb_diagnosis"]
            for k in ("topic","types","primary_type","confidence","matched_rules","signals"):
                assert k in kb, f"kb_diagnosis.{k} missing"
            assert 0.0 <= kb["confidence"] <= 1.0


# ─── 4. GOVERNANCE — DIAGNOSE & PROPOSALS ───────────────────────────────────

VALID_TYPES = {
    "coverage_gap","representation_inconsistency","authority_imbalance",
    "semantic_duplication","granularity_too_fine","missing_schema",
    "domain_boundary_collapse","semantic_incoherence","cross_topic_drift","unknown",
}

class TestGovernance:
    def test_diagnose_nvda(self):
        d = check(post("/repair/diagnose", {"topic":"nvda"}),
                  keys=["topic","types","primary_type","confidence","matched_rules","total_rules","signals"],
                  label="diagnose-nvda")
        assert d["total_rules"] == 9
        assert d["primary_type"] in VALID_TYPES

    def test_diagnose_sparse_topic(self):
        d = check(post("/repair/diagnose", {"topic":"completely_unknown_xyz_987"}),
                  keys=["signals"], label="diagnose-sparse")
        assert d["signals"]["atom_count"] == 0.0

    def test_diagnose_macro(self):
        d = check(post("/repair/diagnose", {"topic":"us_macro"}),
                  keys=["types","signals"], label="diagnose-macro")
        assert len(d["types"]) >= 1

    def test_diagnose_empty_topic_400(self):
        assert post("/repair/diagnose", {"topic":""}).status_code == 400

    def test_diagnose_missing_field_400(self):
        assert post("/repair/diagnose", {}).status_code == 400

    def test_proposals_shape(self):
        d = check(post("/repair/proposals", {"topic":"nvda"}),
                  keys=["topic","diagnosis","proposals"], label="proposals-nvda")
        if d["proposals"]:
            p = d["proposals"][0]
            for k in ("id","strategy","description","simulation","preview","validation"):
                assert k in p, f"proposals[0].{k} missing"

    def test_proposals_has_primary(self):
        d = check(post("/repair/proposals", {"topic":"us_macro"}), label="proposals-primary")
        props = d.get("proposals", [])
        if props:
            assert any(p.get("is_primary") for p in props), "No is_primary proposal"

    def test_proposals_empty_topic_400(self):
        assert post("/repair/proposals", {"topic":""}).status_code == 400

    def test_repair_impact(self):
        d = check(get("/repair/impact"), label="repair-impact")
        assert isinstance(d, dict)

    def test_repair_impact_strategy_filter(self):
        assert get("/repair/impact", params={"strategy":"ingest_missing"}).status_code == 200

    def test_execute_unknown_proposal(self):
        r = post("/repair/execute", {"proposal_id": str(uuid.uuid4()), "dry_run": True})
        assert r.status_code in (200, 500)
        assert "error" in r.json() or "success" in r.json()

    def test_execute_missing_id_400(self):
        assert post("/repair/execute", {"dry_run": True}).status_code == 400

    def test_rollback_missing_id_400(self):
        assert post("/repair/rollback", {}).status_code == 400

    def test_execute_dry_run_real_proposal(self):
        d = post("/repair/proposals", {"topic":"nvda"}).json()
        props = d.get("proposals", [])
        if not props:
            pytest.skip("No proposals for nvda")
        r = post("/repair/execute", {"proposal_id": props[0]["id"], "dry_run": True})
        assert r.status_code == 200
        result = r.json()
        assert "dry_run" in result or "success" in result or "error" in result


# ─── 5. GRAPH LAYER ──────────────────────────────────────────────────────────

class TestGraphLayer:
    def test_kb_graph_structure(self):
        d = check(post("/kb/graph", {"message":"How does Fed policy affect tech stocks?"}),
                  keys=["graph_context","atom_count"], label="kb-graph")
        assert len(d["graph_context"]) > 50

    def test_kb_graph_has_pagerank(self):
        d = check(post("/kb/graph", {"message":"macro regime inflation fed rate"}),
                  keys=["graph_context"], label="kb-graph-pr")
        ctx = d["graph_context"]
        assert any(kw in ctx for kw in ("CENTRAL CONCEPTS","KNOWLEDGE GRAPH","PageRank"))

    def test_kb_graph_empty_message_400(self):
        assert post("/kb/graph", {}).status_code == 400

    def test_kb_traverse(self):
        d = check(post("/kb/traverse", {"topic":"us_macro"}),
                  keys=["topic","traversal","atom_count"], label="kb-traverse")
        assert isinstance(d["traversal"], str)

    def test_kb_traverse_sparse(self):
        d = check(post("/kb/traverse", {"topic":"totally_unknown_xyz_123"}),
                  keys=["traversal"], label="kb-traverse-sparse")
        assert isinstance(d["traversal"], str)

    def test_kb_traverse_empty_topic_400(self):
        assert post("/kb/traverse", {}).status_code == 400

    def test_kb_conflicts_shape(self):
        d = check(get("/kb/conflicts"), keys=["count","conflicts"], label="kb-conflicts")
        assert len(d["conflicts"]) == d["count"]

    def test_kb_conflicts_subject_filter(self):
        d = check(get("/kb/conflicts", params={"subject":"nvda","limit":10}),
                  keys=["count","conflicts"], label="kb-conflicts-filter")
        assert isinstance(d["conflicts"], list)

    def test_kb_refresh_queue_shape(self):
        d = check(get("/kb/refresh-queue"), label="kb-rq")
        for k in ("domain_refresh_queue","synthesis_queue","kb_insufficient_log"):
            assert k in d, f"refresh-queue.{k} missing"

    def test_kb_refresh_queue_processed(self):
        assert get("/kb/refresh-queue", params={"processed":1}).status_code == 200


# ─── 6. ADAPTATION LOOP ──────────────────────────────────────────────────────

class TestAdaptation:
    SID = f"adapt_{uuid.uuid4().hex[:8]}"

    def test_status_new_session(self):
        d = check(get("/adapt/status", params={"session_id": self.SID}),
                  keys=["session_id","streak","last_stress"], label="adapt-new")
        assert d["streak"] == 0
        assert d["last_stress"] == 0.0

    def test_status_all_sessions(self):
        assert isinstance(check(get("/adapt/status"), label="adapt-all"), dict)

    def test_reset(self):
        post("/retrieve", {"message":"NVDA signals","session_id": self.SID})
        d = check(post("/adapt/reset", {"session_id": self.SID}),
                  keys=["session_id","reset"], label="adapt-reset")
        assert d["reset"] is True
        after = check(get("/adapt/status", params={"session_id": self.SID}), label="adapt-post-reset")
        assert after["streak"] == 0

    def test_streak_is_integer(self):
        sid = f"streak_{uuid.uuid4().hex[:8]}"
        for _ in range(4):
            post("/retrieve", {"message":"macro regime fed stance","session_id":sid})
        s = check(get("/adapt/status", params={"session_id":sid}),
                  keys=["streak","last_stress"], label="adapt-streak")
        assert isinstance(s["streak"], int) and s["streak"] >= 0
        assert 0.0 <= s["last_stress"] <= 1.0

    def test_adaptation_block_shape_if_present(self):
        d = post("/retrieve", {"message":"NVDA META signals","session_id": self.SID}).json()
        if "adaptation" in d:
            for k in ("streak","consolidation_mode","retrieval_scope_broadened",
                      "prefer_high_authority","prefer_recent"):
                assert k in d["adaptation"], f"adaptation.{k} missing"


# ─── 7. DIRECT QUERY / SEARCH / CONTEXT ──────────────────────────────────────

class TestDirectQuery:
    def test_query_subject(self):
        d = check(get("/query", params={"subject":"nvda"}),
                  keys=["results","count"], label="query-subj")
        assert d["count"] == len(d["results"])

    def test_query_predicate(self):
        d = check(get("/query", params={"predicate":"signal_direction"}),
                  keys=["results","count"], label="query-pred")
        assert d["count"] >= 0

    def test_query_combined(self):
        d = check(get("/query", params={"subject":"nvda","predicate":"signal_direction"}),
                  keys=["results"], label="query-combo")
        assert isinstance(d["results"], list)

    def test_query_limit(self):
        d = check(get("/query", params={"predicate":"last_price","limit":3}),
                  keys=["results"], label="query-limit")
        assert len(d["results"]) <= 3

    def test_search_basic(self):
        d = check(get("/search", params={"q":"fed interest rate"}),
                  keys=["results","count"], label="search-basic")
        assert isinstance(d["results"], list)

    def test_search_limit(self):
        d = check(get("/search", params={"q":"nvda","limit":5}),
                  keys=["results"], label="search-limit")
        assert len(d["results"]) <= 5

    def test_search_no_q_400(self):
        assert get("/search").status_code == 400

    def test_context_entity(self):
        d = check(get("/context/nvda"),
                  keys=["entity","facts","count"], label="context-nvda")
        assert d["entity"] == "nvda"
        assert d["count"] == len(d["facts"])

    def test_context_unknown(self):
        d = check(get("/context/totally_unknown_xyz_9999"),
                  keys=["entity","facts","count"], label="context-unknown")
        assert d["count"] >= 0


# ─── 8. EDGE CASES ───────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_retrieve_empty_message(self):
        d = check(post("/retrieve", {"message":""}),
                  keys=["snippet","atoms"], label="edge-empty-msg")
        assert isinstance(d["atoms"], list)

    def test_retrieve_malformed_json(self):
        r = requests.post(f"{BASE}/retrieve", data="not-json",
                          headers={"Content-Type":"application/json"})
        assert r.status_code in (400, 200)

    def test_retrieve_very_long_message(self):
        d = check(post("/retrieve", {"message": "AAPL MSFT NVDA " * 60}),
                  keys=["snippet","atoms"], label="edge-long")
        assert isinstance(d["atoms"], list)

    def test_query_no_params(self):
        d = check(get("/query"), keys=["results","count"], label="edge-query-noparams")
        assert isinstance(d["results"], list)

    def test_diagnose_empty_topic_400(self):
        assert post("/repair/diagnose", {"topic":""}).status_code == 400

    def test_proposals_empty_topic_400(self):
        assert post("/repair/proposals", {"topic":""}).status_code == 400

    def test_kb_graph_empty_message_400(self):
        assert post("/kb/graph", {}).status_code == 400

    def test_kb_traverse_empty_topic_400(self):
        assert post("/kb/traverse", {}).status_code == 400

    def test_ingest_null_confidence(self):
        r = post("/ingest", {"subject":"test_null","predicate":"p","object":"v","source":"t"})
        assert r.status_code in (200, 400)

    def test_retrieve_unicode_message(self):
        d = check(post("/retrieve", {"message":"联储 利率 通货膨胀 宏观 macro regime"}),
                  keys=["snippet","atoms"], label="edge-unicode")
        assert isinstance(d["atoms"], list)

    def test_search_special_chars(self):
        d = check(get("/search", params={"q":"NVDA & (fed OR macro)"}),
                  keys=["results"], label="edge-search-special")
        assert isinstance(d["results"], list)
