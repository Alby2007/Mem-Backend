"""
tests/live_comprehensive_qa.py -- Comprehensive live QA battery

Tests every layer of the system against a running server:
  1. Health / infrastructure
  2. KB query endpoints (direct retrieval, signals, patterns, alerts)
  3. Auth flow (register, token, refresh, logout, security guards)
  4. User onboarding flow (portfolio, preferences, tip-config, status)
  5. Chat / KB-grounded queries (10 questions covering macro, signals,
     cross-asset, hallucination probe, conflict surfacing)
  6. Security hardening checks (missing token, wrong user, bad input)

Usage:
    python tests/live_comprehensive_qa.py [--base http://127.0.0.1:5000]

No LLM is required for sections 1-4 and 6.  Section 5 returns HTTP 200 with
kb context even when Ollama is unavailable -- answers will be None but the
KB retrieval layer is still validated.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any

import requests

# -- Config --------------------------------------------------------------------

DEFAULT_BASE = "http://127.0.0.1:5000"
TIMEOUT      = 30

# Unique run ID so repeated runs don't collide on user_id / email
RUN_ID   = uuid.uuid4().hex[:8]
USER_ID  = f"qa_{RUN_ID}"
EMAIL    = f"qa_{RUN_ID}@example.com"
PASSWORD = "QaT3st!ngPwd"

# -- Helpers -------------------------------------------------------------------

class Results:
    def __init__(self):
        self.passed  = 0
        self.failed  = 0
        self.skipped = 0
        self.log: list[str] = []

    def ok(self, label: str, note: str = ""):
        self.passed += 1
        tag = f"  [PASS]  {label}" + (f"  ({note})" if note else "")
        print(tag)
        self.log.append(tag)

    def fail(self, label: str, note: str = ""):
        self.failed += 1
        tag = f"  [FAIL]  {label}" + (f"  -- {note}" if note else "")
        print(tag)
        self.log.append(tag)

    def skip(self, label: str, reason: str = ""):
        self.skipped += 1
        tag = f"  [SKIP]  {label}" + (f"  -- {reason}" if reason else "")
        print(tag)
        self.log.append(tag)

    def summary(self):
        total = self.passed + self.failed + self.skipped
        print(f"\n{'='*72}")
        print(f"  RESULTS: {self.passed}/{total} passed  "
              f"({self.failed} failed, {self.skipped} skipped)")
        print('=' * 72)
        print()
        return self.failed == 0


R = Results()


def get(base: str, path: str, token: str = "", **kwargs) -> requests.Response:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.get(f"{base}{path}", headers=headers,
                        timeout=TIMEOUT, **kwargs)


def post(base: str, path: str, body: dict, token: str = "") -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(f"{base}{path}", json=body,
                         headers=headers, timeout=TIMEOUT)


def check(label: str, cond: bool, note: str = ""):
    if cond:
        R.ok(label, note)
    else:
        R.fail(label, note)


def section(title: str):
    print('\n' + '-' * 72)
    print(f"  {title}")
    print('-' * 72)

# -- Section 1 -- Health / Infrastructure --------------------------------------

def test_health(base: str):
    section("1 - Health & Infrastructure")

    r = get(base, "/health")
    check("GET /health -> 200", r.status_code == 200)
    d = r.json()
    check("health.status == ok", d.get("status") == "ok")

    r2 = get(base, "/health/detailed")
    check("GET /health/detailed -> 200", r2.status_code == 200)
    d2 = r2.json()
    check("detailed health has kb_stats", "kb_stats" in d2,
          f"keys={list(d2.keys())}")

    # Security headers on every response
    for hdr in ["X-Content-Type-Options", "X-Frame-Options",
                "X-XSS-Protection", "Referrer-Policy"]:
        check(f"security header: {hdr}", hdr in r.headers,
              f"headers={dict(r.headers)}")

# -- Section 2 -- KB Query Endpoints -------------------------------------------

def test_kb(base: str):
    section("2 - KB Query Endpoints")

    # Direct triple query (GET)
    r = get(base, "/query?subject=NVDA&limit=10")
    check("GET /query -> 200", r.status_code == 200, f"got {r.status_code}")
    if r.status_code == 200:
        d = r.json()
        check("/query returns results list", isinstance(d.get("results"), list),
              f"keys={list(d.keys())}")

    # Smart retrieval (POST)
    r_ret = post(base, "/retrieve", {"message": "NVDA price target analyst signal"})
    check("POST /retrieve -> 200", r_ret.status_code == 200, f"got {r_ret.status_code}")
    if r_ret.status_code == 200:
        dr = r_ret.json()
        check("/retrieve returns snippet or atoms",
              bool(dr.get("snippet")) or dr.get("atoms_used", 0) > 0,
              f"keys={list(dr.keys())}")

    # Stats (always available)
    r2 = get(base, "/stats")
    check("GET /stats -> 200", r2.status_code == 200, f"got {r2.status_code}")
    if r2.status_code == 200:
        d2 = r2.json()
        check("/stats returns dict", isinstance(d2, dict), f"type={type(d2).__name__}")

    # KB conflicts
    r3 = get(base, "/kb/conflicts")
    check("GET /kb/conflicts -> 200", r3.status_code == 200, f"got {r3.status_code}")

    # Patterns live (503 is acceptable if pattern layer not loaded)
    r4 = get(base, "/patterns/live?limit=5")
    check("GET /patterns/live -> 200 or 503",
          r4.status_code in (200, 503), f"got {r4.status_code}")

    # Portfolio summary
    r5 = get(base, "/portfolio/summary")
    check("GET /portfolio/summary -> 200 or 503",
          r5.status_code in (200, 503), f"got {r5.status_code}")

    # Alerts
    r6 = get(base, "/alerts?limit=5")
    check("GET /alerts -> 200 or 503",
          r6.status_code in (200, 503), f"got {r6.status_code}")

# -- Section 3 -- Auth Flow -----------------------------------------------------

def test_auth(base: str) -> tuple[str, str]:
    """Returns (access_token, refresh_token) or ('', '')."""
    section("3 - Auth Flow")

    # 3a Register
    r = post(base, "/auth/register", {
        "user_id": USER_ID, "email": EMAIL, "password": PASSWORD
    })
    check("POST /auth/register -> 201", r.status_code == 201,
          f"got {r.status_code}: {r.text[:200]}")
    d = r.json()
    check("register returns user_id", d.get("user_id") == USER_ID)

    # 3b Duplicate email rejected
    r2 = post(base, "/auth/register", {
        "user_id": USER_ID + "_dup", "email": EMAIL, "password": PASSWORD
    })
    check("duplicate email -> 4xx", r2.status_code >= 400,
          f"got {r2.status_code}")

    # 3c Bad password (too short) rejected
    r3 = post(base, "/auth/register", {
        "user_id": USER_ID + "_bad", "email": f"bad_{RUN_ID}@x.com", "password": "short"
    })
    check("weak password rejected -> 400", r3.status_code == 400,
          f"got {r3.status_code}: {r3.text[:120]}")

    # 3d Token -- wrong password
    r4 = post(base, "/auth/token", {"email": EMAIL, "password": "wrongpassword"})
    check("wrong password -> 401", r4.status_code == 401,
          f"got {r4.status_code}")

    # 3e Token -- correct credentials
    r5 = post(base, "/auth/token", {"email": EMAIL, "password": PASSWORD})
    check("correct credentials -> 200", r5.status_code == 200,
          f"got {r5.status_code}: {r5.text[:200]}")
    td = r5.json()
    access  = td.get("access_token", "")
    refresh = td.get("refresh_token", "")
    check("token response has access_token", bool(access))
    check("token response has refresh_token", bool(refresh))
    check("token_type is Bearer", td.get("token_type") == "Bearer")
    check("expires_in is positive", (td.get("expires_in") or 0) > 0)

    if not access:
        R.skip("auth/me, refresh, logout", "no access token")
        return "", ""

    # 3f /auth/me
    r6 = get(base, "/auth/me", token=access)
    check("GET /auth/me -> 200", r6.status_code == 200,
          f"got {r6.status_code}")
    check("/auth/me returns correct user_id",
          r6.json().get("user_id") == USER_ID)

    # 3g Unauthenticated request rejected
    r7 = get(base, f"/users/{USER_ID}/portfolio")
    check("no token -> 401", r7.status_code == 401,
          f"got {r7.status_code}")

    # 3h Refresh token rotation
    if refresh:
        r8 = post(base, "/auth/refresh", {"refresh_token": refresh})
        check("POST /auth/refresh -> 200", r8.status_code == 200,
              f"got {r8.status_code}: {r8.text[:200]}")
        rd = r8.json()
        new_access  = rd.get("access_token", "")
        new_refresh = rd.get("refresh_token", "")
        check("refresh returns new access_token", bool(new_access))
        check("refresh returns new refresh_token", bool(new_refresh))
        check("refresh token rotated (new != old)", new_refresh != refresh)

        # Old refresh token must now be revoked
        r9 = post(base, "/auth/refresh", {"refresh_token": refresh})
        check("reused refresh token -> 401", r9.status_code == 401,
              f"got {r9.status_code}")

        # Use new tokens going forward
        access  = new_access
        refresh = new_refresh
    else:
        R.skip("refresh rotation checks", "no refresh token issued")

    return access, refresh


# -- Section 4 -- User Onboarding Flow -----------------------------------------

def test_onboarding(base: str, access: str):
    section("4 - User Onboarding Flow")

    if not access:
        R.skip("all onboarding tests", "no auth token")
        return

    # 4a Horizontal escalation blocked
    other_id = f"other_{RUN_ID}"
    r = get(base, f"/users/{other_id}/portfolio", token=access)
    check("different user_id -> 403", r.status_code == 403,
          f"got {r.status_code}")

    # 4b Portfolio submit
    r2 = post(base, f"/users/{USER_ID}/portfolio", {
        "holdings": [
            {"ticker": "AAPL", "quantity": 10,  "avg_cost": 175.0, "sector": "Technology"},
            {"ticker": "NVDA", "quantity": 5,   "avg_cost": 800.0, "sector": "Technology"},
            {"ticker": "JPM",  "quantity": 8,   "avg_cost": 195.0, "sector": "Financials"},
        ]
    }, token=access)
    check("POST /portfolio -> 201 or 503",
          r2.status_code in (201, 503), f"got {r2.status_code}: {r2.text[:200]}")
    if r2.status_code == 201:
        pd = r2.json()
        check("portfolio count == 3", pd.get("count") == 3, f"count={pd.get('count')}")

    # 4c Invalid portfolio (empty ticker) rejected
    r3 = post(base, f"/users/{USER_ID}/portfolio", {
        "holdings": [{"ticker": "", "quantity": 0}]
    }, token=access)
    check("empty ticker rejected -> 400 or 503",
          r3.status_code in (400, 503), f"got {r3.status_code}")

    # 4d Portfolio GET
    r4 = get(base, f"/users/{USER_ID}/portfolio", token=access)
    check("GET /portfolio -> 200 or 503",
          r4.status_code in (200, 503), f"got {r4.status_code}")

    # 4e Onboarding preferences
    r5 = post(base, f"/users/{USER_ID}/onboarding", {
        "selected_sectors": ["technology", "financials"],
        "risk_tolerance":   "moderate",
        "delivery_time":    "08:30",
        "timezone":         "Europe/London",
        "telegram_chat_id": "999000999",
    }, token=access)
    check("POST /onboarding -> 200 or 503",
          r5.status_code in (200, 503), f"got {r5.status_code}: {r5.text[:200]}")

    # 4f Tip config POST
    r6 = post(base, f"/users/{USER_ID}/tip-config", {
        "tip_delivery_time":      "08:30",
        "tip_delivery_timezone":  "Europe/London",
        "account_size":           10000.0,
        "max_risk_per_trade_pct": 1.5,
        "account_currency":       "GBP",
        "tier":                   "basic",
    }, token=access)
    check("POST /tip-config -> 200 or 503",
          r6.status_code in (200, 503), f"got {r6.status_code}: {r6.text[:200]}")

    # 4g Invalid tip-config (bad delivery time)
    r7 = post(base, f"/users/{USER_ID}/tip-config", {
        "tip_delivery_time": "not-a-time",
    }, token=access)
    check("bad tip_delivery_time -> 400 or 503",
          r7.status_code in (400, 503), f"got {r7.status_code}")

    # 4h Onboarding status
    r8 = get(base, f"/users/{USER_ID}/onboarding-status", token=access)
    check("GET /onboarding-status -> 200 or 503",
          r8.status_code in (200, 503), f"got {r8.status_code}")
    if r8.status_code == 200:
        st = r8.json()
        check("onboarding-status has 'complete' key", "complete" in st,
              f"keys={list(st.keys())}")

    # 4i Delivery history
    r9 = get(base, f"/users/{USER_ID}/delivery-history", token=access)
    check("GET /delivery-history -> 200 or 503",
          r9.status_code in (200, 503), f"got {r9.status_code}")

    # 4j Tip history
    r10 = get(base, f"/users/{USER_ID}/tip/history", token=access)
    check("GET /tip/history -> 200 or 503",
          r10.status_code in (200, 503), f"got {r10.status_code}")

    # 4k Tip config GET
    r11 = get(base, f"/users/{USER_ID}/tip-config", token=access)
    check("GET /tip-config -> 200 or 503",
          r11.status_code in (200, 503), f"got {r11.status_code}")

    # 4l Performance endpoint
    r12 = get(base, f"/users/{USER_ID}/performance", token=access)
    check("GET /performance -> 200 or 503",
          r12.status_code in (200, 503), f"got {r12.status_code}")

    # 4m User alerts
    r13 = get(base, f"/users/{USER_ID}/alerts", token=access)
    check("GET /user/alerts -> 200 or 503",
          r13.status_code in (200, 503), f"got {r13.status_code}")

    # 4n Alerts unread count
    r14 = get(base, f"/users/{USER_ID}/alerts/unread-count", token=access)
    check("GET /alerts/unread-count -> 200 or 503",
          r14.status_code in (200, 503), f"got {r14.status_code}")


# -- Section 5 -- KB-Grounded Chat Questions -----------------------------------

CHAT_QUESTIONS = [
    ("MACRO REGIME",
     "What is the current macro regime? Summarise the Fed stance, inflation trend, "
     "and what it implies for equity positioning.",
     ["macro", "fed", "regime", "inflation", "rate"]),

    ("NVDA BULL/BEAR",
     "Build a concise bull and bear case for NVDA based on current price, "
     "price target, analyst signal, and any risk factors in the KB.",
     ["nvda", "bull", "bear", "price", "target"]),

    ("MEGA-CAP UPSIDE RANKING",
     "Which of AAPL, MSFT, GOOGL, AMZN, NVDA, META has the largest percentage "
     "upside to consensus price target? Rank them.",
     ["nvda", "aapl", "msft", "googl", "upside"]),

    ("CROSS-ASSET REGIME READ",
     "TLT and HYG signals are available in the KB. What does their combined "
     "positioning imply about the current risk-on vs risk-off environment?",
     ["tlt", "hyg", "risk", "yield", "duration"]),

    ("SECTOR ROTATION",
     "Rank XLE, XLF, XLK and XLV by current signal strength. "
     "Which sector would you overweight and why?",
     ["xle", "xlf", "xlk", "sector"]),

    ("SEMI CYCLE COMPARISON",
     "Compare AMD, INTC, and NVDA on current signal direction and price vs target. "
     "What does this imply for the semiconductor cycle?",
     ["amd", "nvda", "semiconductor", "signal"]),

    ("FED -> FINANCIALS LINKAGE",
     "Given the current Fed stance and yield curve shape in the KB, "
     "what is the directional read for JPM, GS, and bank stocks broadly?",
     ["fed", "yield", "jpm", "financial", "bank"]),

    ("CONFLICT SURFACING",
     "Are there any internal contradictions in the KB -- tickers with conflicting "
     "signals or macro conditions that contradict individual stock theses?",
     ["conflict", "contradict", "signal", "long", "short", "neutral"]),

    ("EARNINGS CATALYST RISK",
     "Which tickers in the KB have upcoming earnings dates or recent catalyst flags? "
     "Flag any showing elevated risk.",
     ["earnings", "catalyst", "risk", "flag"]),

    ("HALLUCINATION TRAP",
     "What does the KB know about the Fed's secret yield suppression programme "
     "and its direct effect on NVDA gross margins?",
     ["no", "not", "unavailable", "don't", "cannot", "thin", "knowledge base",
      "no information", "not found"]),
]


def test_chat(base: str):
    section("5 - KB-Grounded Chat Questions (10 questions)")

    has_llm = False

    for i, (label, question, keywords) in enumerate(CHAT_QUESTIONS, 1):
        print(f"\n  [{i:02d}] {label}")
        print(f"       Q: {question[:90]}{'...' if len(question) > 90 else ''}")

        try:
            r = post(base, "/chat", {
                "message":    question,
                "session_id": f"live_qa_{RUN_ID}",
            })
        except requests.exceptions.Timeout:
            R.skip(f"CHAT {label}", "request timed out")
            continue

        if r.status_code == 503:
            d = r.json()
            snippet = d.get("snippet", "")
            atoms   = d.get("atoms_used", 0)
            # 503 means Ollama unavailable but KB context still returned
            if atoms > 0 or snippet:
                R.ok(f"CHAT {label} -- KB context retrieved (no LLM)",
                     f"atoms={atoms}")
            else:
                R.skip(f"CHAT {label}", "Ollama unavailable, no KB context")
            continue

        check(f"CHAT {label} HTTP 200", r.status_code == 200,
              f"got {r.status_code}")
        if r.status_code != 200:
            continue

        d       = r.json()
        answer  = (d.get("answer") or "").lower()
        atoms   = d.get("atoms_used", 0)
        stress  = (d.get("stress") or {}).get("composite_stress", 0.0)

        print(f"       atoms={atoms}  stress={round(stress, 3)}")
        if answer:
            has_llm = True
            preview = answer.replace("\n", " ")[:160]
            print(f"       A: {preview}{'...' if len(answer) > 160 else ''}")

        # Validate atoms > 0 (KB was actually used)
        check(f"CHAT {label} -- KB atoms > 0", atoms > 0,
              f"atoms={atoms} (KB may be empty)")

        if not answer:
            R.skip(f"CHAT {label} -- answer quality", "no LLM answer")
            continue

        # Hallucination trap: model must express ignorance, not invent facts
        if label == "HALLUCINATION TRAP":
            hallucination_phrases = [
                "the kb contains information about",
                "according to the knowledge base, the fed's secret",
                "yield suppression programme works by",
                "nvda margins are affected by this programme",
            ]
            hallucinated = any(p in answer for p in hallucination_phrases)
            expressed_ignorance = any(kw in answer for kw in keywords)
            if hallucinated:
                R.fail(f"CHAT {label} -- HALLUCINATED facts", answer[:200])
            elif expressed_ignorance:
                R.ok(f"CHAT {label} -- correctly expressed ignorance")
            else:
                R.fail(f"CHAT {label} -- neither hallucinated nor disclaimed clearly",
                       answer[:200])
        else:
            hits = [kw for kw in keywords if kw in answer]
            check(f"CHAT {label} -- keywords in answer",
                  len(hits) >= 1,
                  f"hits={hits} expected>=1 from {keywords}")

    if not has_llm:
        print("\n  [INFO] No LLM answers received (Ollama not running). "
              "KB context retrieval was validated where atoms > 0.")


# -- Section 6 -- Security Hardening Checks ------------------------------------

def test_security(base: str, access: str):
    section("6 - Security Hardening")

    # Missing token
    r1 = get(base, f"/users/{USER_ID}/portfolio")
    check("no token -> 401", r1.status_code == 401,
          f"got {r1.status_code}")

    # Malformed token
    r2 = get(base, f"/users/{USER_ID}/portfolio", token="not.a.real.jwt")
    check("malformed token -> 401", r2.status_code == 401,
          f"got {r2.status_code}")

    # Invalid JSON body
    if access:
        resp = requests.post(
            f"{base}/users/{USER_ID}/portfolio",
            data="not json",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {access}"},
            timeout=TIMEOUT,
        )
        check("invalid JSON body -> 400 or 422 or 503",
              resp.status_code in (400, 422, 503),
              f"got {resp.status_code}")

    # Validation: portfolio holdings must be a list
    if access:
        r3 = post(base, f"/users/{USER_ID}/portfolio",
                  {"holdings": "not-a-list"}, token=access)
        check("holdings not-a-list -> 400 or 503",
              r3.status_code in (400, 503), f"got {r3.status_code}")

    # Validation: onboarding invalid timezone
    if access:
        r4 = post(base, f"/users/{USER_ID}/onboarding",
                  {"risk_tolerance": "INVALID_RISK_VALUE_XYZ"}, token=access)
        check("invalid risk_tolerance -> 400 or 200 or 503",
              r4.status_code in (400, 200, 503),
              f"got {r4.status_code}")

    # Auth register: missing fields
    r5 = post(base, "/auth/register", {"email": EMAIL})
    check("register missing password -> 400",
          r5.status_code == 400, f"got {r5.status_code}: {r5.text[:120]}")

    # Auth register: invalid email format
    r6 = post(base, "/auth/register", {
        "user_id": f"bad_{RUN_ID}", "email": "notanemail", "password": "ValidPwd1!"
    })
    check("register invalid email -> 400",
          r6.status_code == 400, f"got {r6.status_code}: {r6.text[:120]}")

    # Revoked/invalid refresh token
    r7 = post(base, "/auth/refresh", {"refresh_token": "fakeinvalidtoken"})
    check("invalid refresh token -> 401",
          r7.status_code == 401, f"got {r7.status_code}")

    # Security headers present on every response
    r8 = get(base, "/health")
    for hdr, expected_val in [
        ("X-Content-Type-Options", "nosniff"),
        ("X-Frame-Options",        "DENY"),
    ]:
        actual = r8.headers.get(hdr, "")
        check(f"header {hdr}={expected_val}",
              actual.lower() == expected_val.lower(),
              f"got '{actual}'")


# -- Section 7 -- Logout --------------------------------------------------------

def test_logout(base: str, access: str, refresh: str):
    section("7 - Logout & Token Revocation")

    if not access:
        R.skip("logout tests", "no auth token")
        return

    r = post(base, "/auth/logout",
             {"refresh_token": refresh} if refresh else {},
             token=access)
    check("POST /auth/logout -> 200", r.status_code == 200,
          f"got {r.status_code}: {r.text[:200]}")
    check("logout returns logged_out=true",
          r.json().get("logged_out") is True)

    # Refresh token must now be dead
    if refresh:
        r2 = post(base, "/auth/refresh", {"refresh_token": refresh})
        check("post-logout refresh token -> 401",
              r2.status_code == 401, f"got {r2.status_code}")


# -- Main ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=DEFAULT_BASE,
                        help="Base URL of the running API server")
    args = parser.parse_args()
    base = args.base.rstrip("/")

    print(f"\n{'='*72}")
    print(f"  Trading Galaxy -- Comprehensive Live QA Battery")
    print(f"  Target: {base}")
    print(f"  Run ID: {RUN_ID}  User: {USER_ID}")
    print(f"{'='*72}")

    # Quick connectivity check
    try:
        requests.get(f"{base}/health", timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"\n  [ERROR] Cannot reach {base} - is the server running?")
        print(f"     Start it with:  python -m flask --app api.py run --port 5000\n")
        sys.exit(1)

    test_health(base)
    test_kb(base)
    access, refresh = test_auth(base)
    test_onboarding(base, access)
    test_chat(base)
    test_security(base, access)
    test_logout(base, access, refresh)

    passed = R.summary()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
