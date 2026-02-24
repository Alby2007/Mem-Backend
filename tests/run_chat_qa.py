"""
tests/run_chat_qa.py — Live interrogation battery for POST /chat
Run: python tests/run_chat_qa.py
"""
import requests

BASE = "http://127.0.0.1:5050"

QUESTIONS = [
    ("Q1  Direct ticker signal",
     "What is the current signal direction and price target on NVDA?"),
    ("Q2  Multi-ticker comparison",
     "Compare signal direction and price targets for NVDA, META and GOOGL. Which has the most upside to its price target?"),
    ("Q3  Macro regime + positioning",
     "What is the current macro regime and Fed stance? How should I position across asset classes given this?"),
    ("Q4  Relational — Fed to tech",
     "How does Fed policy affect tech stocks through yield sensitivity?"),
    ("Q5  Sector rotation signals",
     "What do the current sector signals say about XLF, XLE and XLK? Which sector has the strongest momentum?"),
    ("Q6  Sparse / hallucination trap",
     "What does the KB know about ytterbium photon qubit arbitrage strategies?"),
    ("Q7  Rates and duration",
     "What are the current signals on TLT and HYG? What does the yield curve shape imply for duration positioning?"),
    ("Q8  Conflict surfacing",
     "Are there any conflicting signals in the current watchlist? Surface any contradictions between tickers."),
]

EXPECTED = {
    "Q1": ["nvda", "signal_direction", "price_target", "253"],
    "Q2": ["nvda", "meta", "googl", "price_target"],
    "Q3": ["macro", "fed", "regime"],
    "Q4": ["fed", "yield", "tech"],
    "Q5": ["xlf", "xle", "xlk", "sector"],
    "Q6": ["no", "not", "unavailable", "thin", "don't", "cannot"],   # must NOT hallucinate facts
    "Q7": ["tlt", "hyg", "yield"],
    "Q8": ["conflict", "contradict", "neutral", "long", "short"],
}

def check_answer(key, answer):
    ans_lower = answer.lower()
    hits = [kw for kw in EXPECTED.get(key, []) if kw in ans_lower]
    return hits

def run():
    print(f"\n{'='*72}")
    print("  Trading Galaxy — KB-Grounded Chat QA Battery")
    print(f"{'='*72}")

    passes = 0
    failures = []

    for label, msg in QUESTIONS:
        key = label.split()[0]
        print(f"\n{'-'*72}")
        print(f"  {label}")
        print(f"{'-'*72}")
        r = requests.post(f"{BASE}/chat",
                          json={"message": msg,
                                "session_id": "qa_battery",
                                "model": "llama3.2:latest"},
                          timeout=120)
        status = r.status_code
        d = r.json()

        stress     = round(d.get("stress", {}).get("composite_stress", 0.0), 3)
        atoms      = d.get("atoms_used", 0)
        model      = d.get("model", "?")
        answer     = d.get("answer") or d.get("error", "NO ANSWER")
        diag       = d.get("kb_diagnosis", {}).get("primary_type", "")
        adaptation = d.get("adaptation", {})

        print(f"  HTTP:{status}  atoms:{atoms}  stress:{stress}  model:{model}")
        if diag:
            print(f"  KB_DIAG: {diag}  conf:{d['kb_diagnosis'].get('confidence',0):.2f}")
        if adaptation:
            print(f"  ADAPTATION: streak={adaptation.get('streak')}  consolidation={adaptation.get('consolidation_mode')}")

        print(f"\n  ANSWER:\n")
        for line in answer.splitlines():
            print(f"    {line}")

        # Quality check
        hits = check_answer(key, answer)
        q6_hallucination = False
        if key == "Q6":
            # Hallucination = model ASSERTS ytterbium trading facts as if they exist.
            # Merely mentioning the topic name while disclaiming is correct behaviour.
            hallucination_phrases = [
                "here is information about ytterbium",
                "the kb contains information",
                "ytterbium photon qubit arbitrage works by",
                "strategy involves",
                "according to the knowledge base, ytterbium",
            ]
            q6_hallucination = any(p in answer.lower() for p in hallucination_phrases)
            # Model correctly expressed ignorance if it uses "no", "not", "don't", "cannot", "unavailable"
            q6_pass = any(kw in answer.lower() for kw in EXPECTED["Q6"]) and not q6_hallucination

        if key == "Q6":
            if q6_pass:
                print(f"\n  ✅ PASS — correctly expressed ignorance / thin coverage")
                passes += 1
            else:
                note = "HALLUCINATION or did not disclaim ignorance"
                print(f"\n  ❌ FAIL — {note}")
                failures.append((label, note))
        elif hits:
            print(f"\n  ✅ PASS — keywords found: {hits}")
            passes += 1
        else:
            note = f"expected keywords not found: {EXPECTED.get(key, [])}"
            print(f"\n  ❌ FAIL — {note}")
            failures.append((label, note))

    print(f"\n{'='*72}")
    print(f"  RESULTS: {passes}/{len(QUESTIONS)} passed")
    if failures:
        print(f"\n  FAILURES:")
        for lbl, note in failures:
            print(f"    - {lbl}: {note}")
    print(f"{'='*72}\n")

    # Second pass: qwen2.5:3b on Q1 and Q3 for model comparison
    print("\n" + "="*72)
    print("  MODEL COMPARISON — qwen2.5:3b on Q1 and Q3")
    print("="*72)
    for label, msg in [QUESTIONS[0], QUESTIONS[2]]:
        print(f"\n  {label}")
        r = requests.post(f"{BASE}/chat",
                          json={"message": msg,
                                "session_id": "qa_qwen",
                                "model": "qwen2.5:3b"},
                          timeout=120)
        d = r.json()
        answer = d.get("answer") or d.get("error", "NO ANSWER")
        stress = round(d.get("stress", {}).get("composite_stress", 0.0), 3)
        print(f"  atoms:{d.get('atoms_used',0)}  stress:{stress}  model:{d.get('model','?')}")
        print(f"\n  ANSWER:\n")
        for line in answer.splitlines():
            print(f"    {line}")

if __name__ == "__main__":
    run()
