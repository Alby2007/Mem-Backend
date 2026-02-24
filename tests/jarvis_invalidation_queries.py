"""
tests/jarvis_invalidation_queries.py — validate LLM reasoning over invalidation layer.
"""
import requests

BASE  = 'http://127.0.0.1:5050'
MODEL = 'llama3.2:latest'
SID   = 'jarvis_inv'

QUERIES = [
    ("NVDA FULL THESIS",
     "NVDA is long with signal_quality confirmed. What is the invalidation_price, "
     "invalidation_distance, and thesis_risk_level? Given the upside_pct, "
     "what is the risk/reward asymmetry? Is this a good entry?"),

    ("META VS MSFT RISK COMPARISON",
     "Compare META and MSFT on thesis_risk_level, invalidation_distance, "
     "and upside_pct. Both have strong signal_quality. Which has better "
     "risk/reward and why does the invalidation distance matter here?"),

    ("TIGHTEST STOPS PORTFOLIO SCREEN",
     "Which tickers in the KB have thesis_risk_level of tight? "
     "For each, what is the invalidation_distance and signal_quality? "
     "Flag any where a tight stop is combined with a strong or confirmed signal."),

    ("AMD ASYMMETRY READ",
     "AMD has a wide thesis_risk_level with strong upside_pct. "
     "Walk through the complete asymmetry picture: "
     "invalidation_price, invalidation_distance, upside_pct, signal_quality. "
     "What does this say about AMD as a risk/reward trade?"),

    ("ENERGY THESIS INVALIDATION",
     "XOM and CVX are in the KB with neutral signals. "
     "What are their invalidation prices and distances? "
     "At what level is the energy thesis definitively wrong?"),
]

SEP = "=" * 72

def run():
    print(f"\n{SEP}")
    print("  JARVIS — Invalidation Layer Queries")
    print(f"{SEP}")

    for label, question in QUERIES:
        print(f"\n{'-'*72}")
        print(f"  [{label}]")
        print(f"  Q: {question}")
        print(f"{'-'*72}")

        r = requests.post(f"{BASE}/chat",
                          json={"message": question,
                                "session_id": SID,
                                "model": MODEL},
                          timeout=120)
        d = r.json()
        atoms  = d.get("atoms_used", 0)
        stress = round(d.get("stress", {}).get("composite_stress", 0.0), 3)
        print(f"  HTTP:{r.status_code}  atoms:{atoms}  stress:{stress}")
        print()
        answer = d.get("answer") or d.get("error", "NO ANSWER")
        for line in answer.splitlines():
            print(f"    {line}")

    print(f"\n{SEP}\n")

if __name__ == "__main__":
    run()
