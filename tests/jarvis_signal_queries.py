"""
tests/jarvis_signal_queries.py — Verify LLM reasoning over derived signal atoms.
Targets signal_quality, macro_confirmation, price_regime, upside_pct.
"""
import requests

BASE  = 'http://127.0.0.1:5050'
MODEL = 'llama3.2:latest'
SID   = 'jarvis_signal'

QUERIES = [
    ("SIGNAL QUALITY SCAN",
     "Which equities in the KB currently have signal_quality of 'strong'? "
     "What do they have in common?"),

    ("DIVERGENCE: EXTENDED SIGNALS",
     "Which tickers are showing 'extended' signal quality? "
     "What does that mean for positioning — should these be trimmed or held?"),

    ("CONFLICTED SIGNALS",
     "Are there any tickers with conflicted signal quality in the KB? "
     "Explain the source of the conflict for each."),

    ("META DEEP DIVE",
     "META has a strong signal_quality. Walk through exactly why — "
     "price_regime, upside_pct, volatility, macro_confirmation. "
     "What is the bear case that could invalidate the signal?"),

    ("CROSS-ASSET MACRO CONFIRMATION",
     "HYG is near_high, TLT is near_high, SPY is near_high. "
     "Most equities show partial macro_confirmation. "
     "What does TLT near_high mean for the risk regime and why does it "
     "prevent full macro confirmation for long equity signals?"),

    ("SEMI CYCLE QUALITY COMPARISON",
     "Compare AMD, INTC and NVDA on signal_quality and macro_confirmation. "
     "Which has the strongest composite signal and which has the most risk?"),

    ("ENERGY SECTOR QUALITY",
     "XOM and CVX are in the KB. What are their signal_quality and "
     "price_regime classifications? Given XOM has negative upside_pct, "
     "what does that say about current energy positioning?"),

    ("UPSIDE RANKING WITH QUALITY FILTER",
     "Rank MSFT, GOOGL, AMZN, NVDA, META and AMD by upside_pct. "
     "Then filter to only those with signal_quality of strong or confirmed. "
     "Which names pass both criteria?"),

    ("FINANCIALS SIGNAL COMPOSITE",
     "JPM, GS and BLK are in the KB. What are their signal_quality and "
     "macro_confirmation values? Given partial macro confirmation across "
     "the market, which financial name has the cleanest signal?"),

    ("PORTFOLIO STRESS TEST",
     "If I hold NVDA, META, MSFT and AMD equally weighted, "
     "what is the aggregate signal quality picture? "
     "Which position is the weakest link and why?"),
]

SEP = "=" * 72

def run():
    print(f"\n{SEP}")
    print("  JARVIS — Signal Quality Layer Queries")
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
