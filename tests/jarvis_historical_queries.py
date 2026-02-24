"""
tests/jarvis_historical_queries.py — Verify LLM reasoning over historical summary atoms.
Targets return_1m/3m/1y, volatility_30d/90d, drawdown_from_52w_high, return_vs_spy.
"""
import requests

BASE  = 'http://127.0.0.1:5050'
MODEL = 'llama3.2:latest'
SID   = 'jarvis_hist'

QUERIES = [
    ("RELATIVE PERFORMANCE SCAN",
     "Which tickers in the KB have outperformed SPY over the last month? "
     "Rank them by return_vs_spy_1m descending."),

    ("MSFT UNDERPERFORMANCE",
     "MSFT has significantly underperformed SPY over 1 and 3 months. "
     "What does the KB show about MSFT's return_vs_spy_1m, return_vs_spy_3m, "
     "drawdown_from_52w_high, and signal_quality? Is there a coherent thesis here?"),

    ("XOM MOMENTUM",
     "XOM has been one of the strongest performers in the KB over 3 months. "
     "Walk through its return_3m, return_vs_spy_3m, price_regime, signal_quality, "
     "and macro_confirmation. Does the KB support continuing to hold energy?"),

    ("VOLATILITY COMPARISON",
     "Compare META, NVDA and MSFT on volatility_30d and volatility_90d. "
     "Which name carries the most realised risk and is that consistent "
     "with their signal_quality classification?"),

    ("DRAWDOWN ANALYSIS",
     "Which tickers show drawdown_from_52w_high worse than -20%? "
     "For each, what is the signal_quality and upside_pct? "
     "Flag any where the drawdown looks like opportunity vs deterioration."),

    ("SEMI CYCLE HISTORICAL CONTEXT",
     "Compare AMD, NVDA and INTC on return_1y, return_3m, and volatility_90d. "
     "Given the return history, which has had the best risk-adjusted performance "
     "and which is lagging?"),

    ("AAPL VS SPY ALPHA",
     "AAPL has outperformed SPY significantly over 1 month but underperformed "
     "over 3 months. What does return_1m, return_3m, return_vs_spy_1m, "
     "return_vs_spy_3m show? Is this a rotation or mean-reversion setup?"),

    ("ENERGY VS TECH ROTATION",
     "Compare XOM and CVX (energy) versus NVDA and AMD (tech) on "
     "return_3m and return_vs_spy_3m. What does this imply for sector rotation?"),
]

SEP = "=" * 72

def run():
    print(f"\n{SEP}")
    print("  JARVIS — Historical Context Queries")
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
