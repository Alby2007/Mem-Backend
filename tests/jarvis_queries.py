"""
tests/jarvis_queries.py — Live KB-grounded chat queries (Jarvis session)
Covers: macro, individual signals, cross-asset, sector rotation, conflict surfacing,
        rates, earnings catalyst, and a hallucination probe.
"""
import requests

BASE   = 'http://127.0.0.1:5050'
MODEL  = 'llama3.2:latest'
SID    = 'jarvis'

QUERIES = [
    ("MACRO REGIME",
     "What is the current macro regime? Summarise the Fed stance, inflation, GDP growth and yield curve shape."),

    ("NVDA THESIS",
     "Build a concise bull/bear case for NVDA based on current price, price target, analyst signal and any recent catalysts or risk factors in the KB."),

    ("CROSS-ASSET: RATES vs EQUITY",
     "TLT and HYG are both near_high. NVDA and META are long. What does that combination say about the current risk-on/risk-off regime?"),

    ("SECTOR ROTATION",
     "Rank XLE, XLF, XLK and XLV by current signal strength. Which sector ETF would you overweight and why?"),

    ("MEGA-CAP UPSIDE RANKING",
     "Which of AAPL, MSFT, GOOGL, AMZN, NVDA, META has the largest percentage upside to consensus price target? Rank them."),

    ("SEMI CYCLE: AMD vs INTC vs NVDA",
     "Compare AMD, INTC, and NVDA on current price vs price target and signal direction. What does this imply for the semiconductor cycle?"),

    ("EARNINGS RISK",
     "Which tickers in the KB have upcoming earnings dates? Are any of them also showing insider transaction activity? Flag any that look elevated risk."),

    ("CONFLICT PROBE",
     "XOM and CVX are both showing neutral signals despite positive macro. Does the KB show any internal contradictions or risk factors for energy names?"),

    ("MACRO → FINANCIALS LINKAGE",
     "JPM, GS and BLK are all in the KB. Given the current Fed stance and yield curve, what is the directional read for financials?"),

    ("HALLUCINATION TRAP",
     "What does the KB know about the Fed's secret repo facility yield suppression programme and its effect on NVDA margins?"),
]

SEP = "=" * 72

def run():
    print(f"\n{SEP}")
    print("  JARVIS — KB-Grounded Chat Session")
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
        http   = r.status_code
        atoms  = d.get("atoms_used", 0)
        stress = round(d.get("stress", {}).get("composite_stress", 0.0), 3)
        diag   = d.get("kb_diagnosis", {})
        diag_s = f"  KB_DIAG: {diag.get('primary_type')} conf={diag.get('confidence',0):.2f}" if diag else ""

        print(f"  HTTP:{http}  atoms:{atoms}  stress:{stress}{diag_s}")
        print()

        answer = d.get("answer") or d.get("error", "NO ANSWER")
        for line in answer.splitlines():
            print(f"    {line}")

    print(f"\n{SEP}\n")

if __name__ == "__main__":
    run()
