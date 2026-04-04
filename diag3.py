import sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
import extensions as ext
from services.paper_trading import _is_market_open

conn = sqlite3.connect(ext.DB_PATH)
conn.row_factory = sqlite3.Row

# What does the actual candidate query return (same as _ai_run_inner)?
rows = conn.execute("""
    SELECT p.id, p.ticker, p.pattern_type, p.direction, p.zone_high, p.zone_low,
           p.quality_score, p.kb_conviction, p.kb_regime, p.kb_signal_dir
    FROM pattern_signals p
    INNER JOIN (
        SELECT ticker, MAX(quality_score) AS best_q
        FROM pattern_signals
        WHERE status NOT IN ('filled','broken')
          AND (
            (quality_score >= 0.70 AND LOWER(kb_conviction) IN ('high','confirmed','strong'))
            OR (quality_score >= 0.65 AND (kb_conviction IS NULL OR kb_conviction = ''))
          )
        GROUP BY ticker
    ) best ON best.ticker = p.ticker AND p.quality_score = best.best_q
    WHERE p.status NOT IN ('filled','broken')
      AND (
        (p.quality_score >= 0.70 AND LOWER(p.kb_conviction) IN ('high','confirmed','strong'))
        OR (p.quality_score >= 0.65 AND (p.kb_conviction IS NULL OR p.kb_conviction = ''))
      )
    ORDER BY p.quality_score DESC
    LIMIT 60
""").fetchall()

print(f"=== Candidate query returns: {len(rows)} rows ===")
open_now = [r for r in rows if _is_market_open(r['ticker'])]
closed_now = [r for r in rows if not _is_market_open(r['ticker'])]
print(f"  OPEN now: {len(open_now)}  |  CLOSED now: {len(closed_now)}")

print(f"\n--- OPEN markets ---")
for r in open_now[:20]:
    print(f"  {r['ticker']:12} {r['direction']:8} q={r['quality_score']:.3f} conv={r['kb_conviction']} sig={r['kb_signal_dir']}")

print(f"\n--- All .L tickers in candidates ---")
uk = [r for r in rows if r['ticker'].upper().endswith('.L')]
for r in uk[:20]:
    print(f"  {r['ticker']:12} {r['direction']:8} q={r['quality_score']:.3f} conv={r['kb_conviction']} sig={r['kb_signal_dir']}")

# Check if conviction threshold is the issue — how many .L with high/confirmed/strong?
print(f"\n=== .L tickers by conviction level ===")
conv_rows = conn.execute("""
    SELECT kb_conviction, COUNT(*) as cnt, MAX(quality_score) as max_q
    FROM pattern_signals
    WHERE status NOT IN ('filled','broken') AND ticker LIKE '%.L'
    GROUP BY kb_conviction ORDER BY max_q DESC
""").fetchall()
for r in conv_rows:
    print(f"  conv='{r['kb_conviction']}' cnt={r['cnt']} max_q={r['max_q']:.3f}")

conn.close()
