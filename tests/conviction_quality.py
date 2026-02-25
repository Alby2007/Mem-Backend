"""Show conviction_tier for highest-confidence names (strong signal_quality)."""
import sqlite3, sys
sys.path.insert(0, '.')

conn = sqlite3.connect('trading_knowledge.db')
c = conn.cursor()

# Join signal_quality, conviction_tier, position_size_pct, thesis_risk_level, upside_pct
c.execute("""
    SELECT
        sq.subject,
        sq.object   as signal_quality,
        ct.object   as conviction_tier,
        trl.object  as thesis_risk_level,
        mc.object   as macro_confirmation,
        ps.object   as position_size_pct,
        up.object   as upside_pct,
        lp.object   as last_price,
        pt.object   as price_target
    FROM facts sq
    LEFT JOIN facts ct  ON ct.subject  = sq.subject AND ct.predicate  = 'conviction_tier'
    LEFT JOIN facts trl ON trl.subject = sq.subject AND trl.predicate = 'thesis_risk_level'
    LEFT JOIN facts mc  ON mc.subject  = sq.subject AND mc.predicate  = 'macro_confirmation'
    LEFT JOIN facts ps  ON ps.subject  = sq.subject AND ps.predicate  = 'position_size_pct'
    LEFT JOIN facts up  ON up.subject  = sq.subject AND up.predicate  = 'upside_pct'
    LEFT JOIN facts lp  ON lp.subject  = sq.subject AND lp.predicate  = 'last_price'
    LEFT JOIN facts pt  ON pt.subject  = sq.subject AND pt.predicate  = 'price_target'
    WHERE sq.predicate = 'signal_quality'
    ORDER BY
        CASE sq.object
            WHEN 'strong'    THEN 1
            WHEN 'confirmed' THEN 2
            WHEN 'extended'  THEN 3
            WHEN 'weak'      THEN 4
            WHEN 'conflicted'THEN 5
            ELSE 6
        END,
        CASE ct.object
            WHEN 'high'   THEN 1
            WHEN 'medium' THEN 2
            WHEN 'low'    THEN 3
            WHEN 'avoid'  THEN 4
            ELSE 5
        END,
        CAST(up.object AS REAL) DESC
""")
rows = c.fetchall()

print(f"{'Ticker':<7} {'SQ':<10} {'CT':<8} {'Risk':<9} {'Macro':<12} {'Size%':<7} {'Upside%':<9} {'Price':<8} {'Target'}")
print("-" * 95)

for r in rows:
    ticker, sq, ct, trl, mac, ps, up, lp, pt = r
    ticker = (ticker or '').upper()[:6]
    sq  = (sq  or '-')[:9]
    ct  = (ct  or '-')[:7]
    trl = (trl or '-')[:8]
    mac = (mac or '-')[:11]
    ps  = (ps  or '-')[:6]
    try:
        up_f = f"{float(up):+.1f}" if up else '-'
    except:
        up_f = '-'
    try:
        lp_f = f"{float(lp):.1f}" if lp else '-'
    except:
        lp_f = '-'
    try:
        pt_f = f"{float(pt):.1f}" if pt else '-'
    except:
        pt_f = '-'
    print(f"{ticker:<7} {sq:<10} {ct:<8} {trl:<9} {mac:<12} {ps:<7} {up_f:<9} {lp_f:<8} {pt_f}")

conn.close()
