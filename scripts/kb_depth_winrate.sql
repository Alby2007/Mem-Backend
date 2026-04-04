-- KB depth vs win rate analysis
-- Run once you have 30-40 closed paper trades across a mix of kb_depth levels.
-- Requires: paper_positions table with ai_reasoning and status columns.
--
-- Usage on OCI:
--   sqlite3 /opt/trading-galaxy/data/trading_knowledge.db < scripts/kb_depth_winrate.sql

SELECT
    CASE
        WHEN ai_reasoning LIKE '%kb_depth=deep%'    THEN 'deep'
        WHEN ai_reasoning LIKE '%kb_depth=shallow%' THEN 'shallow'
        ELSE 'thin'
    END AS kb_depth,
    COUNT(*)                                                                          AS trades,
    ROUND(AVG(pnl_r), 3)                                                              AS avg_r,
    ROUND(
        SUM(CASE WHEN status IN ('t2_hit', 't1_hit') THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        1
    )                                                                                 AS win_pct
FROM paper_positions
WHERE user_id = 'albertjemmettwaite_uggwq'
  AND status IN ('t2_hit', 't1_hit', 'stopped_out')
GROUP BY kb_depth
ORDER BY avg_r DESC;
