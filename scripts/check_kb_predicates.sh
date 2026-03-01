#!/bin/bash
DB=/opt/trading-galaxy/data/trading_knowledge.db

echo "=== KB Change Predicates ==="
sqlite3 "$DB" "SELECT predicate, COUNT(*) as cnt, MAX(created_at) as latest FROM facts WHERE predicate IN ('regime_label','market_regime','conviction_tier','macro_signal','geopolitical_risk','sector_tailwind','pre_earnings_flag','signal_direction') GROUP BY predicate ORDER BY cnt DESC;"

echo ""
echo "=== tip_followups rows ==="
sqlite3 "$DB" "SELECT id, user_id, ticker, timeframe, expires_at, status FROM tip_followups ORDER BY id DESC LIMIT 20;"

echo ""
echo "=== NULL expires_at count ==="
sqlite3 "$DB" "SELECT COUNT(*) FROM tip_followups WHERE expires_at IS NULL AND status IN ('watching','active');"
