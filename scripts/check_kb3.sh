#!/bin/bash
DB=/opt/trading-galaxy/data/trading_knowledge.db

echo "=== Matching predicates from 8-predicate filter ==="
sqlite3 "$DB" "SELECT predicate, COUNT(*) as cnt, MAX(timestamp) as latest FROM facts WHERE predicate IN ('regime_label','market_regime','conviction_tier','macro_signal','geopolitical_risk','sector_tailwind','pre_earnings_flag','signal_direction') GROUP BY predicate ORDER BY cnt DESC;"

echo ""
echo "=== Has signal_direction written? ==="
sqlite3 "$DB" "SELECT COUNT(*) FROM facts WHERE predicate='signal_direction';"

echo ""
echo "=== Has conviction_tier written? ==="
sqlite3 "$DB" "SELECT subject, object, timestamp FROM facts WHERE predicate='conviction_tier' ORDER BY timestamp DESC LIMIT 5;"

echo ""
echo "=== Has market_regime written? ==="
sqlite3 "$DB" "SELECT subject, object, timestamp FROM facts WHERE predicate='market_regime' ORDER BY timestamp DESC LIMIT 5;"

echo ""
echo "=== Similar predicates that ARE written (fuzzy match) ==="
sqlite3 "$DB" "SELECT DISTINCT predicate FROM facts WHERE predicate LIKE '%regime%' OR predicate LIKE '%conviction%' OR predicate LIKE '%signal%' OR predicate LIKE '%sector%' OR predicate LIKE '%macro%' ORDER BY predicate;"
