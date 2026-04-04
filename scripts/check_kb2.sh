#!/bin/bash
DB=/opt/trading-galaxy/data/trading_knowledge.db

echo "=== facts table schema ==="
sqlite3 "$DB" "PRAGMA table_info(facts);"

echo ""
echo "=== Sample predicate names in facts ==="
sqlite3 "$DB" "SELECT DISTINCT predicate FROM facts ORDER BY predicate LIMIT 60;"

echo ""
echo "=== tip_followups schema ==="
sqlite3 "$DB" "PRAGMA table_info(tip_followups);"
