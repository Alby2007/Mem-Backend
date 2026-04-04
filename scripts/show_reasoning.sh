#!/bin/bash
# Show full reasoning for last 5 entry log entries
DB="/opt/trading-galaxy/data/trading_knowledge.db"
sqlite3 -column -header "$DB" "
SELECT created_at, ticker, substr(detail, 1, 500) as detail
FROM paper_agent_log
WHERE user_id='albertjemmettwaite_uggwq' AND event_type='entry'
ORDER BY id DESC LIMIT 5;
"
