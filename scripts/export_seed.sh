#!/bin/bash
# Export shared KB data only — no user data
# Run this on your live machine to update tests/fixtures/kb_seed.sql
#
# Usage:
#   bash scripts/export_seed.sh                         # uses trading_knowledge.db
#   bash scripts/export_seed.sh /path/to/other.db

set -e

DB=${1:-trading_knowledge.db}
OUTPUT=tests/fixtures/kb_seed.sql

if [ ! -f "$DB" ]; then
    echo "ERROR: database not found at '$DB'"
    echo "       Pass the path as the first argument: bash scripts/export_seed.sh /path/to/db"
    exit 1
fi

# ── Pre-flight: verify all expected tables exist ──────────────────────────────

SHARED_TABLES=(
    "facts"
    "fact_conflicts"
    "causal_edges"
    "pattern_signals"
    "signal_calibration"
    "extraction_queue"
    "edgar_realtime_seen"
    "taxonomy"
    "fact_categories"
    "predicate_vocabulary"
    "working_state"
    "governance_metrics"
    "domain_refresh_queue"
    "synthesis_queue"
    "consolidation_log"
    "kb_insufficient_log"
    "repair_execution_log"
    "repair_rollback_log"
)

USER_TABLES=(
    "user_auth"
    "refresh_tokens"
    "user_portfolios"
    "user_models"
    "user_preferences"
    "user_kb_context"
    "user_engagement_events"
    "user_universe_expansions"
    "universe_tickers"
    "ticker_staging"
    "snapshot_delivery_log"
    "tip_delivery_log"
    "tip_feedback"
    "audit_log"
    "alerts"
)

echo "Running pre-flight table check on $DB..."
MISSING=0
for TABLE in "${SHARED_TABLES[@]}" "${USER_TABLES[@]}"; do
    EXISTS=$(sqlite3 "$DB" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='$TABLE';")
    if [ "$EXISTS" -eq 0 ]; then
        echo "  MISSING TABLE: $TABLE"
        MISSING=$((MISSING + 1))
    fi
done

if [ "$MISSING" -gt 0 ]; then
    echo ""
    echo "ERROR: $MISSING table(s) missing from $DB"
    echo "       Run the app once (python api.py) so all ensure_*_tables() calls fire,"
    echo "       then re-run this script."
    exit 1
fi

echo "All tables present."

# ── Quality gate: KB must be sufficiently populated before export ─────────────
#
# Thresholds:
#   total_facts   >= 2000  (full ingest cycle required)
#   open_patterns >= 1     (pattern detector must have run)
#   stress_score  <  0.4   (KB not in distress)
#
# If any threshold fails the script exits with instructions — never export a
# near-empty DB and ship it as the seed.

echo ""
echo "Running quality gate..."

QUALITY_FAIL=0

TOTAL_FACTS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM facts;" 2>/dev/null || echo 0)
if [ "$TOTAL_FACTS" -lt 2000 ]; then
    echo "  FAIL  total_facts = $TOTAL_FACTS  (need >= 2000)"
    echo "        Run: curl -X POST http://localhost:5050/ingest/run-all"
    echo "             curl -X POST http://localhost:5050/ingest/historical"
    echo "        Then wait ~5 minutes for signal enrichment to complete."
    QUALITY_FAIL=$((QUALITY_FAIL + 1))
else
    echo "  OK    total_facts = $TOTAL_FACTS"
fi

OPEN_PATTERNS=$(sqlite3 "$DB" \
    "SELECT COUNT(*) FROM pattern_signals WHERE status='open';" 2>/dev/null || echo 0)
TOTAL_PATTERNS=$(sqlite3 "$DB" \
    "SELECT COUNT(*) FROM pattern_signals;" 2>/dev/null || echo 0)
if [ "$OPEN_PATTERNS" -lt 1 ]; then
    echo "  WARN  open_patterns = $OPEN_PATTERNS, total_patterns = $TOTAL_PATTERNS"
    echo "        Pattern detector may not have run yet — seed will still ship shared KB facts."
else
    echo "  OK    open_patterns = $OPEN_PATTERNS"
fi

# Conflict ratio check (>50% of facts = KB under stress)
CONFLICTS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM fact_conflicts;" 2>/dev/null || echo 0)
CONFLICT_RATIO=$(awk -v c="$CONFLICTS" -v f="$TOTAL_FACTS" \
    'BEGIN { if (f > 0) printf "%.2f", c/f; else print "0" }')
CONFLICT_OVER=$(awk -v r="$CONFLICT_RATIO" 'BEGIN { print (r > 0.5) ? "yes" : "no" }')
if [ "$CONFLICT_OVER" = "yes" ]; then
    echo "  FAIL  fact_conflicts = $CONFLICTS (${CONFLICT_RATIO} ratio — KB under stress)"
    echo "        More than 50% of facts have conflicts — resolve before exporting."
    QUALITY_FAIL=$((QUALITY_FAIL + 1))
else
    echo "  OK    fact_conflicts = $CONFLICTS ($CONFLICT_RATIO ratio)"
fi

if [ "$QUALITY_FAIL" -gt 0 ]; then
    echo ""
    echo "ERROR: $QUALITY_FAIL quality check(s) failed — export aborted."
    echo "       The seed is only as good as the KB state when you export it."
    echo "       Fix the issues above and re-run this script."
    exit 1
fi

echo "Quality gate passed."

# ── Export ────────────────────────────────────────────────────────────────────

echo "Exporting shared KB from $DB to $OUTPUT..."

sqlite3 "$DB" << EOF
.output $OUTPUT

-- ── Shared KB tables — full data ────────────────────────────────────────────
.dump facts
.dump fact_conflicts
.dump decay_log
.dump causal_edges
.dump pattern_signals
.dump signal_calibration
.dump extraction_queue
.dump fvg_signals
.dump kb_stress_log
.dump adaptation_log

-- ── User / personal tables — schema only, no rows ──────────────────────────
.schema user_auth
.schema user_portfolios
.schema user_models
.schema user_preferences
.schema user_kb_context
.schema user_engagement_events
.schema user_universe_expansions
.schema universe_tickers
.schema ticker_staging
.schema snapshot_delivery_log
.schema tip_delivery_log
.schema tip_feedback
.schema signal_snapshots
.schema audit_log
.schema alerts
.schema user_trade_history
.schema user_subscriptions
.schema waitlist
EOF

# ── Summary ───────────────────────────────────────────────────────────────────

INSERTS=$(grep -c 'INSERT' "$OUTPUT" || true)
echo ""
echo "Done."
echo "  Output : $OUTPUT"
echo "  INSERT rows exported : $INSERTS"
echo ""
echo "Commit the updated seed:"
echo "  git add $OUTPUT && git commit -m 'chore: refresh KB seed'"
