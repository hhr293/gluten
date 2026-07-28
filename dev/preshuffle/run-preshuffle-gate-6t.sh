#!/bin/bash
# 6T A/B: preshuffle rule ON (gated) vs OFF, using a driver-local stats TSV.
# Runs the full 108-query catalog by default. Set QUERIES=q14a (etc.) to smoke.
#
# Prereqs:
#   1) env.sh points NATIVE_JARS at the v5+ jar (with statsFile support).
#   2) 6T Hive catalog (default tpcds_analyze_6000) has been ANALYZE'd on dim tables.
#   3) TSV produced by run-dump-tpcds-stats-tsv-6t.sh.
#
# Tunables (env vars):
#   STATS_FILE     - path to the TSV, default preshuffle-stats-tpcds-6t.tsv
#   MIN_ROWS       - minimum rowCount on fact side, default 100000000 (1e8) for 6T
#   MIN_RATIO      - required rows/groupNdv, default 10.0
#   QUERIES        - single query smoke, e.g. QUERIES=q14a; empty = full suite
#   REQUIRE_STATS  - fail-closed (skip on missing stats), default true
set +e
cd /home/pnp/gluten-tools/bench

LOG4J_CONF=/home/pnp/gluten-tools/bench/log4j2-preshuffle-gate.properties
COMMON_LOG_OPTS="--files ${LOG4J_CONF}"
export EXTRA_DRIVER_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF}) -Dderby.system.home=/tmp/derby"
export EXTRA_EXECUTOR_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF})"

STATS_FILE=${STATS_FILE:-/home/pnp/gluten-tools/bench/preshuffle-stats-tpcds-6t.tsv}
MIN_ROWS=${MIN_ROWS:-100000000}
MIN_RATIO=${MIN_RATIO:-10.0}
REQUIRE_STATS=${REQUIRE_STATS:-true}

if [[ ! -f "$STATS_FILE" ]]; then
  echo "ERROR: stats file $STATS_FILE not found. Run run-dump-tpcds-stats-tsv-6t.sh first."
  exit 1
fi

STATS_CONF="--conf spark.gluten.sql.columnar.preShufflePartialAgg.statsFile=${STATS_FILE}"
Q_TAG=${QUERIES:+-${QUERIES}}

# ---------- OFF baseline ----------
tag_off=preshuffle-gate-6t-off${Q_TAG}
echo "=== $(date) tpcds 6T preshuffle OFF (${QUERIES:-full-suite}) starting ==="
EXTRA_CONF="${COMMON_LOG_OPTS} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.enabled=false" \
  ${QUERIES:+QUERIES=$QUERIES} \
  APP_NAME=$tag_off \
  bash run-bench.sh $tag_off > ${tag_off}.stdout.log 2>&1
echo "=== $(date) tpcds 6T preshuffle OFF done ==="

# ---------- ON gated ----------
tag_on=preshuffle-gate-6t-on-gated${Q_TAG}
echo "=== $(date) tpcds 6T preshuffle ON (gated ratio=${MIN_RATIO}, minRows=${MIN_ROWS}, ${QUERIES:-full-suite}) starting ==="
EXTRA_CONF="${COMMON_LOG_OPTS} ${STATS_CONF} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.enabled=true \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.requireStats=${REQUIRE_STATS} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.minRows=${MIN_ROWS} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.minRatio=${MIN_RATIO}" \
  ${QUERIES:+QUERIES=$QUERIES} \
  APP_NAME=$tag_on \
  bash run-bench.sh $tag_on > ${tag_on}.stdout.log 2>&1
echo "=== $(date) tpcds 6T preshuffle ON done ==="

# ---------- Summary ----------
for tag in $tag_off $tag_on; do
  inserted=$(grep -c "InsertPreShufflePartialAggRule: inserted FlushableHashAggregate" $tag/log.txt 2>/dev/null)
  passed=$(grep -c "InsertPreShufflePartialAggRule: stats gate passed" $tag/log.txt 2>/dev/null)
  skipped_rows=$(grep -c "InsertPreShufflePartialAggRule: skipped, rows=" $tag/log.txt 2>/dev/null)
  skipped_ratio=$(grep -c "InsertPreShufflePartialAggRule: skipped, rows/ndv=" $tag/log.txt 2>/dev/null)
  missing=$(grep -c "InsertPreShufflePartialAggRule: stats missing" $tag/log.txt 2>/dev/null)
  total_secs=$(awk '{s+=$2} END{printf "%.1f", s}' $tag/results.txt 2>/dev/null)
  echo "$tag: inserted=$inserted stats_passed=$passed skipped_rows=$skipped_rows skipped_ratio=$skipped_ratio stats_missing=$missing total=${total_secs}s"
done
