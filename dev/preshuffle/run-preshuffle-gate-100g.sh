#!/bin/bash
# Smoke: 100G lowbcast, gated ratio=10, stats read from driver-local TSV.
# The TSV was produced by run-dump-tpcds-stats-tsv.sh from tpcds_analyze.
# Runs q14a only to verify the rule fires with file-based stats.
set +e
cd /home/pnp/gluten-tools/bench

LOG4J_CONF=/home/pnp/gluten-tools/bench/log4j2-preshuffle-gate.properties
COMMON_LOG_OPTS="--files ${LOG4J_CONF}"
export EXTRA_DRIVER_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF}) -Dderby.system.home=/tmp/derby"
export EXTRA_EXECUTOR_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF})"

LOWBCAST_CONF="--conf spark.sql.autoBroadcastJoinThreshold=1048576"
STATS_FILE=/home/pnp/gluten-tools/bench/preshuffle-stats-tpcds-100g.tsv
STATS_CONF="--conf spark.gluten.sql.columnar.preShufflePartialAgg.statsFile=${STATS_FILE}"

echo "=== $(date) tpcds 100G preshuffle ON (gated ratio=10, lowbcast, statsFile, q14a only) starting ==="
EXTRA_CONF="${COMMON_LOG_OPTS} ${LOWBCAST_CONF} ${STATS_CONF} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.enabled=true \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.requireStats=true \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.minRows=10000000 \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.minRatio=10.0" \
  QUERIES=q14a \
  APP_NAME=preshuffle-gate-100g-on-gated-lowbcast-statsfile \
  bash run-bench.sh preshuffle-gate-100g-on-gated-lowbcast-statsfile > preshuffle-gate-100g-on-gated-lowbcast-statsfile.stdout.log 2>&1
echo "=== $(date) tpcds 100G preshuffle ON (gated ratio=10, lowbcast, statsFile, q14a only) done ==="

tag=preshuffle-gate-100g-on-gated-lowbcast-statsfile
loaded=$(grep -c "preShufflePartialAgg: loaded " $tag/log.txt 2>/dev/null)
passed=$(grep -c "InsertPreShufflePartialAggRule: stats gate passed" $tag/log.txt 2>/dev/null)
inserted=$(grep -c "InsertPreShufflePartialAggRule: inserted FlushableHashAggregate" $tag/log.txt 2>/dev/null)
skipped_rows=$(grep -c "InsertPreShufflePartialAggRule: skipped, rows=" $tag/log.txt 2>/dev/null)
skipped_ratio=$(grep -c "InsertPreShufflePartialAggRule: skipped, rows/ndv=" $tag/log.txt 2>/dev/null)
missing=$(grep -c "InsertPreShufflePartialAggRule: stats missing" $tag/log.txt 2>/dev/null)
total_secs=$(awk '{s+=$2} END{printf "%.1f", s}' $tag/results.txt 2>/dev/null)
echo "$tag: loaded_msgs=$loaded inserted=$inserted stats_passed=$passed skipped_rows=$skipped_rows skipped_ratio=$skipped_ratio stats_missing=$missing total=${total_secs}s"
