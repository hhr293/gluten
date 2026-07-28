#!/bin/bash
# Baseline: 100G lowbcast, preshuffle rule OFF, q14a only.
set +e
cd /home/pnp/gluten-tools/bench

LOG4J_CONF=/home/pnp/gluten-tools/bench/log4j2-preshuffle-gate.properties
COMMON_LOG_OPTS="--files ${LOG4J_CONF}"
export EXTRA_DRIVER_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF}) -Dderby.system.home=/tmp/derby"
export EXTRA_EXECUTOR_JAVA_OPTS="-Dlog4j2.configurationFile=$(basename ${LOG4J_CONF})"

LOWBCAST_CONF="--conf spark.sql.autoBroadcastJoinThreshold=1048576"

echo "=== $(date) tpcds 100G preshuffle OFF (lowbcast, q14a only) starting ==="
EXTRA_CONF="${COMMON_LOG_OPTS} ${LOWBCAST_CONF} \
  --conf spark.gluten.sql.columnar.preShufflePartialAgg.enabled=false" \
  QUERIES=q14a \
  APP_NAME=preshuffle-gate-100g-off-lowbcast-q14a \
  bash run-bench.sh preshuffle-gate-100g-off-lowbcast-q14a > preshuffle-gate-100g-off-lowbcast-q14a.stdout.log 2>&1
echo "=== $(date) tpcds 100G preshuffle OFF (lowbcast, q14a only) done ==="

tag=preshuffle-gate-100g-off-lowbcast-q14a
inserted=$(grep -c "InsertPreShufflePartialAggRule: inserted FlushableHashAggregate" $tag/log.txt 2>/dev/null)
total_secs=$(awk '{s+=$2} END{printf "%.1f", s}' $tag/results.txt 2>/dev/null)
echo "$tag: inserted=$inserted total=${total_secs}s"
