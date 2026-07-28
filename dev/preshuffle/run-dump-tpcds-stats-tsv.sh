#!/bin/bash
# Dump tpcds_analyze -> TSV file for the preshuffle rule's stats loader.
set -x
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../env.sh
source ${SCRIPT_DIR}/env.sh

STATS_DB=${STATS_DB:-tpcds_analyze}
STATS_OUT=${STATS_OUT:-/home/pnp/gluten-tools/bench/preshuffle-stats-tpcds-100g.tsv}

if pgrep -f "spark-shell" > /dev/null 2>&1; then
    pkill -9 -f "spark-shell"
    sleep 2
fi

export TERM=dumb

cat ${SCRIPT_DIR}/dump-tpcds-stats-tsv.scala | setsid ${SPARK_HOME}/bin/spark-shell \
  --name dump-tpcds-stats-tsv \
  --driver-memory ${MEM_DRIVER:=20G} \
  --num-executors 1 \
  --executor-cores 8 \
  --executor-memory ${MEM_EXECUTOR} \
  --master yarn \
  --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=${NATIVE_JARS} \
  --conf spark.executor.extraClassPath=${NATIVE_JARS} \
  --conf spark.executor.memoryOverhead=${MEM_OVERHEAD} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=${MEM_OFFHEAP} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.gluten.sql.columnar.backend.lib=velox \
  --conf spark.gluten.sql.columnar.scanOnly=false \
  --conf spark.gluten.sql.columnar.backend.velox.memoryCapRatio=0.75 \
  --conf spark.log.level=WARN \
  --conf spark.driver.bench.stats-db="${STATS_DB}" \
  --conf spark.driver.bench.stats-out="${STATS_OUT}" \
  2>&1 | tee /tmp/dump-tpcds-stats-tsv.log | tail -50

echo ""
echo "===== $STATS_OUT ====="
cat $STATS_OUT
