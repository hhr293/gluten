---
layout: page
title: Pre-Shuffle Partial Aggregate (stats-driven)
nav_order: 20
parent: /developer-overview/
---

# Pre-shuffle partial aggregate optimization

For shuffles whose payload is *exactly* the partitioning key (typical of
LeftSemi / LeftAnti build sides and INTERSECT-style distinct patterns), Velox
already ships the rows through the network unaggregated. When the fact side of
a join emits many duplicate keys, that shuffle carries a lot of redundant
traffic that a partial dedup before the shuffle could eliminate.

`InsertPreShufflePartialAggRule` inserts a `FlushableHashAggregateExecTransformer`
just below such a shuffle so the shuffle sees pre-deduped rows. It fires only
when a stats-driven gate confirms the shuffle subtree has enough rows and
enough duplication per key to pay for the hash-table build.

## When it fires

All of the following must hold:

  * The exchange's data columns (excluding `hash_partition_key`) exactly match
    the partitioning base attributes -- the payload is only the partition key.
  * The subtree contains at least one join (structural signal for high
    duplication).
  * The exchange is not the source of any `ReusedExchangeExec` elsewhere in
    the plan.
  * The stats gate passes: `rowCount >= minRows` and `rowCount / groupNdv >= minRatio`.

## Stats source: driver-local TSV

The rule does *not* rely on `spark.sql.cbo.enabled` or on Spark's per-node stats
propagation. It reads a plain TSV file whose path is set by
`spark.gluten.sql.columnar.preShufflePartialAgg.statsFile`.

Format, one line per table:

```
table<TAB>rowCount<TAB>col1:ndv1,col2:ndv2,...
```

Reading from a driver-local file lets the loader work whether tables are Hive
tables or tempViews created from `spark.read.parquet(path)` -- the rule only
needs the physical scan's table name (or the trailing segment of its root path)
to look up the entry.

## Confs

| Conf | Default | Meaning |
| --- | --- | --- |
| `spark.gluten.sql.columnar.preShufflePartialAgg.enabled` | `false` | Turn the rule on. |
| `spark.gluten.sql.columnar.preShufflePartialAgg.statsFile` | *unset* | Path (driver-local) to the TSV file. |
| `spark.gluten.sql.columnar.preShufflePartialAgg.minRows` | `10000000` | Minimum row count on the fact side to consider dedup. |
| `spark.gluten.sql.columnar.preShufflePartialAgg.minRatio` | `4.0` | Required rows/groupNdv ratio. |
| `spark.gluten.sql.columnar.preShufflePartialAgg.requireStats` | `false` | Fail-closed when stats are missing (skip instead of falling back). |

## End-to-end usage

The `dev/preshuffle` directory ships the scripts that dump the stats file and
run the benchmark, both at 100G and 6T.

### 1. Analyze the tables

Run `ANALYZE TABLE ... COMPUTE STATISTICS` (table-level) on your fact tables and
`ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` on your dim tables. Only the
dim NDVs actually feed the gate: fact-key NDV is bounded by the joined dim's PK
NDV, so we skip the fact-side per-column ANALYZE (which is the expensive part).

### 2. Dump the catalog to a TSV

```bash
# 100G (defaults: STATS_DB=tpcds_analyze, STATS_OUT=preshuffle-stats-tpcds-100g.tsv)
bash dev/preshuffle/run-dump-tpcds-stats-tsv.sh

# 6T
bash dev/preshuffle/run-dump-tpcds-stats-tsv-6t.sh
```

Each script shells out to `dev/preshuffle/dump-tpcds-stats-tsv.scala`, which reads
`DESCRIBE EXTENDED <table>` and `DESCRIBE EXTENDED <table> <col>` for each table
in the catalog DB and emits one TSV line per table.

### 3. Run the benchmark

```bash
# 100G, q14a smoke
bash dev/preshuffle/run-preshuffle-gate-100g.sh

# 100G, rule OFF baseline
bash dev/preshuffle/run-preshuffle-gate-100g-off.sh

# 6T, A/B (full 108-query catalog by default; QUERIES=q14a for smoke)
bash dev/preshuffle/run-preshuffle-gate-6t.sh
```

Each script wires `--conf spark.gluten.sql.columnar.preShufflePartialAgg.*` and
then greps `log.txt` for the rule's `inserted / stats_passed / skipped_rows /
skipped_ratio / stats_missing` messages.

## Tuning the gate

`minRows` should sit above the point where dedup starts to pay: for 100G the
default 1e7 is roughly right for `store_sales`; for 6T bump it to 1e8. `minRatio`
should be well above 1 (a ratio of 1 means every row already has a distinct
key). The 6T script defaults to `minRatio=10` because at that scale even mild
duplication moves noticeable amounts of bytes off the wire.
