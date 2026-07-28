/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ShuffleExchangeLike}

/**
 * Inserts a FlushableHashAggregateExecTransformer (partial DISTINCT) before a ColumnarExchange when
 * the shuffle payload is exactly the partitioning key and the subtree contains a join. This is a
 * structural heuristic: `payload == partition key` strongly suggests the downstream consumer only
 * needs the distinct set of key values (LeftSemi/LeftAnti build sides, INTERSECT-style distinct
 * aggregates), because the physical planner has already stripped every non-key column.
 *
 * Eligibility conditions (all must hold):
 *   - The exchange's data columns (excluding hash_partition_key) exactly match the partitioning
 *     base attributes. Shuffle payload is only the partitioning key.
 *   - No aggregate already exists in the shuffle-local subtree (avoid stacking; also acts as a
 *     sentinel that blocks the count/sum-over-key case since Spark inserts a partial agg directly
 *     under such shuffles).
 *   - The subtree contains at least one join (structural signal for high-duplication data; without
 *     a join, dedup rarely pays).
 *   - The exchange is not referenced by any [[ReusedExchangeExec]] in the whole plan. Avoids
 *     changing the row stream for other consumers whose semantics may differ.
 */
case class InsertPreShufflePartialAggRule(session: SparkSession)
  extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = VeloxConfig.get
    if (!conf.enablePreShufflePartialAgg) {
      return plan
    }
    val minRows = conf.preShufflePartialAggMinRows
    val minRatio = conf.preShufflePartialAggMinRatio
    val requireStats = conf.preShufflePartialAggRequireStats
    val stats = PreShufflePartialAggStats.loadStats(conf.preShufflePartialAggStatsFile)
    val reusedExchanges = collectReusedExchanges(plan)
    plan.transformUpWithPruning(_.containsPattern(EXCHANGE)) {
      case exchange: ShuffleExchangeLike if isEligible(exchange, reusedExchanges) =>
        val dataColumns = getDataColumns(exchange)
        if (!passesStatsGate(exchange, dataColumns, minRows, minRatio, requireStats, stats)) {
          exchange
        } else {
          val newChild = insertAggBelow(exchange.child)
          if (newChild ne exchange.child) {
            val keyNames = dataColumns.map(_.name).mkString(",")
            logInfo(
              s"InsertPreShufflePartialAggRule: inserted FlushableHashAggregate before " +
                s"${exchange.getClass.getSimpleName} on keys=[$keyNames]")
            exchange.withNewChildren(List(newChild))
          } else {
            exchange
          }
        }
      case other => other
    }
  }

  /**
   * Returns true if the exchange subtree passes the row-count and rows/groupNdv gate.
   *
   * Reads statistics from a driver-local TSV file specified by
   * `spark.gluten.sql.columnar.preShufflePartialAgg.statsFile`, bypassing Spark's per-node stats
   * propagation and the Spark catalog entirely. This avoids requiring `spark.sql.cbo.enabled` and
   * works whether tables are loaded as tempViews (LogicalRelation without catalogTable) or as Hive
   * tables (LogicalRelation with catalogTable) -- the rule only needs the physical scan's
   * `tableIdentifier.table` name to look up the file entry.
   *
   * When statistics are missing, behavior depends on `requireStats`:
   *   - requireStats=false (default): fall through, keep old structural-only behavior
   *   - requireStats=true: skip insertion (fail-closed)
   */
  private def passesStatsGate(
      exchange: ShuffleExchangeLike,
      dataColumns: Seq[Attribute],
      minRows: Long,
      minRatio: Double,
      requireStats: Boolean,
      stats: Map[String, PreShufflePartialAggStats.TableStats]): Boolean = {
    val perTable = collectSubtreeStats(exchange, stats)
    val rows = perTable.map(_._2.rowCount).reduceOption(_ max _)
    val ndv = groupNdvAcross(perTable.map(_._2), dataColumns)
    val keysStr = dataColumns.map(_.name).mkString(",")
    (rows, ndv) match {
      case (Some(r), Some(n)) =>
        if (r < minRows) {
          logInfo(
            s"InsertPreShufflePartialAggRule: skipped, rows=$r < minRows=$minRows " +
              s"(keys=[$keysStr])")
          false
        } else if (n <= 0L) {
          logInfo(
            s"InsertPreShufflePartialAggRule: stats gate ndv=$n non-positive, " +
              s"fallback requireStats=$requireStats (rows=$r keys=[$keysStr])")
          !requireStats
        } else {
          val ratio = r.toDouble / n.toDouble
          if (ratio < minRatio) {
            logInfo(
              s"InsertPreShufflePartialAggRule: skipped, rows/ndv=$ratio < minRatio=$minRatio " +
                s"(rows=$r ndv=$n keys=[$keysStr])")
            false
          } else {
            logInfo(
              s"InsertPreShufflePartialAggRule: stats gate passed rows=$r ndv=$n ratio=$ratio " +
                s"(keys=[$keysStr])")
            true
          }
        }
      case _ =>
        logInfo(
          s"InsertPreShufflePartialAggRule: stats missing rows=$rows ndv=$ndv, " +
            s"fallback requireStats=$requireStats (keys=[$keysStr])")
        !requireStats
    }
  }

  /**
   * Walks the physical subtree, gathers every scan's table name, and pairs it with the pre-loaded
   * [[PreShufflePartialAggStats.TableStats]] entry (if any). The name is taken from
   * `tableIdentifier.table` when present (Hive-catalog tables) and falls back to the last segment
   * of the scan's root path when absent (tempViews created from `spark.read.parquet(path)` -- no
   * CatalogTable is attached). Fact and dim tables typically live on separate sides of a join;
   * we want both, then take the max rowCount (fact side proxy for upstream shuffle rows) and probe
   * every table's colNdv for each grouping key's NDV (dim side proxy for output group cardinality).
   */
  private def collectSubtreeStats(
      plan: SparkPlan,
      stats: Map[String, PreShufflePartialAggStats.TableStats])
      : Seq[(String, PreShufflePartialAggStats.TableStats)] = {
    // Under AQE, the subtree under the current exchange is often just QueryStageExec/
    // ReusedExchangeExec references pointing to already-materialised prior stages.
    // We drill through those references so we can still see the underlying scans.
    val names = collectScansThroughStages(plan).flatMap { case (ti, path) => ti.orElse(path) }
    names.flatMap(name => stats.get(name).map(name -> _))
  }

  private def collectScansThroughStages(
      plan: SparkPlan): Seq[(Option[String], Option[String])] = {
    val seen = scala.collection.mutable.Set.empty[Int]
    val out = scala.collection.mutable.ArrayBuffer.empty[(Option[String], Option[String])]
    def visit(p: SparkPlan): Unit = {
      if (p == null || !seen.add(System.identityHashCode(p))) return
      p match {
        case s: DatasourceScanTransformer =>
          out += ((s.tableIdentifier.map(_.table), scanNameFromRelation(s.relation)))
        case s: org.apache.spark.sql.execution.FileSourceScanExec =>
          out += ((s.tableIdentifier.map(_.table), scanNameFromRelation(s.relation)))
        case q: org.apache.spark.sql.execution.adaptive.QueryStageExec =>
          visit(q.plan)
        case r: ReusedExchangeExec =>
          visit(r.child)
        case _ =>
      }
      p.children.foreach(visit)
    }
    visit(plan)
    out.toSeq
  }

  /**
   * Extracts the trailing path segment from a [[HadoopFsRelation]]'s root -- e.g.
   * `hdfs://host:port/parquet_tpcds_100/store_sales` -> `store_sales`. This is a best-effort
   * fallback for tempView scans that never went through a CatalogTable.
   */
  private def scanNameFromRelation(
      relation: org.apache.spark.sql.sources.BaseRelation): Option[String] = relation match {
    case hfs: org.apache.spark.sql.execution.datasources.HadoopFsRelation =>
      val paths = hfs.location.rootPaths
      if (paths.isEmpty) None
      else {
        val last = paths.head.getName
        if (last.nonEmpty) Some(last) else None
      }
    case _ => None
  }

  /**
   * Upper-bound NDV for the grouping key tuple, searched across every collected table's colNdv. A
   * key's column NDV is typically in the *dim* table (e.g. `c_customer_sk` in customer,
   * `i_brand_id` in item), even though the fact-side scan contributes rowCount. If a key cannot be
   * found in any table's colNdv, returns None.
   */
  private def groupNdvAcross(
      tableStats: Seq[PreShufflePartialAggStats.TableStats],
      keys: Seq[Attribute]): Option[Long] = {
    val maxRowCap = tableStats.map(_.rowCount).reduceOption(_ max _)
    val perColumn = keys.map {
      a => tableStats.iterator.flatMap(_.colNdv.get(a.name)).reduceOption(_ min _)
    }
    if (perColumn.exists(_.isEmpty)) return None
    val product = perColumn.flatten.foldLeft(1L) {
      (acc, n) =>
        if (n <= 0L) acc
        else if (acc > Long.MaxValue / n) Long.MaxValue
        else acc * n
    }
    Some(math.min(product, maxRowCap.getOrElse(Long.MaxValue)))
  }

  private def collectReusedExchanges(plan: SparkPlan): Set[Exchange] = {
    val buf = scala.collection.mutable.Set.empty[Exchange]
    plan.foreach {
      case r: ReusedExchangeExec => buf += r.child
      case _ =>
    }
    buf.toSet
  }

  private def isEligible(
      exchange: ShuffleExchangeLike,
      reusedExchanges: Set[Exchange]): Boolean = {
    // Reuse guard: skip exchanges that are the source of a ReusedExchangeExec elsewhere in
    // the plan -- dedup here would alter the row stream seen by other consumers, whose
    // semantics may not tolerate it.
    if (reusedExchanges.contains(exchange)) return false

    val dataColumns = getDataColumns(exchange)
    if (dataColumns.isEmpty) return false

    val partBaseAttrs = getPartitioningBaseAttributes(exchange)
    if (partBaseAttrs.isEmpty) return false

    val dataAttrIds = dataColumns.map(_.exprId).toSet
    val partAttrIds = partBaseAttrs.map(_.exprId).toSet
    if (dataAttrIds != partAttrIds) return false

    val subtree = exchange.child
    if (hasExistingAggregate(subtree)) return false
    if (!hasJoinBelow(subtree)) return false

    true
  }

  private def getDataColumns(exchange: ShuffleExchangeLike): Seq[Attribute] = {
    exchange.child.output.filterNot(_.name.startsWith("hash_partition_key"))
  }

  private def getPartitioningBaseAttributes(exchange: ShuffleExchangeLike): Seq[Attribute] = {
    exchange.outputPartitioning match {
      case hp: HashPartitioning =>
        hp.expressions.flatMap(_.references.toSeq).distinct
      case _ => Seq.empty
    }
  }

  // Both hasExistingAggregate and hasJoinBelow stop at inner ShuffleExchangeLike boundaries.
  // Anything below an inner shuffle belongs to a separate shuffle region and is irrelevant to
  // whether pre-shuffle dedup at THIS exchange is safe or worthwhile.
  private def hasExistingAggregate(plan: SparkPlan): Boolean = {
    plan match {
      case _: ShuffleExchangeLike => false
      case _: HashAggregateExecTransformer => true
      case _ => plan.children.exists(hasExistingAggregate)
    }
  }

  private def hasJoinBelow(plan: SparkPlan): Boolean = {
    plan match {
      case _: ShuffleExchangeLike => false
      case _: HashJoinLikeExecTransformer => true
      case _ => plan.children.exists(hasJoinBelow)
    }
  }

  /**
   * Walks from the exchange child downward through VeloxResizeBatchesExec to find the
   * hash_partition_key ProjectExecTransformer. Inserts a FlushableHashAggregateExecTransformer as
   * the child of that project, grouping on the data columns.
   */
  private def insertAggBelow(plan: SparkPlan): SparkPlan = {
    plan match {
      case resize: VeloxResizeBatchesExec =>
        val newChild = insertAggBelow(resize.child)
        if (newChild ne resize.child) {
          resize.withNewChildren(List(newChild)).asInstanceOf[SparkPlan]
        } else {
          resize
        }

      case project: ProjectExecTransformer if hasHashPartitionKey(project) =>
        val childOutput = project.child.output
        val dataColumns = project.output.filterNot(_.name.startsWith("hash_partition_key"))
        val groupingAttrs = dataColumns.flatMap {
          dc => childOutput.find(_.exprId == dc.exprId)
        }
        if (groupingAttrs.size != dataColumns.size) {
          return project
        }
        val groupingExprs: Seq[NamedExpression] = groupingAttrs
        val agg = FlushableHashAggregateExecTransformer(
          requiredChildDistributionExpressions = None,
          groupingExpressions = groupingExprs,
          aggregateExpressions = Seq.empty,
          aggregateAttributes = Seq.empty,
          initialInputBufferOffset = 0,
          resultExpressions = groupingExprs,
          child = project.child
        )
        project.withNewChildren(List(agg)).asInstanceOf[SparkPlan]

      case _ => plan
    }
  }

  private def hasHashPartitionKey(project: ProjectExecTransformer): Boolean = {
    project.output.exists(_.name.startsWith("hash_partition_key"))
  }
}

object InsertPreShufflePartialAggRule {
  def apply(session: SparkSession): InsertPreShufflePartialAggRule = {
    new InsertPreShufflePartialAggRule(session)
  }
}
