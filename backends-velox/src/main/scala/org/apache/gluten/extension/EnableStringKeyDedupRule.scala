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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.types.StringType

/**
 * Tags `FlushableHashAggregateExecTransformer` operators that would benefit from Velox's string-key
 * dedup. Reads the driver-local preshuffle stats TSV, for each Flushable partial agg with
 * StringType grouping keys computes rowCount / groupNdv over the subtree's scans, and if the ratio
 * meets the threshold replaces the agg with a
 * [[FlushableStringKeyDedupHashAggregateExecTransformer]]. That subclass emits `stringKeyDedup=1`
 * in the substrait AggregateRel advisory extension; the plan converter side flips the query config
 * for the executing stage.
 */
case class EnableStringKeyDedupRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = VeloxConfig.get
    val statsPath = conf.preShufflePartialAggStatsFile
    if (statsPath.isEmpty) return plan
    val userEnabled = session.conf.get(VeloxConfig.STRING_KEY_DEDUP_ENABLED.key, "false").toBoolean
    if (userEnabled) return plan

    val stats = PreShufflePartialAggStats.loadStats(statsPath)
    if (stats.isEmpty) return plan

    val minRatio = conf.stringKeyDedupMinRatio
    val minRows = conf.stringKeyDedupMinRows

    var replaced = 0
    val out = plan.transformUp {
      case agg: FlushableHashAggregateExecTransformer =>
        if (!hasStringGroupingKey(agg)) {
          agg
        } else {
          val stringKeys = agg.groupingExpressions
            .flatMap(_.references.toSeq)
            .filter(_.dataType == StringType)
            .distinct
          if (stringKeys.nonEmpty && passesStatsGate(agg, stringKeys, stats, minRows, minRatio)) {
            replaced += 1
            logInfo(
              s"EnableStringKeyDedupRule: tagging flushable agg for string-key dedup " +
                s"(keys=[${stringKeys.map(_.name).mkString(",")}])")
            FlushableStringKeyDedupHashAggregateExecTransformer(
              agg.requiredChildDistributionExpressions,
              agg.groupingExpressions,
              agg.aggregateExpressions,
              agg.aggregateAttributes,
              agg.initialInputBufferOffset,
              agg.resultExpressions,
              agg.child
            )
          } else {
            agg
          }
        }
    }
    if (replaced > 0) {
      logInfo(s"EnableStringKeyDedupRule: replaced=$replaced flushable aggs")
    }
    out
  }

  private def hasStringGroupingKey(agg: HashAggregateExecTransformer): Boolean = {
    agg.groupingExpressions.exists(_.dataType == StringType)
  }

  /**
   * Passes when the agg's underlying scans report rowCount >= minRows and rowCount / groupNdv >=
   * minRatio for the tuple of string grouping keys. NDV is estimated by taking, for each table in
   * the subtree, the max per-key NDV among that table's string keys (bounded by that table's
   * rowCount), then multiplying across tables. Within a table, keys are assumed correlated (e.g.
   * i_category is coarser than i_class is coarser than i_brand); across tables, independence. We DO
   * NOT cap the resulting product by the fact rowCount: the point of the gate is to compute
   * factRows / distinctGroups, and capping collapses meaningful ratios to 1.0.
   */
  private def passesStatsGate(
      agg: HashAggregateExecTransformer,
      stringKeys: Seq[Attribute],
      stats: Map[String, PreShufflePartialAggStats.TableStats],
      minRows: Long,
      minRatio: Double): Boolean = {
    val perTable = collectSubtreeStats(agg, stats)
    val factRows = perTable.map(_._2.rowCount).reduceOption(_ max _)
    val ndvEst = groupNdvEstimate(perTable, stringKeys)
    (factRows, ndvEst.product) match {
      case (Some(r), Some(n)) if r >= minRows && n > 0L =>
        val ratio = r.toDouble / n.toDouble
        if (ratio >= minRatio) {
          val keysStr = stringKeys.map(_.name).mkString(",")
          val breakdownStr = ndvEst.perTable.map { case (t, nn) => s"$t:$nn" }.mkString(",")
          logInfo(
            s"EnableStringKeyDedupRule: partial-agg gate PASSED factRows=$r ndv=$n " +
              s"ratio=$ratio perTableNdv=[$breakdownStr] (keys=[$keysStr])")
          true
        } else {
          false
        }
      case _ =>
        false
    }
  }

  private def collectSubtreeStats(
      plan: SparkPlan,
      stats: Map[String, PreShufflePartialAggStats.TableStats])
      : Seq[(String, PreShufflePartialAggStats.TableStats)] = {
    val rawScans = collectScansThroughStages(plan)
    val names = rawScans.flatMap { case (ti, path) => ti.orElse(path) }
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
        case q: QueryStageExec =>
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
   * Per-table NDV contribution (max per-key NDV among that table's string keys, bounded by table
   * rowCount) and the multiplied product across tables. Returned so the gate log can show which
   * table contributed how many distinct groups.
   */
  private case class NdvEstimate(perTable: Seq[(String, Long)], product: Option[Long])

  private def groupNdvEstimate(
      perTable: Seq[(String, PreShufflePartialAggStats.TableStats)],
      keys: Seq[Attribute]): NdvEstimate = {
    val keyNames = keys.map(_.name).toSet
    val contributions = perTable.flatMap {
      case (table, ts) =>
        val hitNdvs = ts.colNdv.iterator
          .collect { case (k, v) if keyNames.contains(k) && v > 0L => v }
          .toSeq
        if (hitNdvs.isEmpty) None
        else Some(table -> math.min(hitNdvs.max, ts.rowCount))
    }
    if (contributions.isEmpty) return NdvEstimate(Seq.empty, None)
    val product = contributions.foldLeft(1L) {
      case (acc, (_, n)) =>
        if (n <= 0L) acc
        else if (acc > Long.MaxValue / n) Long.MaxValue
        else acc * n
    }
    NdvEstimate(contributions, Some(product))
  }
}

object EnableStringKeyDedupRule {
  def apply(session: SparkSession): EnableStringKeyDedupRule = {
    new EnableStringKeyDedupRule(session)
  }
}
