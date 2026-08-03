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
 * Tags `FlushableHashAggregateExecTransformer` operators that would benefit from the
 * hash-cache-in-slot Velox patch. The patch caches the just-computed hash in row[-1] after the
 * HashTable transitions to kHash mode; the win only exists once the table is in kHash. Velox flips
 * into kHash only past ~100k distinct values (kMaxDistinct) or on unsupported valueId key types, so
 * the gate keys on estimated group NDV and requires at least one StringType key (int hashing is
 * already cheap in RowContainer::hash, so the +8B/row cost isn't recovered).
 *
 * The rule reads the same driver-local preshuffle stats TSV as EnableStringKeyDedupRule and uses
 * the same NDV estimator (per-table max bounded by rowCount, then product across tables). On a
 * passing gate, the agg is replaced with a [[FlushableHashCacheInSlotHashAggregateExecTransformer]]
 * whose substrait AggregateRel emits `hashCacheInSlot=1`; SubstraitToVeloxPlan flips the per-stage
 * query config.
 */
case class EnableHashCacheInSlotRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = VeloxConfig.get
    val statsPath = conf.preShufflePartialAggStatsFile
    if (statsPath.isEmpty) return plan
    val userEnabled =
      session.conf.get(VeloxConfig.HASH_CACHE_IN_SLOT_ENABLED.key, "false").toBoolean
    if (userEnabled) return plan

    val stats = PreShufflePartialAggStats.loadStats(statsPath)
    if (stats.isEmpty) return plan

    val minRows = conf.hashCacheInSlotMinRows
    val minNdv = conf.hashCacheInSlotMinNdv

    var replaced = 0
    val out = plan.transformUp {
      case agg: FlushableHashAggregateExecTransformer =>
        val hasStr = agg.groupingExpressions.exists(_.dataType == StringType)
        if (!hasStr) {
          agg
        } else {
          val allKeys = agg.groupingExpressions
            .flatMap(_.references.toSeq)
            .distinct
          if (allKeys.nonEmpty && passesStatsGate(agg, allKeys, stats, minRows, minNdv)) {
            replaced += 1
            logInfo(
              s"EnableHashCacheInSlotRule: tagging flushable agg for hash-cache-in-slot " +
                s"(keys=[${allKeys.map(_.name).mkString(",")}])")
            FlushableHashCacheInSlotHashAggregateExecTransformer(
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
      logInfo(s"EnableHashCacheInSlotRule: replaced=$replaced flushable aggs")
    }
    out
  }

  /**
   * Passes when the agg's underlying scans report rowCount >= minRows AND estimated group NDV >=
   * minNdv. NDV is estimated with the same helper as EnableStringKeyDedupRule: per-table max
   * per-key NDV bounded by that table's rowCount, then multiplied across tables (correlated within
   * a table, independent across tables).
   */
  private def passesStatsGate(
      agg: HashAggregateExecTransformer,
      keys: Seq[Attribute],
      stats: Map[String, PreShufflePartialAggStats.TableStats],
      minRows: Long,
      minNdv: Long): Boolean = {
    val perTable = collectSubtreeStats(agg, stats)
    val factRows = perTable.map(_._2.rowCount).reduceOption(_ max _)
    val ndvEst = groupNdvEstimate(perTable, keys)
    (factRows, ndvEst.product) match {
      case (Some(r), Some(n)) if r >= minRows && n >= minNdv =>
        val keysStr = keys.map(_.name).mkString(",")
        val breakdownStr = ndvEst.perTable.map { case (t, nn) => s"$t:$nn" }.mkString(",")
        logInfo(
          s"EnableHashCacheInSlotRule: partial-agg gate PASSED factRows=$r ndv=$n " +
            s"perTableNdv=[$breakdownStr] (keys=[$keysStr])")
        true
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

object EnableHashCacheInSlotRule {
  def apply(session: SparkSession): EnableHashCacheInSlotRule = {
    new EnableHashCacheInSlotRule(session)
  }
}
