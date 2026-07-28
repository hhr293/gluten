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

import org.apache.spark.internal.Logging

import scala.io.Source

/**
 * Driver-local loader for pre-shuffle statistics supplied via a plain TSV file.
 *
 * Rules that want to size or skip pre-shuffle work based on real rowCount / NDV numbers can call
 * [[PreShufflePartialAggStats.loadStats]] with the value of the
 * `spark.gluten.sql.columnar.preShufflePartialAgg.statsFile` conf. The file is parsed once per
 * driver JVM and cached by path, so subsequent lookups are Map reads.
 *
 * Reading stats from a driver-local file lets a rule bypass Spark's per-node stats propagation
 * and the Spark catalog entirely: it works whether the tables are loaded as tempViews
 * (LogicalRelation without catalogTable) or as Hive tables (LogicalRelation with catalogTable).
 * The caller only needs the physical scan's table name to look up the entry.
 */
object PreShufflePartialAggStats extends Logging {

  /** Per-table rowCount and column NDV loaded from the driver-local stats file. */
  case class TableStats(rowCount: Long, colNdv: Map[String, Long])

  private val cache = new java.util.concurrent.ConcurrentHashMap[String, Map[String, TableStats]]()

  /**
   * Return the parsed stats for the given path, or an empty Map if `pathOpt` is empty. Parses on
   * first call per path; subsequent calls hit the cache.
   */
  def loadStats(pathOpt: Option[String]): Map[String, TableStats] = pathOpt match {
    case None => Map.empty
    case Some(path) =>
      val cached = cache.get(path)
      if (cached != null) return cached
      cache.computeIfAbsent(path, p => parseStatsFile(p))
  }

  /**
   * TSV format, one line per table: `table<TAB>rowCount<TAB>col1:ndv1,col2:ndv2,...`. The third
   * column is optional (fact tables often need only rowCount). Lines beginning with `#` and blank
   * lines are skipped. Malformed lines log a warning and are ignored.
   */
  private def parseStatsFile(path: String): Map[String, TableStats] = {
    val src = Source.fromFile(path)
    try {
      val entries = src.getLines().flatMap {
        raw =>
          val line = raw.trim
          if (line.isEmpty || line.startsWith("#")) None
          else {
            val cols = line.split("\t", -1)
            if (cols.length < 2) {
              logWarning(s"preShufflePartialAgg statsFile: skip malformed line: $raw")
              None
            } else {
              try {
                val table = cols(0)
                val rows = cols(1).toLong
                val ndv = if (cols.length >= 3 && cols(2).nonEmpty) {
                  cols(2).split(",").flatMap {
                    kv =>
                      val idx = kv.indexOf(':')
                      if (idx <= 0 || idx == kv.length - 1) None
                      else {
                        try Some(kv.substring(0, idx) -> kv.substring(idx + 1).toLong)
                        catch { case _: NumberFormatException => None }
                      }
                  }.toMap
                } else Map.empty[String, Long]
                Some(table -> TableStats(rows, ndv))
              } catch {
                case _: NumberFormatException =>
                  logWarning(s"preShufflePartialAgg statsFile: skip malformed line: $raw")
                  None
              }
            }
          }
      }.toMap
      logInfo(s"preShufflePartialAgg: loaded ${entries.size} table stats from $path")
      entries
    } finally {
      src.close()
    }
  }
}
