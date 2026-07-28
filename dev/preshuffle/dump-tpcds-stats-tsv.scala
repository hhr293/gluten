// Dump tpcds_analyze catalog stats to a driver-local TSV for the preshuffle rule.
//
// Output format, one line per table:
//   table<TAB>rowCount<TAB>col1:ndv1,col2:ndv2,...
//
// Fact tables have rowCount but no column NDVs (we only ran table-level ANALYZE on
// them); their third column is empty. Dim tables have both.
//
// Reads db name and output path from Spark confs so the run script can wire them.

import java.io.PrintWriter

val db = sc.getConf.get("spark.driver.bench.stats-db", "tpcds_analyze")
val out = sc.getConf.get("spark.driver.bench.stats-out", "/tmp/preshuffle-stats.tsv")

spark.sql(s"USE $db")

val tables = spark.sql("SHOW TABLES").collect().map(_.getString(1))
println(s"=== dumping ${tables.length} tables from $db -> $out ===")

val pw = new PrintWriter(out)
pw.println("# table\trowCount\tcol1:ndv1,col2:ndv2,...")

for (t <- tables.sorted) {
  val describeRows = spark.sql(s"DESCRIBE EXTENDED $t").collect()
  val statsField = describeRows
    .find(r => r.getString(0) == "Statistics")
    .map(_.getString(1))
    .getOrElse("")
  val rowCount = "([0-9]+) rows".r.findFirstMatchIn(statsField).map(_.group(1).toLong).getOrElse(0L)
  if (rowCount == 0L) {
    println(s"    $t: no rowCount -- skip")
  } else {
    // Column names from the schema; DESCRIBE EXTENDED <table> <col> exposes distinct_count.
    val cols = spark.table(t).schema.fields.map(_.name)
    val colNdv = cols.flatMap {
      c =>
        try {
          val colRows = spark.sql(s"DESCRIBE EXTENDED $t $c").collect()
          val dc = colRows
            .find(r => r.getString(0) == "distinct_count")
            .map(_.getString(1))
          dc.filter(_.nonEmpty).map(v => s"$c:$v")
        } catch {
          case _: Throwable => None
        }
    }
    val colStr = colNdv.mkString(",")
    pw.println(s"$t\t$rowCount\t$colStr")
    println(s"    $t: rows=$rowCount cols=${colNdv.length}")
  }
}
pw.close()
println(s"=== wrote $out ===")
System.exit(0)
