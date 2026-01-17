package org.example.analyzer.examples

import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.analyzer.listeners.{SQLQueryListener, TaskMetricListener}
import org.example.analyzer.reporting.AntiPatternReporter

object SkewDetectionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Anti Pattern")
      .master("local[*]")
      .getOrCreate()

    spark.listenerManager.register(SQLQueryListener)
    spark.sparkContext.addSparkListener(TaskMetricListener)

    val groupCounts = Seq(
      ("groupA", 40000),
      ("groupB", 5000),
      ("groupC", 5000),
      ("groupD", 2000),
      ("groupE", 500)
    )

    var startId = 0L
    var dfs = Seq.empty[DataFrame]

    for ((group, count) <- groupCounts) {
      val df = spark.range(startId, startId + count)
        .withColumn("group_key", lit(group))
        .withColumn("value", (rand() * 1000).cast("int"))
        .select(col("id").cast("int"), col("group_key"), col("value"))

      dfs = dfs :+ df
      startId += count
    }

    val skewedDf = dfs.reduce(_ union _)
    skewedDf.groupBy("group_key").count().show()

    AntiPatternReporter.printFinalReport()
  }
}
