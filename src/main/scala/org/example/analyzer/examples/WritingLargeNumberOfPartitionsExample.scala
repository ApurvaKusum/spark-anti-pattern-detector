package org.example.analyzer.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, rand}
import org.example.analyzer.listeners.{SQLQueryListener, TaskMetricListener}
import org.example.analyzer.reporting.AntiPatternReporter

object WritingLargeNumberOfPartitionsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Anti Pattern")
      .master("local[*]")
      .getOrCreate()

    spark.listenerManager.register(SQLQueryListener)
    spark.sparkContext.addSparkListener(TaskMetricListener)

    val overPartitionedData = spark.range(0, 100)
      .withColumn("year", lit(2023))
      .withColumn("month", lit(10))
      .withColumn("day", lit(27))
      .withColumn("unique_id_skew", rand())

    println("Running Write Test...")
    overPartitionedData.write
      .mode("overwrite")
      .partitionBy("year", "month", "day", "unique_id_skew")
      .parquet("/tmp/anti_pattern_over_partition")

    Thread.sleep(2000)
    AntiPatternReporter.printFinalReport()
  }
}
