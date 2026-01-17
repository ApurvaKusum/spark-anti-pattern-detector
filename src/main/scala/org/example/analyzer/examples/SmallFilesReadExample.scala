package org.example.analyzer.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.example.analyzer.listeners.{SQLQueryListener, TaskMetricListener}
import org.example.analyzer.reporting.AntiPatternReporter

object SmallFilesReadExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Anti Pattern")
      .master("local[*]")
      .getOrCreate()

    spark.listenerManager.register(SQLQueryListener)
    spark.sparkContext.addSparkListener(TaskMetricListener)

    val smallFileData = spark.range(0, 1000)
      .withColumn("data", lit("repro_small_file_pattern"))
      .repartition(200)

    val readPath = "/tmp/anti_pattern_small_files"
    smallFileData.write.mode("overwrite").parquet(readPath)

    println("Running Read Test...")
    spark.read.parquet(readPath).count()

    Thread.sleep(2000)
    AntiPatternReporter.printFinalReport()
  }
}
