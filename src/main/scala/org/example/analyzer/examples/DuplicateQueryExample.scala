package org.example.analyzer.examples

import org.apache.spark.sql.SparkSession
import org.example.analyzer.listeners.{SQLQueryListener, TaskMetricListener}
import org.example.analyzer.reporting.AntiPatternReporter

object DuplicateQueryExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Anti Pattern")
      .master("local[*]")
      .getOrCreate()

    spark.listenerManager.register(SQLQueryListener)
    spark.sparkContext.addSparkListener(TaskMetricListener)

    import spark.implicits._

    val customers = Seq(
      (1, "alice", 20),
      (2, "Bob", 27),
      (3, "charlie", 12))
    .toDF("customer_id", "name", "age")

    println("Running count for first time: " + customers.count())
    Thread.sleep(2000)
    print("Running count for second time: " + customers.count())
    Thread.sleep(2000)

    AntiPatternReporter.printFinalReport()
  }
}
