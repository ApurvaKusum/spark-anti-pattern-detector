package org.example.analyzer.reporting

import scala.collection.mutable

/**
 * AntiPatternReporter tracks all anti-patterns detected in spark application and
 * generates final report.
 */
object AntiPatternReporter {
  private val alerts = mutable.ListBuffer[String]()

  def logAlert(alertMessage: String): Unit = synchronized {
    alerts += alertMessage
  }

  def printFinalReport(): Unit = {
    if (alerts.nonEmpty) {
      println("\n" + "="*50)
      println("      SPARK ANTI-PATTERN SUMMARY REPORT")
      println("="*50)
      alerts.distinct.foreach(alert => println(s"-> $alert"))
      println("="*50 + "\n")
    }
  }
}
