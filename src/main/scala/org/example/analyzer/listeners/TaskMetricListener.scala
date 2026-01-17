package org.example.analyzer.listeners

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.example.analyzer.reporting.AntiPatternReporter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskMetricListener extends  SparkListener{

  private val executorRunTime = new mutable.HashMap[Int, mutable.ArrayBuffer[Long]]

  /**
   * Custom method for finding median of an Array
   */
  private def calculateMedian(buffer: ArrayBuffer[Long]): Double = {
    if (buffer.isEmpty) return 0.0
    val sorted = buffer.sorted
    val size = sorted.size
    if (size % 2 == 1) {
      sorted(size / 2).toDouble
    } else {
      val middle1 = sorted(size / 2 - 1)
      val middle2 = sorted(size / 2)
      (middle1 + middle2) / 2.0
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    // We are storing executorRunTime for every task corresponding to it's stageId which would
    // later be used for detecting skew.
    val taskRunTime = taskEnd.taskMetrics.executorRunTime

    if (executorRunTime.contains(taskEnd.stageId)) {
      executorRunTime(taskEnd.stageId) += taskRunTime
    } else {
      executorRunTime +=  (taskEnd.stageId -> new mutable.ArrayBuffer[Long])
      executorRunTime(taskEnd.stageId) += taskRunTime
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val maxTaskRunTime = executorRunTime(stageCompleted.stageInfo.stageId).max

    val medianTaskRunTime = calculateMedian(executorRunTime(stageCompleted.stageInfo.stageId))

    if (maxTaskRunTime > (medianTaskRunTime * 5 )) {
      AntiPatternReporter.logAlert("Wide Data Skew Alert")
    }
  }
}
