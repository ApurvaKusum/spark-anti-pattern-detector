package org.example.analyzer.rules

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.example.analyzer.reporting.AntiPatternReporter

/**
 * This rule detects smallFiles issues while reading the dataset.
 * Calculates average file size from HadoopFsRelation
 * to detect high I/O overhead caused by fragmented small files (<16MB)
 */
object CheckSmallFiles extends CustomRule {

  override def apply(qe: QueryExecution): Unit = {
    val optimizedPlan = qe.optimizedPlan

    optimizedPlan.collect {
      case _ @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
        val numFiles = fsRelation.location.inputFiles.length
        val totalSizeOfTable = fsRelation.location.sizeInBytes

        if (numFiles > 0) {
          val avgSize = (totalSizeOfTable/numFiles)/(1024*1024)
          if (numFiles > 100 && avgSize < 16){
            AntiPatternReporter.logAlert(s"!!! Anti-Pattern: Small Files Detected !!!")
            AntiPatternReporter.logAlert(s"Total Files : $numFiles")
            AntiPatternReporter.logAlert(s"Total File Size: $avgSize MB")
          }
        }
    }
  }
}
