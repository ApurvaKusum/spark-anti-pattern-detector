package org.example.analyzer.rules

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.example.analyzer.reporting.AntiPatternReporter

/**
 * This rule checks if our output table is Over Partitioned.
 * Ideally we should consider partitioning our tables with 3 partition levels or less. We raise alert whenever
 * a table have more than 3 layer partitioning. And we are also checking the column type of the columns on
 * which we are trying to partition
 */
object CheckOverPartition extends CustomRule {

  override def apply(qe: QueryExecution): Unit = {
    val optimizedPlan = qe.optimizedPlan

    optimizedPlan.collect {
      case  command: InsertIntoHadoopFsRelationCommand =>
        val partitionColumn = command.partitionColumns

        if (partitionColumn.length > 3) {
          AntiPatternReporter.logAlert(s"------------!!! Over-Partitioning Alert !!! -----------------")
            AntiPatternReporter.logAlert(s"Alert: Query is writing with ${partitionColumn.length} partition levels.")
          AntiPatternReporter.logAlert("Recommendation: Reduce partition depth to avoid metadata overhead.")
        }

        partitionColumn.foreach {
          col =>
            val dataTypes = col.dataType.typeName
            if (dataTypes == "double"|| dataTypes=="float"){
              AntiPatternReporter.logAlert(s"!!! Critical Anti-Pattern: Partitioning by $dataTypes column [${col.name}] !!!")
            }
        }
    }
  }
}
