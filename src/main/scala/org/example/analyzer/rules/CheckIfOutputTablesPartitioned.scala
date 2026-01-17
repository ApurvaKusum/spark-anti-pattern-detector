package org.example.analyzer.rules

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.example.analyzer.reporting.AntiPatternReporter

/**
 * This rule checks if our output table is partitioned or not.
 * Ideally we should consider partitioning our tables before writing them. We raise alert whenever
 * a table without partitioning is written.
 */
object CheckIfOutputTablesPartitioned extends CustomRule {

  override def apply(qe: QueryExecution): Unit = {
    val optimizedPlan = qe.optimizedPlan

    optimizedPlan.collect {
      case p: InsertIntoHadoopFsRelationCommand =>
        if (p.partitionColumns.isEmpty) {
          val tablePath = p.options.get("path")
          AntiPatternReporter.logAlert("Table written without partitioning Alert")
        }
    }
  }
}
