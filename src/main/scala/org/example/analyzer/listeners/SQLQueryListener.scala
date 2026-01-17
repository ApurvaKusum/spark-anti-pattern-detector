package org.example.analyzer.listeners

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.example.analyzer.rules.{CheckIfOutputTablesPartitioned, CheckOverPartition, CheckSmallFiles, CustomRule, DuplicateQueryRule}

object SQLQueryListener extends QueryExecutionListener {

  /**
   * Set of rules to be applied after every successful query run.
   */
  val rules: Seq[CustomRule] = Seq (
    DuplicateQueryRule,
    CheckSmallFiles,
    CheckOverPartition,
    CheckIfOutputTablesPartitioned
  )

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    rules.foreach(_.apply(qe))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ???
}
