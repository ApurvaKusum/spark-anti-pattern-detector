package org.example.analyzer.rules

import org.apache.spark.sql.execution.QueryExecution

/**
 * Interface for rules to be applied on executed query plans for detecting anti-pattern.
 * Each rule should implement apply method for defining their checks.
 */
trait CustomRule {
  def apply(qe: QueryExecution): Unit
}
