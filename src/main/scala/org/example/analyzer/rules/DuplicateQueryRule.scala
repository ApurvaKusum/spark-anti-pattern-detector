package org.example.analyzer.rules

import org.apache.spark.sql.execution.QueryExecution
import org.example.analyzer.reporting.AntiPatternReporter

import scala.collection.mutable

/**
 * This rule detects duplicate queries in your spark application.
 * For determining duplicates we are calculating semanticHash of each query and validating against
 * all other executed queries inside current spark application.
 */
object DuplicateQueryRule extends CustomRule {
  // HashMap of semantic hash and corresponding logical plan.
  private val semanticHashAndPlanMapping = new mutable.HashMap[Int, String]

  override def apply(qe: QueryExecution): Unit = {
    val hashVal = qe.optimizedPlan.canonicalized.semanticHash()
    if (semanticHashAndPlanMapping.contains(hashVal)) {
      AntiPatternReporter.logAlert("Duplicate Query Alert")
    } else {
      semanticHashAndPlanMapping += (hashVal -> qe.optimizedPlan.canonicalized.prettyJson)
    }
  }
}
