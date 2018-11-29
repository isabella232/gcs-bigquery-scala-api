package com.emarsys.google.bigquery.builder

case class QueryHaving(
    fieldExpressions: List[String] = List(),
    groupByFields: List[String] = List(),
    condition: QueryCondition = QueryCondition.empty
)

object QueryHaving {
  def empty = QueryHaving()
}
