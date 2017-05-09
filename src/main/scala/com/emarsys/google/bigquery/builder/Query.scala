package com.emarsys.google.bigquery.builder

trait Query {
  def show: String
}

case class TableQuery(
                       querySource: QuerySource,
                       condition: QueryCondition = QueryCondition.empty,
                       fields: String = "*",
                       queryHaving: QueryHaving = QueryHaving.empty
                     ) extends Query {

  private val where =
      if (condition.show.nonEmpty)
        s" WHERE ${condition.show}"
      else ""

  private val having =
      if (queryHaving.condition.show.nonEmpty)
        s" HAVING ${queryHaving.condition.show}"
      else ""

  private val groupBy =
      if (queryHaving.groupByFields.isEmpty) ""
      else " GROUP BY " + queryHaving.groupByFields.mkString(",")

  private val allFields = fields + (
      if (queryHaving.fieldExpressions.isEmpty) ""
      else ", " + queryHaving.fieldExpressions.mkString(",")
    )

  override def show =
      s"SELECT $allFields FROM ${querySource.show}$where$groupBy$having"
}