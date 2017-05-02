package com.emarsys.google.bigquery.builder

trait Query {
  def show: String
}

case class TableQuery(querySource: QuerySource, condition: QueryCondition = QueryCondition.empty, fields: String = "*") extends Query {

  private val where =
      if (condition.show.nonEmpty)
        s" WHERE ${condition.show}"
      else ""

  override def show =
      s"SELECT $fields FROM ${querySource.show}$where"
}