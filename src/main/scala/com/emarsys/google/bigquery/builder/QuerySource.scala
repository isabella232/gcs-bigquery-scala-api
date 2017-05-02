package com.emarsys.google.bigquery.builder

import com.emarsys.google.bigquery.model.BqTableReference

trait QuerySource {
  def show: String
  def isLegacy: Boolean
}

trait SingleTableSource extends QuerySource {
  val tableReference: BqTableReference
}

sealed trait Table {
  val alias: String
}
case class BaseTable(singleTableQuerySource: SingleTableSource, alias: String) extends Table
case class JoinedTable(singleTableQuerySource: SingleTableSource, alias: String, joinCondition: String) extends Table

case class StandardTableSource(tableReference: BqTableReference) extends SingleTableSource {
  override def show = tableReference.standardName
  override def isLegacy = false
}

case class LegacyTableSource(tableReference: BqTableReference) extends SingleTableSource {
  override def show = tableReference.legacyName
  override def isLegacy = true
}

case class MultiTableSource(baseTable: BaseTable, joinedTables: JoinedTable*) extends QuerySource {
  override def show = {
    val joinQueryPart = joinedTables.map(joinedTable =>
      s"${joinedTable.singleTableQuerySource.show} ${joinedTable.alias} ON (${joinedTable.joinCondition})"
    ).mkString(" LEFT JOIN ")
    s"${baseTable.singleTableQuerySource.show} ${baseTable.alias} LEFT JOIN $joinQueryPart"
  }
  override def isLegacy = baseTable.singleTableQuerySource.isLegacy
}
