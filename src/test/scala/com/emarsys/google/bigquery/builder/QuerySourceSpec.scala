package com.emarsys.google.bigquery.builder

import com.emarsys.google.bigquery.model.BqTableReference
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class QuerySourceSpec extends AnyWordSpec with Matchers {

  "QuerySource" should {

    "be build from single table reference" in {
      val tableReference = BqTableReference("project", "dataSet", "table")
      StandardTableSource(tableReference).show shouldEqual tableReference.standardName
    }

    "be build from single table reference in legacy format" in {
      val tableReference = BqTableReference("project", "dataSet", "table")
      LegacyTableSource(tableReference).show shouldEqual tableReference.legacyName
    }

    "be build from 2 tables" in {
      val tableReference1 = BqTableReference("project", "dataSet", "table1")
      val tableReference2 = BqTableReference("project", "dataSet", "table2")
      val baseTable       = BaseTable(StandardTableSource(tableReference1), "t1")
      val joinedTable = JoinedTable(
        StandardTableSource(tableReference2),
        "t2",
        "t1.field = t2.field"
      )
      val expected =
        s"${tableReference1.standardName} t1 LEFT JOIN ${tableReference2.standardName} t2 ON (t1.field = t2.field)"
      MultiTableSource(baseTable, joinedTable).show shouldEqual expected
    }

    "be build from 3 tables" in {
      val tableReference1 = BqTableReference("project", "dataSet", "table1")
      val tableReference2 = BqTableReference("project", "dataSet", "table2")
      val tableReference3 = BqTableReference("project", "dataSet", "table3")
      val baseTable       = BaseTable(StandardTableSource(tableReference1), "t1")
      val joinedTable1 = JoinedTable(
        StandardTableSource(tableReference2),
        "t2",
        "t1.field = t2.field"
      )
      val joinedTable2 = JoinedTable(
        StandardTableSource(tableReference3),
        "t3",
        "t2.field = t3.field"
      )
      val expected =
        s"${tableReference1.standardName} t1 LEFT JOIN ${tableReference2.standardName} t2 ON (t1.field = t2.field) LEFT JOIN ${tableReference3.standardName} t3 ON (t2.field = t3.field)"
      MultiTableSource(baseTable, joinedTable1, joinedTable2).show shouldEqual expected
    }

    "be build from 3 tables in legacy format" in {
      val tableReference1 = BqTableReference("project", "dataSet", "table1")
      val tableReference2 = BqTableReference("project", "dataSet", "table2")
      val tableReference3 = BqTableReference("project", "dataSet", "table3")
      val baseTable       = BaseTable(LegacyTableSource(tableReference1), "t1")
      val joinedTable1 = JoinedTable(
        LegacyTableSource(tableReference2),
        "t2",
        "t1.field = t2.field"
      )
      val joinedTable2 = JoinedTable(
        LegacyTableSource(tableReference3),
        "t3",
        "t2.field = t3.field"
      )
      val expected =
        s"${tableReference1.legacyName} t1 LEFT JOIN ${tableReference2.legacyName} t2 ON (t1.field = t2.field) LEFT JOIN ${tableReference3.legacyName} t3 ON (t2.field = t3.field)"
      MultiTableSource(baseTable, joinedTable1, joinedTable2).show shouldEqual expected
    }

  }

}
