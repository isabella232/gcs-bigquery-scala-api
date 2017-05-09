package com.emarsys.google.bigquery.builder

import com.emarsys.google.bigquery.model.BqTableReference
import org.scalatest.{Matchers, WordSpec}

class QuerySpec extends WordSpec with Matchers {

  import QueryCondition._

  val customerId = 1234

  val tableReference = BqTableReference("project", "dataset", "table")

  "Standard TableQuery" should {

    "show query with no condition" in {
      val expectedQuery = "SELECT * FROM `project.dataset.table`"
      TableQuery(StandardTableSource(tableReference)).show shouldEqual expectedQuery
    }

    "use customer condition" in {
      val expectedQuery = s"SELECT * FROM `project.dataset.table` WHERE customer_id = $customerId"
      TableQuery(StandardTableSource(tableReference), "customer_id" === customerId).show shouldEqual expectedQuery
    }

    "use given fields" in {
      val expectedQuery = s"SELECT count(*) FROM `project.dataset.table` WHERE customer_id = $customerId"
      TableQuery(StandardTableSource(tableReference), "customer_id" === customerId, "count(*)").show shouldEqual expectedQuery
    }
  }

  "Legacy TableQuery" should {

    "show query with no condition" in {
      val expectedQuery = "SELECT * FROM [project:dataset.table]"
      TableQuery(LegacyTableSource(tableReference)).show shouldEqual expectedQuery
    }

    "use customer condition" in {
      val expectedQuery = s"SELECT * FROM [project:dataset.table] WHERE customer_id = $customerId"
      TableQuery(LegacyTableSource(tableReference), "customer_id" === customerId).show shouldEqual expectedQuery
    }

    "use given fields" in {
      val expectedQuery = s"SELECT count(*) FROM [project:dataset.table] WHERE customer_id = $customerId"
      TableQuery(LegacyTableSource(tableReference), "customer_id" === customerId, "count(*)").show shouldEqual expectedQuery
    }

  }
}
