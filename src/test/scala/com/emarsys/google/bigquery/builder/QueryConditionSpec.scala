package com.emarsys.google.bigquery.builder

import com.emarsys.google.bigquery.builder.QueryCondition._
import cats.syntax.semigroup._
import com.emarsys.google.bigquery.model.BqTableReference
import org.scalatest.{Matchers, WordSpec}


class QueryConditionSpec extends WordSpec with Matchers {

  val customerId = 1234
  val otherCustomerId = 4567

  "Query Condition" when {

    "no table is specified" should {

      "CustomerCondition" in {
        val expectedCondition = s"customer_id = $customerId"
        CustomerCondition(customerId).show shouldEqual expectedCondition
      }

    }

    "table is specified" should {

      val table = Some(BaseTable(StandardTableSource(BqTableReference("", "", "")), "alias"))

      "CustomerCondition" in {
        val expectedCondition = s"alias.customer_id = $customerId"
        CustomerCondition(customerId, table).show shouldEqual expectedCondition
      }

    }

    "multiple conditions" should {

      "Multiple conditions" in {
        val expectedCondition = s"(customer_id = $customerId AND customer_id = $otherCustomerId)"
        (QueryCondition.customer(customerId) |+| QueryCondition.customer(otherCustomerId)).show shouldEqual expectedCondition
      }

      "disjunction of conditions" in {
        val expectedCondition = s"(customer_id = $customerId OR customer_id = $otherCustomerId)"
        disjunction(Seq(QueryCondition.customer(customerId), QueryCondition.customer(otherCustomerId))).show shouldEqual expectedCondition
      }

      "conjunction of a disjunction and a conjunction" in {
        val expectedCondition = s"((customer_id = $customerId OR customer_id = $otherCustomerId) AND (customer_id = $customerId AND customer_id = $otherCustomerId))"
        (disjunction(Seq(QueryCondition.customer(customerId), QueryCondition.customer(otherCustomerId))) |+|
          (QueryCondition.customer(customerId) |+| QueryCondition.customer(otherCustomerId))).show shouldEqual expectedCondition
      }

    }

  }

}
