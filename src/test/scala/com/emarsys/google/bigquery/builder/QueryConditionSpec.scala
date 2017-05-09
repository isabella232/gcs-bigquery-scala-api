package com.emarsys.google.bigquery.builder

import org.scalatest.{Matchers, WordSpec}


class QueryConditionSpec extends WordSpec with Matchers {

  import QueryCondition._

  val customerId = 1234
  val otherString = "test"

  "Query Condition" when {

    "single condition" should {

      "CustomerCondition" in {
        val expectedCondition = s"customer_id = $customerId"
        ("customer_id" === customerId).show shouldEqual expectedCondition
      }

    }

    "multiple conditions" should {

      "Multiple conditions" in {
        val expectedCondition = s"(customer_id = $customerId AND other_field = $otherString)"
        ("customer_id" === customerId && "other_field" === otherString).show shouldEqual expectedCondition
      }

      "disjunction of conditions" in {
        val expectedCondition = s"(customer_id = $customerId OR other_field = $otherString)"
        ("customer_id" === customerId || "other_field" === otherString).show shouldEqual expectedCondition
      }

      "conjunction of a disjunction and a conjunction" in {
        val expectedCondition = s"((customer_id = $customerId OR other_field = $otherString) AND (customer_id = $customerId AND other_field = $otherString))"

        (("customer_id" === customerId || "other_field" === otherString)
          && ("customer_id" === customerId && "other_field" === otherString)).show shouldEqual expectedCondition
      }

    }

  }

}
