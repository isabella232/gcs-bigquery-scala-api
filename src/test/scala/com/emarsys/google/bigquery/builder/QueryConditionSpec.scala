package com.emarsys.google.bigquery.builder

import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._


class QueryConditionSpec extends WordSpec with Matchers {

  import QueryCondition._

  val customerId = 1234
  val otherString = "test"

  "Query Condition" when {

    "single condition" should {

      "Equals condition" in {
        val expectedCondition = s"customer_id = $customerId"
        ("customer_id" === customerId).show shouldEqual expectedCondition
      }

      "Less or equals condition" in {
        val expectedCondition = s"count <= 2"
        ("count" <<= 2).show shouldEqual expectedCondition
      }

      "Greater or equals condition" in {
        val expectedCondition = s"count >= 2"
        ("count" >>= 2).show shouldEqual expectedCondition
      }

      "IsInTheLastDuration condition with minutes" in {
        val expected = """date_field > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -300, "SECOND")"""
        ("date_field" isInTheLast 5.minutes).show shouldEqual expected
      }

      "IsInTheLastDuration condition with hours" in {
        val expected = """date_field > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3600, "SECOND")"""
        ("date_field" isInTheLast 1.hours).show shouldEqual expected
      }

      "IsInTheLastDuration condition with seconds" in {
        val expected = """date_field > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "SECOND")"""
        ("date_field" isInTheLast 3.seconds).show shouldEqual expected
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
