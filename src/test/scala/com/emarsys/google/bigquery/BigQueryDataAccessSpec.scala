package com.emarsys.google.bigquery

import com.emarsys.google.bigquery.builder.{StandardTableSource, TableQuery}
import com.emarsys.google.bigquery.exception.UnsuccessfulQueryException
import com.emarsys.google.bigquery.syntax._
import com.emarsys.google.bigquery.format._
import com.emarsys.google.bigquery.model.BqTableReference
import org.joda.time.DateTime
import org.joda.time.DateTime

case class ClickEvent(campaignId: Int, eventTime: DateTime, deviceName: String, customerId: Int, messageId: Int)

class BigQueryDataAccessSpec extends BaseQueryTest with BigQueryConfig{

  override lazy val bigQuery = BigQueryApi(projectId, credentialWrite)

  val bqTableReference = BqTableReference("[project]", "[dataSet]", "[table]")

  "BigQueryDataAccess" should {

    "run executeQuery" in {
      val query = TableQuery(StandardTableSource(bqTableReference), fields = "campaign_id, event_time, platform, customer_id, message_id")
      val result = executeQuery[ClickEvent](query).futureValue
      result.size shouldEqual 5
      result.forall(_.isInstanceOf[ClickEvent]) shouldBe true
      result.forall(_.eventTime.isInstanceOf[DateTime]) shouldBe true
    }

    "throw exception on invalid query" in {
      val query = TableQuery(StandardTableSource(bqTableReference), fields = "non_existing_field")
      val exception = executeQuery[ClickEvent](query).failed.futureValue
      exception.isInstanceOf[UnsuccessfulQueryException]
      exception.getMessage contains "Query could not be executed"
    }

  }

}
