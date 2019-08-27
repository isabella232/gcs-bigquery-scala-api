package com.emarsys.google.bigquery

import java.io.IOException
import java.math.BigInteger

import com.emarsys.google.bigquery.builder.{StandardTableSource, TableQuery}
import com.emarsys.google.bigquery.exception.UnsuccessfulQueryException
import com.emarsys.google.bigquery.syntax._
import com.emarsys.google.bigquery.format._
import com.emarsys.google.bigquery.model.BqTableReference
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

case class ClickEvent(
    campaignId: Int,
    eventTime: DateTime,
    deviceName: String,
    customerId: Int,
    messageId: Int
)

class BigQueryDataAccessSpec extends BaseQueryTest {

  override implicit val executor: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executor)

  override lazy val bigQuery = BigQueryApi(google.projectName, credentialWrite)

  val bqTableReference = BqTableReference("[project]", "[dataSet]", "[table]")

  "BigQueryDataAccess" should {

    "run executeQuery" in {
      val query = TableQuery(
        StandardTableSource(bqTableReference),
        fields = "campaign_id, event_time, platform, customer_id, message_id"
      )
      val result = executeQuery[ClickEvent](query).futureValue
      result.size shouldEqual 0
    }

    "throw exception on invalid query" in {
      val query = TableQuery(
        StandardTableSource(bqTableReference),
        fields = "non_existing_field"
      )
      val exception = executeQuery[ClickEvent](query).failed.futureValue
      exception.isInstanceOf[UnsuccessfulQueryException]
      exception.getMessage contains "Query could not be executed"
    }

  }

  "#getAmountOfTableRows" when {

    "table contains 8 rows" should {
      "return 8" in {
        val expectedNumOfRows = 8
        val numOfRows         = getAmountOfTableRows(bqTableReference).futureValue
        numOfRows shouldBe BigInteger.valueOf(expectedNumOfRows.longValue())
      }
    }
  }
}
