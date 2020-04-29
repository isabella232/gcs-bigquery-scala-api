package com.emarsys.google.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.google.bigquery.builder.{Query, StandardTableSource, TableQuery}
import com.emarsys.google.bigquery.model.BigQueryJobModel.{BigQueryJobError, BigQueryJobResult, Reasons}
import com.emarsys.google.bigquery.model.BqTableReference
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BigQueryAsyncExecutorInstanceSpec extends TestKit(ActorSystem("gcs-segment")) with AnyWordSpecLike with Matchers {
  val waitTimeout: FiniteDuration = 20.seconds

  import system.dispatcher

  implicit val bigQueryAsyncExecutor: BigQueryAsyncExecutor = new BigQueryAsyncExecutorInstance()

  private val tableToCopy  = BqTableReference(GoogleCloudConfig.google.projectName, GoogleCloudConfig.google.bigQuery.dataset, "it_test_copy")
  private val invalidTable = BqTableReference(GoogleCloudConfig.google.projectName, GoogleCloudConfig.google.bigQuery.dataset, "invalid")
  private val copyTargetTable =
    BqTableReference(GoogleCloudConfig.google.projectName, GoogleCloudConfig.google.bigQuery.resultsDataset, "it_test_copy_target")

  case class StringQuery(q: String) extends Query {
    override def show: String = q
    override val isLegacy     = false
  }

  "#runAsyncJob" when {
    "called with copy command" should {
      "copy query result to destination table" in {
        val tableToCopyQuery = TableQuery(StandardTableSource(tableToCopy))

        val Success(result) = Await.ready(
   
          bigQueryAsyncExecutor.runAsyncJob(
            bigQueryAsyncExecutor.copy(tableToCopyQuery, copyTargetTable, WriteDisposition.WRITE_TRUNCATE),
            copyTargetTable
          ),
          waitTimeout
        ).value.get

        result shouldBe a[BigQueryJobResult]
      }

      "fail if invalid query passed" in {
        val invalidQuery = StringQuery(s"select * from ${tableToCopy.standardName} where invalid = true")

        val Failure(error) = Await.ready(
          bigQueryAsyncExecutor.runAsyncJob(
            bigQueryAsyncExecutor.copy(invalidQuery, copyTargetTable, WriteDisposition.WRITE_TRUNCATE),
            copyTargetTable
          ),
          waitTimeout
        ).value.get

        error shouldBe a[BigQueryJobError]
      }

      "fail with retriable error if invalid table passed" in {
        val invalidTableToCopyQuery = TableQuery(StandardTableSource(invalidTable))

        val Failure(error) = Await.ready(
          bigQueryAsyncExecutor.runAsyncJob(
            bigQueryAsyncExecutor.copy(invalidTableToCopyQuery, copyTargetTable, WriteDisposition.WRITE_TRUNCATE),
            copyTargetTable
          ),
          waitTimeout
        ).value.get

        error should matchPattern {
          case BigQueryJobError(_, Reasons.NotFound, _, _, _) =>
        }
      }
    }
  }
}
