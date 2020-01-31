package com.emarsys.google.bigquery

import java.math.BigInteger

import akka.event.LoggingAdapter
import com.google.api.services.bigquery.model.{GetQueryResultsResponse, Job, JobReference, Table}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

trait BaseQueryTest extends AnyWordSpec with Matchers with BigQueryDataAccess with ScalaFutures {

  implicit val logger: LoggingAdapter = null
  implicit val executor: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executor)
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(200, Millis))

  def mockedResult(jobId: String) = {
    if (jobId != "invalid") {
      val response = new GetQueryResultsResponse()
      response.setTotalRows(BigInteger.ZERO)
      response.setJobComplete(true)
    } else {
      val response = null
      response
    }
  }

  override def execute[T](command: TableCommand[T])(implicit ec: ExecutionContext): Future[T] = {
    command match {
      case ResultCommand(command) =>
        Future.successful(mockedResult(command.getJobId).asInstanceOf[T])
      case QueryCommand(command) =>
        Future.successful(
          createJob(command.getJsonContent.toString).asInstanceOf[T]
        )
      case GetTableCommand(command) =>
        val numOfRows: Integer = 8
        Future.successful(new Table().setNumRows(BigInteger.valueOf(numOfRows.longValue())).asInstanceOf[T])
      case _ => Future.failed(new Exception("Test command is invalid"))
    }
  }

  def createJob(content: String) = {
    val job          = new Job()
    val jobReference = new JobReference()
    if (content.contains("non_existing_field")) {
      jobReference.setJobId("invalid")
    } else {
      jobReference.setJobId("jobId1")
    }
    job.setJobReference(jobReference)
    job
  }

}
