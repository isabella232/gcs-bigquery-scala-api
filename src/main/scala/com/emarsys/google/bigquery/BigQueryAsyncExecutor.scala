package com.emarsys.google.bigquery

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout
import com.emarsys.google.bigquery.JobStatusChecker.{GetJobResult, JobResult}
import com.emarsys.google.bigquery.model.BigQueryJobModel.{BigQueryJobError, BigQueryJobResult}
import com.emarsys.google.bigquery.model.BqTableReference
import com.google.api.services.bigquery.model.Job

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

trait BigQueryAsyncExecutor extends BigQueryExecutor {

  implicit val system: ActorSystem
  implicit val executor: ExecutionContextExecutor
  override implicit lazy val logger: LoggingAdapter =
    Logging(system, classOf[BigQueryAsyncExecutor])
  implicit val askTimeout   = Timeout(3600.seconds)
  lazy val jobStatusChecker = system.actorOf(JobStatusChecker.props(this))

  def runAsyncJob[T <: Job](command: TableCommand[T], table: BqTableReference): Future[Either[BigQueryJobError, BigQueryJobResult]] = {
    logger.info("Executing job:" + command)
    (for {
      job <- execute(command)
      jobResult <- (jobStatusChecker ? GetJobResult(
        job.getJobReference.getJobId,
        table.project
      )).mapTo[JobResult]
    } yield handleJobResult(table, jobResult)).recover {
      case e: Exception =>
        logger.error(e, "Job failed for table {}", table.table)
        Left(BigQueryJobError(e.getMessage, "", "", table.table))
    }
  }

  def handleJobResult(table: BqTableReference, jobResult: JobResult): Either[BigQueryJobError, BigQueryJobResult] = {
    val errorResult = Option(jobResult.job.getStatus.getErrorResult)
    if (errorResult.isEmpty) {
      logger.info("Job finished for table {}", table.table)
      Right(BigQueryJobResult(Option(jobResult.job.getStatistics.getQuery.getNumDmlAffectedRows)))
    } else {
      logger.error(
        "Job failed for table {} error: {} - {} - {}",
        table.table,
        errorResult.get.getMessage,
        errorResult.get.getReason,
        errorResult.get.getLocation
      )
      Left(BigQueryJobError(
        errorResult.get.getMessage,
        errorResult.get.getReason,
        errorResult.get.getLocation,
        table.table
      ))
    }
  }
}
