package com.emarsys.google.bigquery

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout
import com.emarsys.google.bigquery.JobStatusChecker.{GetJobResult, JobResult}
import com.emarsys.google.bigquery.model.BigQueryJobModel.{
  BigQueryJobError,
  BigQueryJobResult,
  UnexpectedBigQueryJobError
}
import com.emarsys.google.bigquery.model.BqTableReference
import com.google.api.services.bigquery.model.Job

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

trait BigQueryAsyncExecutor extends BigQueryDataAccess {
  def runAsyncJob[T <: Job](command: TableCommand[T], table: BqTableReference): Future[BigQueryJobResult]

  def handleJobResult(table: BqTableReference, jobResult: JobResult): Future[BigQueryJobResult]
}

class BigQueryAsyncExecutorInstance(implicit system: ActorSystem, override val executor: ExecutionContextExecutor)
    extends BigQueryAsyncExecutor {
  implicit val askTimeout: Timeout                  = Timeout(3600.seconds)
  lazy val jobStatusChecker: ActorRef               = system.actorOf(JobStatusChecker.props(this))
  override implicit lazy val logger: LoggingAdapter = Logging(system, classOf[BigQueryAsyncExecutorInstance])

  override def runAsyncJob[T <: Job](command: TableCommand[T], table: BqTableReference): Future[BigQueryJobResult] = {
    logger.info("Executing job:" + command)
    executeCommand(command, table).recoverWith {
      case e: BigQueryJobError => Future.failed(e)
      case e: Exception =>
        logger.error(e, "Job failed for table {}", table.table)
        Future.failed(UnexpectedBigQueryJobError(e))
    }
  }

  private def executeCommand[T <: Job](command: TableCommand[T], table: BqTableReference): Future[BigQueryJobResult] =
    for {
      job       <- execute(command)
      jobResult <- (jobStatusChecker ? GetJobResult(job.getJobReference.getJobId, table.project)).mapTo[JobResult]
      result    <- handleJobResult(table, jobResult)
    } yield result

  override def handleJobResult(table: BqTableReference, jobResult: JobResult): Future[BigQueryJobResult] =
    Option(jobResult.job.getStatus.getErrorResult) match {
      case Some(errorResult) =>
        Future.failed(BigQueryJobError(errorResult, table.table))
      case None =>
        logger.info("Job finished for table {}", table.table)
        getAmountOfTableRows(table).map(amount => BigQueryJobResult(amount))
    }
}
