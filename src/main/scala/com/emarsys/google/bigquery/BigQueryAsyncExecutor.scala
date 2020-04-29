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

import scala.concurrent.{ExecutionContextExecutor, Future}

trait BigQueryAsyncExecutor extends BigQueryDataAccess {
  def runAsyncJob[T <: Job](command: TableCommand[T], table: BqTableReference): Future[BigQueryJobResult]

  def handleJobResult(table: BqTableReference, jobResult: JobResult): Future[BigQueryJobResult]
}

class BigQueryAsyncExecutorInstance(implicit system: ActorSystem, override val executor: ExecutionContextExecutor)
    extends BigQueryAsyncExecutor {
  implicit val askTimeout: Timeout                  = Timeout(google.bigQuery.jobTimeout)
  lazy val jobStatusChecker: ActorRef               = system.actorOf(JobStatusChecker.props(this))
  override implicit lazy val logger: LoggingAdapter = Logging(system, classOf[BigQueryAsyncExecutorInstance])

  override def runAsyncJob[T <: Job](command: TableCommand[T], table: BqTableReference): Future[BigQueryJobResult] = {
    logger.info("Executing job:" + command)
    for {
      job    <- execute(command).recoverWith(wrapException(table, None))
      result <- waitForResult(job, table).recoverWith(wrapException(table, Some(job.getJobReference.getJobId)))
    } yield result
  }

  private def wrapException[A](table: BqTableReference, jobId: Option[String]): PartialFunction[Throwable, Future[A]] = {
    case e: BigQueryJobError => Future.failed(e)
    case e =>
      logger.error(e, "Job failed for table {}", table.table)
      Future.failed(UnexpectedBigQueryJobError(e, jobId))
  }

  private def waitForResult[T <: Job](job: T, table: BqTableReference): Future[BigQueryJobResult] =
    for {
      jobResult <- (jobStatusChecker ? GetJobResult(job.getJobReference.getJobId, table.project)).mapTo[JobResult]
      result    <- handleJobResult(table, jobResult)
    } yield result

  override def handleJobResult(table: BqTableReference, jobResult: JobResult): Future[BigQueryJobResult] =
    Option(jobResult.job.getStatus.getErrorResult) match {
      case Some(errorResult) =>
        Future.failed(BigQueryJobError(errorResult, table.table, jobResult.job.getJobReference.getJobId))
      case None =>
        logger.info("Job finished for table {}", table.table)
        getAmountOfTableRows(table).map(amount => BigQueryJobResult(amount, jobResult.job.getJobReference.getJobId))
    }
}
