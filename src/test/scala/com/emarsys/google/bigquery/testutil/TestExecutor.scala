package com.emarsys.google.bigquery.testutil

import com.emarsys.google.bigquery._
import com.emarsys.google.bigquery.model.BqQueryJobConfig
import com.google.api.services.bigquery.model.{ErrorProto, Job, JobReference, JobStatus}

import scala.concurrent.{ExecutionContext, Future}

import java.time.LocalDate

trait TestExecutor extends BigQueryExecutor {

  var commandsReceived = Seq.empty[TableCommand[_]]

  val oneTableErrorDate = LocalDate.parse("2016-10-10")

  def resetCommands() = commandsReceived = Seq()

  override def execute[T](
      command: TableCommand[T]
  )(implicit ec: ExecutionContext): Future[T] = command match {
    case CopyCommand(_, jobConfig) if isOpenQueryOnErrorDate(jobConfig) =>
      Future.successful(
        createJob(JobStatusChecker.STATUS_ERROR).asInstanceOf[T]
      )
    case JobStatusCommand(cmd) if cmd.getJobId == "project:errorId" =>
      Future.successful(
        createJob(JobStatusChecker.STATUS_ERROR).asInstanceOf[T]
      )
    case _ =>
      if (!command.isInstanceOf[JobStatusCommand]) {
        commandsReceived +:= command
      }
      Future.successful(createJob(JobStatusChecker.STATUS_DONE).asInstanceOf[T])
  }

  def isOpenQueryOnErrorDate(jobConfig: BqQueryJobConfig) = {
    jobConfig.query.show
      .contains("opens_" + oneTableErrorDate.formatted("yyyyMMdd"))
  }

  def createJob(status: String) = {
    val job       = new Job()
    val jobStatus = new JobStatus()
    jobStatus.setState(status)
    job.setId("project:jobId")
    if (status == JobStatusChecker.STATUS_ERROR) {
      job.setId("project:errorId")
      val errorProto = new ErrorProto().setDebugInfo("test error")
      jobStatus.setErrorResult(errorProto)
    }
    job.setStatus(jobStatus)
    job.setJobReference(new JobReference().setJobId(job.getId))
    job
  }

}

trait TestAsyncExecutor extends BigQueryAsyncExecutor with TestExecutor
