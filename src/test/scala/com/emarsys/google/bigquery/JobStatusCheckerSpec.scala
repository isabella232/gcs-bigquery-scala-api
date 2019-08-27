package com.emarsys.google.bigquery

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.emarsys.google.bigquery.JobStatusChecker.{GetJobResult, JobResult}
import com.google.api.services.bigquery.model.{Job, JobStatus}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.{ExecutionContext, Future}

class JobStatusCheckerSpec
    extends TestKit(ActorSystem("testSystem"))
    with WordSpecLike
    with Matchers
    with GoogleCloudConfig
    with ImplicitSender {

  class TestPollingBigQuery extends BigQueryExecutor {

    override implicit val logger = null
    var callIndex                = 0

    override def execute[T](
        command: TableCommand[T]
    )(implicit ec: ExecutionContext): Future[T] = {
      val statusCommand = command.asInstanceOf[JobStatusCommand].command
      if (statusCommand.getJobId == "jobId" && statusCommand.getProjectId == "projectId") {
        callIndex = callIndex + 1
        val status =
          if (callIndex % 3 == 0) JobStatusChecker.STATUS_DONE
          else JobStatusChecker.STATUS_RUNNING
        Future.successful(createJob(status).asInstanceOf[T])
      } else {
        Future.failed(
          new Exception("Test command is invalid" + statusCommand.getJobId)
        )
      }
    }
  }
  class TestBigQuery extends BigQueryExecutor {

    override implicit val logger = null

    override def execute[T](
        command: TableCommand[T]
    )(implicit ec: ExecutionContext): Future[T] = {
      Future.successful(createJob(JobStatusChecker.STATUS_DONE).asInstanceOf[T])
    }
  }

  trait FakePollingBigQueryScope {
    val bigQuery         = new TestPollingBigQuery
    val jobStatusChecker = system.actorOf(JobStatusChecker.props(bigQuery))
  }

  trait FakeBigQueryScope {
    val bigQuery         = new TestBigQuery
    val jobStatusChecker = system.actorOf(JobStatusChecker.props(bigQuery))
  }

  "JobStatusChecker" should {

    "return Job if status is not running" in new FakeBigQueryScope {
      jobStatusChecker ! GetJobResult("jobId", "projectId")

      expectMsgPF()(extractJob).getStatus.getState shouldEqual JobStatusChecker.STATUS_DONE
    }

    "poll job status if status is running" in new FakePollingBigQueryScope {
      jobStatusChecker ! GetJobResult("jobId", "projectId")

      expectMsgPF()(extractJob).getStatus.getState shouldEqual JobStatusChecker.STATUS_DONE
    }

  }

  def extractJob: PartialFunction[Any, Job] = {
    case JobResult(job) => job
  }

  def createJob(status: String) = {
    val job       = new Job()
    val jobStatus = new JobStatus()
    jobStatus.setState(status)
    job.setStatus(jobStatus)
    job
  }

}
