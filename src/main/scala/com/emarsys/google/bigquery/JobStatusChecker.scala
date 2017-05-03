package com.emarsys.google.bigquery

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.google.api.services.bigquery.model.Job

import scala.util.{Failure, Success}

object JobStatusChecker {

  val STATUS_RUNNING = "RUNNING"
  val STATUS_DONE    = "DONE"
  val STATUS_ERROR   = "ERROR"

  case class GetJobResult(jobId: String, projectId: String)
  case class PollJobResult(sender: ActorRef, jobRequest: GetJobResult)
  case class JobResult(job: Job)

  def props(bigQuery: BigQueryExecutor)(implicit actorSystem: ActorSystem) = Props(new JobStatusChecker(bigQuery, actorSystem))

}

class JobStatusChecker(bigQueryExecutor: BigQueryExecutor, actorSystem: ActorSystem)
    extends Actor
    with ActorLogging
    with CommandFactory
    with BigQueryConfig {

  lazy val bigQuery = BigQueryApi(projectId, credentialWrite)

  import JobStatusChecker._
  import context.dispatcher

  def receive = {
    case jobRequest: GetJobResult =>
      queryJob(jobRequest).map(pollJob(sender(), jobRequest)).onComplete {
        case Failure(e) => log.warning(e.getMessage)
        case Success(_) => ()
      }

    case PollJobResult(sender, jobRequest) =>
      queryJob(jobRequest).map(pollJob(sender, jobRequest))
  }

  def queryJob(jobRequest: GetJobResult) =
    bigQueryExecutor.execute[Job](status(jobRequest.jobId, jobRequest.projectId))


  def pollJob(sender: ActorRef, jobRequest: GetJobResult) = { job: Job =>
    if (job.getStatus.getState == STATUS_RUNNING) {
      context.system.scheduler.scheduleOnce(jobPollTimeout, self, PollJobResult(sender, jobRequest))
    } else {
      sender ! JobResult(job)
    }
  }

}
