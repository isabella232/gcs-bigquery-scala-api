package com.emarsys.google.bigquery

import akka.event.LoggingAdapter
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait BaseQueryTest extends WordSpec with Matchers with BigQueryDataAccess with ScalaFutures {

  implicit val logger: LoggingAdapter = null
  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
  implicit val defaultPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(200, Millis))

}
