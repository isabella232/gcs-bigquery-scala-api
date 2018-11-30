package com.emarsys.google.bigquery

import akka.event.LoggingAdapter
import com.emarsys.google.bigquery.model.BqQueryJobConfig
import com.google.api.services.bigquery.model.{GetQueryResultsResponse, Job, Table, TableDataInsertAllResponse}
import com.google.api.services.bigquery.{Bigquery, BigqueryRequest}
import org.joda.time.Duration

import scala.concurrent.{ExecutionContext, Future}

sealed abstract class TableCommand[T](val command: BigqueryRequest[T])
case class DeleteTableCommand(override val command: Bigquery#Tables#Delete) extends TableCommand[Void](command)
case class CreateTableCommand(override val command: Bigquery#Tables#Insert) extends TableCommand[Table](command)
case class InsertAllTableCommand(
    override val command: Bigquery#Tabledata#InsertAll
) extends TableCommand[TableDataInsertAllResponse](command)
case class GetTableCommand(override val command: Bigquery#Tables#Get) extends TableCommand[Table](command)
case class CopyCommand(
    override val command: Bigquery#Jobs#Insert,
    jobConfig: BqQueryJobConfig
) extends TableCommand[Job](command)
case class InsertDataCommand(override val command: Bigquery#Jobs#Insert) extends TableCommand[Job](command)
case class ExtractCommand(override val command: Bigquery#Jobs#Insert)    extends TableCommand[Job](command)
case class QueryCommand(override val command: Bigquery#Jobs#Insert)      extends TableCommand[Job](command)
case class ResultCommand(override val command: Bigquery#Jobs#GetQueryResults)
    extends TableCommand[GetQueryResultsResponse](command)
case class JobStatusCommand(override val command: Bigquery#Jobs#Get) extends TableCommand[Job](command)

trait BigQueryExecutor {

  implicit val logger: LoggingAdapter

  def logTime[T](f: => (T, String)): T = {
    val start             = org.joda.time.DateTime.now()
    val (result, message) = f
    val end               = org.joda.time.DateTime.now()
    val time              = Duration.millis(end.minus(start.getMillis).getMillis)
    val log               = (s: String) => if (logger != null) logger.debug(s) else println(s)

    log(
      message + "BigQuery Execution timer=" + (time.getMillis.toDouble / 1000.0)
    )
    result
  }

  def execute[T](command: TableCommand[T])(implicit ec: ExecutionContext): Future[T] =
    Future {
      logTime {
        command match {
          case DeleteTableCommand(c) =>
            (c.execute, s"delete:  ${c.getTableId}: ")
          case CreateTableCommand(c) =>
            (c.execute, s"create table ${c.getDatasetId}: ")
          case InsertAllTableCommand(c) =>
            (c.execute, s"insert all: ${c.getTableId}: ")
          case CopyCommand(c, _) =>
            (c.execute, s"copy ${c.getProjectId}: ")
          case ExtractCommand(c) =>
            (c.execute, s"extract ${c.getProjectId}: ")
          case QueryCommand(c) =>
            (c.execute, s"query ${c.getProjectId}: ")
          case ResultCommand(c) =>
            (c.execute, s"result ${c.getJobId}: ")
          case JobStatusCommand(c) =>
            (c.execute, s"status ${c.getJobId}: ")
          case InsertDataCommand(c) =>
            (c.execute, s"insert data ${c.getProjectId}: ")
          case GetTableCommand(c) =>
            (c.execute, s"get table ${c.getTableId}: ")
        }
      }

    }
}
