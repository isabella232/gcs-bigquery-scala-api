package com.emarsys.google.bigquery

import java.math.BigInteger

import com.emarsys.google.bigquery.api.BigQueryFormat
import com.emarsys.google.bigquery.builder.Query
import com.emarsys.google.bigquery.syntax._
import com.emarsys.google.bigquery.exception.UnsuccessfulQueryException
import com.emarsys.google.bigquery.model.BqTableReference
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}

trait BigQueryDataAccess extends CommandFactory with BigQueryExecutor with BigQueryConfig with TableCommandFactory {

  implicit val executor: ExecutionContextExecutor

  lazy val bigQuery = BigQueryApi(projectId, credentialWrite)

  def executeQuery[T](
      query: Query
  )(implicit format: BigQueryFormat[T]): Future[List[T]] = {

    queryResult(query).map { result =>
      if (result == null) {
        throw new UnsuccessfulQueryException(
          "Query could not be executed: " + query.show
        )
      } else if (result.getTotalRows.equals(BigInteger.ZERO)) {
        List.empty[T]
      } else {
        val rows: List[TableRow] = result.getRows.asScala.toList
        rows.map(_.as[T])
      }
    }
  }

  def tableExists(tableRef: BqTableReference): Future[Boolean] = {
    val result = execute[Table](getTable(tableRef))
    result.map(_ => true).recover {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 => false
      case e: Throwable                                             => throw e
    }
  }

  private def queryResult(q: Query): Future[GetQueryResultsResponse] = {
    for {
      queryResponse <- execute[Job](query(q, projectId))
      queryResult <- execute[GetQueryResultsResponse](
        result(queryResponse.getJobReference.getJobId, projectId)
      )
    } yield {
      queryResult
    }
  }
}
