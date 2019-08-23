package com.emarsys.google.bigquery.model

object BigQueryJobModel {

  case class BigQueryJobResult(affectedRows: BigInt)

  sealed trait BigQueryJobError

  case class GeneralBigQueryJobError(message: String, reason: String, location: String, table: String)
      extends BigQueryJobError

  case class BigQueryResourceNotFoundError(message: String, table: String) extends BigQueryJobError

}
