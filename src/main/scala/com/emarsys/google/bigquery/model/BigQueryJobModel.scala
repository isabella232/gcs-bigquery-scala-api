package com.emarsys.google.bigquery.model

object BigQueryJobModel {

  case class BigQueryJobResult(affectedRows: BigInt)

  case class BigQueryJobError(message: String, reason: String, location: String, table: String) extends Throwable

}
