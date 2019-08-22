package com.emarsys.google.bigquery.model

object BigQueryJobModel {

  case class BigQueryJobResult(affectedRows: BigInt)

  case class BigQueryJobError(message: String, reason: String, location: String, table: String) extends Throwable

  case class BigQueryResourceNotFoundError(override val message: String, override val table: String)
      extends BigQueryJobError(message, "", "", table)

}
