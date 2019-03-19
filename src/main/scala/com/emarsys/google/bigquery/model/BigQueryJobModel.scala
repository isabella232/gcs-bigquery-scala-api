package com.emarsys.google.bigquery.model

object BigQueryJobModel {

  case class BigQueryJobResult(affectedRows: Option[Long])

  case class BigQueryJobError(message: String, reason: String, location: String, table: String)

}