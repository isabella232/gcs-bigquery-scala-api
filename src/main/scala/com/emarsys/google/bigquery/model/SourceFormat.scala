package com.emarsys.google.bigquery.model

sealed trait SourceFormat {
  def show: String
}
case object CsvFormat extends SourceFormat {
  override def show = "CSV"
}

case object JsonFormat extends SourceFormat {
  override def show = "NEWLINE_DELIMITED_JSON"
}

case object AvroFormat extends SourceFormat {
  override def show = "AVRO"
}