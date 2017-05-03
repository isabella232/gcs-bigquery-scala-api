package com.emarsys.google.bigquery.model

sealed trait FileFormat {
  def show: String
}
case object CsvFormat extends FileFormat {
  override def show = "CSV"
}

case object JsonFormat extends FileFormat {
  override def show = "NEWLINE_DELIMITED_JSON"
}

case object AvroFormat extends FileFormat {
  override def show = "AVRO"
}