package com.emarsys.google.bigquery.model

import com.emarsys.google.bigquery.builder.{LegacyTableSource, StandardTableSource, TableQuery}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BigQueryModelSpec extends AnyWordSpec with Matchers {

  val bqTableReference = BqTableReference("project", "dataSet", "table")

  "BqTableReference" should {

    "build TableReference object" in {
      val tableReference = bqTableReference.toJava
      tableReference.getProjectId shouldEqual "project"
      tableReference.getDatasetId shouldEqual "dataSet"
      tableReference.getTableId shouldEqual "table"
    }

  }

  "BqTableFieldSchema" should {

    "build TableFieldSchema object" in {
      val tableFieldSchema = BqTableFieldSchema("fieldName", "fieldType").toJava
      tableFieldSchema.getName shouldEqual "fieldName"
      tableFieldSchema.getType shouldEqual "fieldType"
    }

  }

  "BqTableSchema" should {

    "build TableSchema object" in {
      val field            = BqTableFieldSchema("fieldName", "fieldType")
      val tableSchema      = BqTableSchema(List(field)).toJava
      val tableFieldSchema = tableSchema.getFields.get(0)
      tableFieldSchema.getName shouldEqual "fieldName"
      tableFieldSchema.getType shouldEqual "fieldType"
    }

  }

  "BqTableRow" should {

    "build TableRow object" in {
      val tableRow = BqTableRow(("id", 1), ("field", "value")).toJava
      tableRow.get("id") shouldEqual 1
      tableRow.get("field") shouldEqual "value"
    }

  }

  "BqTableData" should {

    "build TableDataInsertAllRequest object" in {
      val tableData = BqTableData(BqTableRow(("field", "value"))).toJava
      val tableRow  = tableData.getRows.get(0).getJson
      tableRow.get("field") shouldEqual "value"
    }

  }

  "BqQueryJobConfig" should {

    "build JobConfigurationQuery object with query" in {
      val jobConfig = BqQueryJobConfig(
        TableQuery(StandardTableSource(bqTableReference)),
        None,
        None
      ).toJava
      jobConfig.getQuery shouldEqual "SELECT * FROM `project.dataSet.table`"
    }

    "build JobConfigurationQuery object with destination table" in {
      val jobConfig = BqQueryJobConfig(
        TableQuery(StandardTableSource(bqTableReference)),
        Some(bqTableReference),
        None
      ).toJava
      jobConfig.getDestinationTable.getProjectId shouldEqual "project"
      jobConfig.getDestinationTable.getDatasetId shouldEqual "dataSet"
      jobConfig.getDestinationTable.getTableId shouldEqual "table"
    }

    "build JobConfigurationQuery object with write disposition" in {
      val jobConfig = BqQueryJobConfig(
        TableQuery(StandardTableSource(bqTableReference)),
        None,
        Some(WriteDisposition.WRITE_APPEND)
      ).toJava
      jobConfig.getWriteDisposition shouldEqual "WRITE_APPEND"
    }

    "set legacy sql if the query uses legacy table source" in {
      val jobConfig = BqQueryJobConfig(
        TableQuery(LegacyTableSource(bqTableReference)),
        None,
        None
      ).toJava
      jobConfig.getUseLegacySql shouldBe true
    }

    "not set legacy sql if the query uses standard table source" in {
      val jobConfig = BqQueryJobConfig(
        TableQuery(StandardTableSource(bqTableReference)),
        None,
        None
      ).toJava
      jobConfig.getUseLegacySql shouldBe false
    }

  }

  "BqQueryJob" should {

    "build Job object" in {
      val job = BqQueryJob(
        BqQueryJobConfig(
          TableQuery(StandardTableSource(bqTableReference)),
          None,
          None
        )
      ).toJava
      job.getConfiguration.getQuery.getQuery shouldEqual "SELECT * FROM `project.dataSet.table`"
    }

  }

  "BqLoadJob" should {

    val bqTableSchema =
      BqTableSchema(List(BqTableFieldSchema("fieldName", "fieldType")))
    val loadJobConfig = BqLoadJobConfig(bqTableReference, bqTableSchema)

    "build JobConfigurationLoad object with proper properties" which {

      val jobConfig = loadJobConfig.toJava

      "is DestinationTable" in {
        jobConfig.getDestinationTable.getProjectId shouldEqual "project"
        jobConfig.getDestinationTable.getDatasetId shouldEqual "dataSet"
        jobConfig.getDestinationTable.getTableId shouldEqual "table"
      }

      "is Schema" in {
        jobConfig.getSchema.getFields.get(0).getName shouldEqual "fieldName"
      }

      "is SourceFormat" in {
        jobConfig.getSourceFormat shouldEqual "CSV"
      }

      "is SourceFormat with JSON" in {
        val jobConfig =
          BqLoadJobConfig(bqTableReference, bqTableSchema, JsonFormat).toJava
        jobConfig.getSourceFormat shouldEqual "NEWLINE_DELIMITED_JSON"
      }

      "is SourceFormat with AVRO" in {
        val jobConfig =
          BqLoadJobConfig(bqTableReference, bqTableSchema, AvroFormat).toJava
        jobConfig.getSourceFormat shouldEqual "AVRO"
      }

      "is CreateDisposition" in {
        jobConfig.getCreateDisposition shouldEqual "CREATE_IF_NEEDED"
      }

      "is CreateDisposition with CREATE_NEVER" in {
        val jobConfig = BqLoadJobConfig(
          bqTableReference,
          bqTableSchema,
          CsvFormat,
          CreateDisposition.CREATE_NEVER
        ).toJava
        jobConfig.getCreateDisposition shouldEqual "CREATE_NEVER"
      }
    }

    "build Job object" which {
      val job = BqLoadJob(loadJobConfig).toJava

      "holds the given configuration" in {
        job.getConfiguration.getLoad.getSchema shouldEqual bqTableSchema.toJava
      }
    }
  }

  "BqExtractJob" should {

    val extractJobConfig =
      BqExtractJobConfig(bqTableReference, "destination uri")
    val jobConfig = extractJobConfig.toJava

    "build JobConfigurationExtract object with proper properties" which {

      "is source table" in {
        jobConfig.getSourceTable.getProjectId shouldEqual "project"
        jobConfig.getSourceTable.getDatasetId shouldEqual "dataSet"
        jobConfig.getSourceTable.getTableId shouldEqual "table"
      }

      "is destination uri" in {
        jobConfig.getDestinationUri shouldEqual "destination uri"
      }

      "is destination format" in {
        jobConfig.getPrintHeader shouldEqual false
        jobConfig.getDestinationFormat shouldEqual CsvFormat.show
        jobConfig.getFieldDelimiter shouldEqual ","
      }

      "is compression" in {
        jobConfig.getCompression shouldEqual "NONE"
      }

    }

    "build Job object" which {

      val job = BqExtractJob(extractJobConfig).toJava

      "holds the given configuration" in {
        job.getConfiguration.getExtract.getDestinationUri shouldEqual "destination uri"
      }
    }

  }

}
