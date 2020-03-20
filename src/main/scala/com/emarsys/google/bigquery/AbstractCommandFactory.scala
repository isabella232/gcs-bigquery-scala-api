package com.emarsys.google.bigquery

import com.emarsys.google.bigquery.model._
import com.emarsys.google.bigquery.builder._
import com.google.api.client.http.ByteArrayContent
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.Table
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}

trait AbstractCommandFactory {

  implicit val bigQuery: Bigquery
}

trait TableCommandFactory extends AbstractCommandFactory {

  def dropTable(tableRef: BqTableReference): DeleteTableCommand = {
    DeleteTableCommand(
      bigQuery
        .tables()
        .delete(tableRef.project, tableRef.dataSet, tableRef.table)
    )
  }

  def createTable(
      tableRef: BqTableReference,
      schema: BqTableSchema
  ): CreateTableCommand = {

    val table =
      new Table().setSchema(schema.toJava).setTableReference(tableRef.toJava)

    CreateTableCommand(
      bigQuery.tables().insert(tableRef.project, tableRef.dataSet, table)
    )
  }

  def insertAll(
      tableRef: BqTableReference,
      data: BqTableData
  ): InsertAllTableCommand = {

    InsertAllTableCommand(
      bigQuery
        .tabledata()
        .insertAll(
          tableRef.project,
          tableRef.dataSet,
          tableRef.table,
          data.toJava
        )
    )
  }

  def getTable(tableRef: BqTableReference): GetTableCommand = {
    GetTableCommand(
      bigQuery.tables().get(tableRef.project, tableRef.dataSet, tableRef.table)
    )
  }

}

trait JobCommandFactory extends AbstractCommandFactory {

  def copy(
      query: Query,
      destinationRef: BqTableReference,
      writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
      labels: Map[String, String] = Map.empty
  ): CopyCommand = {

    val jobConfig =
      BqQueryJobConfig(query, Some(destinationRef), Some(writeDisposition))

    CopyCommand(
      bigQuery
        .jobs()
        .insert(destinationRef.project, BqQueryJob(jobConfig, labels).toJava),
      jobConfig
    )
  }

  def append(query: Query, destinationRef: BqTableReference, labels: Map[String, String] = Map.empty): CopyCommand = {
    val jobConfig = BqQueryJobConfig(
      query,
      Some(destinationRef),
      Some(WriteDisposition.WRITE_APPEND)
    )
    CopyCommand(
      bigQuery
        .jobs()
        .insert(destinationRef.project, BqQueryJob(jobConfig, labels).toJava),
      jobConfig
    )
  }

  def insert(
      content: String,
      destinationRef: BqTableReference,
      schema: BqTableSchema,
      sourceFormat: FileFormat = CsvFormat,
      createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED
  ): InsertDataCommand = {

    val jobConfig =
      BqLoadJobConfig(destinationRef, schema, sourceFormat, createDisposition)

    InsertDataCommand(
      bigQuery
        .jobs()
        .insert(
          destinationRef.project,
          BqLoadJob(jobConfig).toJava,
          new ByteArrayContent("application/octet-stream", content.getBytes())
        )
    )
  }

  def query(query: Query, projectId: String, labels: Map[String, String] = Map.empty): QueryCommand = {
    val jobConfig = BqQueryJobConfig(query, None, None)

    QueryCommand(
      bigQuery.jobs().insert(projectId, BqQueryJob(jobConfig, labels).toJava)
    )
  }

  def extract(
      sourceTable: BqTableReference,
      destinationUri: String,
      labels: Map[String, String] = Map.empty
  ): ExtractCommand = {
    val jobConfig = BqExtractJobConfig(sourceTable, destinationUri)
    ExtractCommand(
      bigQuery
        .jobs()
        .insert(sourceTable.project, BqExtractJob(jobConfig, labels).toJava)
    )
  }

  def result(jobId: String, projectId: String): ResultCommand =
    ResultCommand(bigQuery.jobs().getQueryResults(projectId, jobId))

  def status(jobId: String, projectId: String): JobStatusCommand =
    JobStatusCommand(bigQuery.jobs().get(projectId, jobId))

}

trait CommandFactory extends TableCommandFactory with JobCommandFactory
