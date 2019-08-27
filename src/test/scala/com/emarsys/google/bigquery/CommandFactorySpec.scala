package com.emarsys.google.bigquery

import com.emarsys.google.bigquery.model.{BqTableData, BqTableReference, BqTableSchema}
import com.emarsys.google.bigquery.builder.{StandardTableSource, TableQuery}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.scalatest.{Matchers, WordSpec}

class CommandFactorySpec extends WordSpec with Matchers with CommandFactory with GoogleCloudConfig {

  lazy val bigQuery = BigQueryApi(google.projectName, credentialWrite)

  val tableReference = BqTableReference("[project]", "[dataSet]", "[table]")

  "Command Api" should {

    "create a job" which {

      "is dropTable" in {

        val command = dropTable(tableReference).command

        command.getProjectId shouldEqual "[project]"
        command.getDatasetId shouldEqual "[dataSet]"
        command.getTableId shouldEqual "[table]"
      }

      "is createTable" in {

        val command = createTable(tableReference, BqTableSchema(Nil)).command

        command.getProjectId shouldEqual "[project]"
        command.getDatasetId shouldEqual "[dataSet]"
      }

      "is insertAll" in {

        val command = insertAll(tableReference, BqTableData()).command

        command.getProjectId shouldEqual "[project]"
        command.getDatasetId shouldEqual "[dataSet]"
        command.getTableId shouldEqual "[table]"
      }

      "is copy" in {

        val command = copy(
          TableQuery(StandardTableSource(tableReference)),
          tableReference
        ).command

        command.getProjectId shouldEqual "[project]"
      }

      "is copy with truncate write disposition" in {

        val command = copy(
          TableQuery(StandardTableSource(tableReference)),
          tableReference,
          WriteDisposition.WRITE_TRUNCATE
        )

        command.jobConfig.disposition.get shouldEqual WriteDisposition.WRITE_TRUNCATE
      }

      "is insert with truncate write disposition" in {

        val command =
          insert("[csv]", tableReference, BqTableSchema(Nil)).command

        command.getProjectId shouldEqual "[project]"
      }

      "is query" in {

        val command = query(
          TableQuery(StandardTableSource(tableReference)),
          "[project]"
        ).command

        command.getProjectId shouldEqual "[project]"
      }

      "is query result" in {

        val command = result("[jobId]", "[project]").command

        command.getProjectId shouldEqual "[project]"
        command.getJobId shouldEqual "[jobId]"
      }

      "is extract" in {

        val command = extract(tableReference, "destination uri").command

        command.getProjectId shouldEqual "[project]"
      }
    }
  }
}
