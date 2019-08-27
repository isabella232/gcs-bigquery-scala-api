package com.emarsys.google.bigquery

import java.io.ByteArrayInputStream

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.bigquery.BigqueryScopes
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}

import scala.concurrent.duration._

trait GoogleCloudConfig {
  private val config = ConfigFactory.load()

  private val googleConfig = config.getConfig("google")

  val credentialWrite: GoogleCredential = GoogleCredential
    .fromStream(new ByteArrayInputStream(configAsJson("secret").getBytes))
    .createScoped(BigqueryScopes.all())

  val jobPollTimeout = Duration(
    googleConfig.getDuration("job-poll-timeout", MILLISECONDS),
    MILLISECONDS
  )

  def configOfProject(properties: String) = {
    googleConfig.getConfig("project").getString(properties)
  }

  def configAsJson(properties: String) = {
    googleConfig
      .getValue(properties)
      .render(
        ConfigRenderOptions.defaults().setJson(true).setOriginComments(false)
      )
      .replace("\\\\", "\\")
  }

  lazy val google =
    GoogleCloudConfig.GoogleConfig(
      projectName = googleConfig.getString("project.name"),
      storageBucket = googleConfig.getString("storage.bucket"),
      bigQuery = GoogleCloudConfig.BigQueryConfig(
        dataset = googleConfig.getString("bigQuery.dataset"),
        resultsDataset = googleConfig.getString("bigQuery.resultsDataset"),
        jobTimeout = GoogleCloudConfig.getFiniteDuration(googleConfig, "bigQuery.job-timeout"),
        queryResultExpiration = googleConfig.getInt("query-result-expiration-seconds").seconds
      )
    )

}

object GoogleCloudConfig extends GoogleCloudConfig {
  case class GoogleConfig(
      projectName: String,
      storageBucket: String,
      bigQuery: BigQueryConfig
  )
  case class BigQueryConfig(
      dataset: String,
      resultsDataset: String,
      jobTimeout: FiniteDuration,
      queryResultExpiration: FiniteDuration
  )
  def getFiniteDuration(conf: com.typesafe.config.Config, key: String): FiniteDuration =
    Some(Duration(conf.getString(key))) collect { case d: FiniteDuration => d } get
}
