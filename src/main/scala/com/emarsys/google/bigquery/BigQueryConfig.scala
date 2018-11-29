package com.emarsys.google.bigquery

import java.io.ByteArrayInputStream
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.bigquery.BigqueryScopes
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import scala.concurrent.duration.{Duration, MILLISECONDS}

trait BigQueryConfig {

  private val config = ConfigFactory.load()

  private val googleConfig = config getConfig "google"

  val credentialWrite = GoogleCredential
    .fromStream(new ByteArrayInputStream(configAsJson("secret").getBytes))
    .createScoped(BigqueryScopes.all())

  val projectId = config.getString("google.project.name")

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

}

object BigQueryConfig extends BigQueryConfig
