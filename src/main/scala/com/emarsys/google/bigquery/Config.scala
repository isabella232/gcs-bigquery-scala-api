package com.emarsys.google.bigquery

import java.io.ByteArrayInputStream

import akka.actor.ActorSystem
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.bigquery.BigqueryScopes
import com.typesafe.config.ConfigRenderOptions

import scala.concurrent.duration.{Duration, MILLISECONDS}


trait Config {

  val system : ActorSystem

  val credentialWrite = GoogleCredential.fromStream(new ByteArrayInputStream(configAsJson("secret").getBytes)).createScoped(BigqueryScopes.all())

  val projectId = system.settings.config.getString("google.project.name")

  val jobPollTimeout = Duration(system.settings.config.getDuration("job-poll-timeout", MILLISECONDS), MILLISECONDS)

  private val googleConfig = system.settings.config getConfig "google"

  def configOfProject(properties: String) = {
    googleConfig.getConfig("project").getString(properties)
  }

  def configAsJson(properties: String) = {
    googleConfig.getValue(properties).render(ConfigRenderOptions.defaults().setJson(true).setOriginComments(false)).replace("\\\\","\\")
  }

}

object Config {



}