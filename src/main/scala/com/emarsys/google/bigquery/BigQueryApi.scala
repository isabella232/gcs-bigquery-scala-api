package com.emarsys.google.bigquery

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery

object BigQueryApi extends GoogleCloudConfig {
  def apply(projectId: String, credential: GoogleCredential): Bigquery = {
    val builder = new Bigquery.Builder(
      new NetHttpTransport(),
      new JacksonFactory(),
      credential
    )
    builder
      .setHttpRequestInitializer(configureRequestTimeouts(builder.getHttpRequestInitializer))
      .setApplicationName(projectId)
      .build()
  }

  private def configureRequestTimeouts(
      existing: HttpRequestInitializer
  ): HttpRequestInitializer = (request: HttpRequest) => {
    existing.initialize(request)
    request
      .setReadTimeout(google.bigQuery.httpReadTimeout.toMillis.toInt)
      .setConnectTimeout(google.bigQuery.httpConnectionTimeout.toMillis.toInt);
    ()
  }
}
