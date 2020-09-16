package com.emarsys.google.bigquery

import com.emarsys.google.bigquery.GoogleCloudConfig.GoogleConfig
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.http.HttpTransportOptions

object BigQueryApi extends GoogleCloudConfig {
  def apply(projectId: String, credential: GoogleCredential, config: GoogleConfig): Bigquery = {
    val builder = bigQueryBuilder(config, credential)

    builder
      .setHttpRequestInitializer(configureRequestTimeouts(Option(builder.getHttpRequestInitializer)))
      .setApplicationName(projectId)
      .build()
  }

  private def bigQueryBuilder(config: GoogleConfig, credential: GoogleCredential): Bigquery.Builder = {
    if (config.useWorkloadIdentityAuth) {
      val transportOptions = HttpTransportOptions
        .newBuilder()
        .build()
      val bigQueryOptions = BigQueryOptions
        .newBuilder()
        .setTransportOptions(transportOptions)
        .build()

      new Bigquery.Builder(
        new NetHttpTransport(),
        new JacksonFactory(),
        transportOptions.getHttpRequestInitializer(bigQueryOptions)
      )
    } else {
      new Bigquery.Builder(
        new NetHttpTransport(),
        new JacksonFactory(),
        credential
      )
    }
  }

  private def configureRequestTimeouts(
      existing: Option[HttpRequestInitializer]
  ): HttpRequestInitializer = (request: HttpRequest) => {
    existing.foreach(_.initialize(request))
    request
      .setReadTimeout(google.bigQuery.httpReadTimeout.toMillis.toInt)
      .setConnectTimeout(google.bigQuery.httpConnectionTimeout.toMillis.toInt);
    ()
  }
}
