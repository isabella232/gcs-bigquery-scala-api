package com.emarsys.google.bigquery

import akka.actor.ActorSystem
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson.JacksonFactory
import com.google.api.services.bigquery.Bigquery

object BigQueryApi {


  def apply(projectId: String, credential: GoogleCredential): Bigquery =
    new Bigquery.Builder(new NetHttpTransport(), new JacksonFactory(), credential)
      .setApplicationName(projectId)
      .build()

}
