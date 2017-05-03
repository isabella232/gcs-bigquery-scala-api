lazy val commonSettings = Seq(
  scalaVersion := "2.12.2",
  organization := "com.emarsys",
  name := "gcs-bigquery-scala-api",
  version := "1.0.0"
)

lazy val IntegrationTest = config("it") extend Test
lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor"              % "2.5.0",
    "org.typelevel"             %% "cats"                               % "0.9.0",
    "com.google.cloud.dataflow" %  "google-cloud-dataflow-java-sdk-all" % "1.7.0"
  )
}

)

publishTo := Some(Resolver.file("releases", new File("releases")))