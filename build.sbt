lazy val commonSettings = Seq(
  scalaVersion := "2.12.2",
  organization := "com.emarsys",
  name := "gcs-bigquery-scala-api",
  version := "1.0.9"
)

lazy val IntegrationTest = config("it") extend Test
lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka"         %% "akka-actor"                         % "2.5.0",
    "org.typelevel"             %% "cats"                               % "0.9.0",
    "com.google.cloud"          %  "google-cloud-bigquery"              % "0.13.0-beta",
    "com.chuusai"               %% "shapeless"                          % "2.3.2",
    "com.typesafe.akka"         %% "akka-testkit"                       % "2.5.0" % "test",
    "org.scalatest"             %% "scalatest"                          % "3.0.1" % "test"
  )
}

)

publishTo := Some(Resolver.file("releases", new File("releases")))