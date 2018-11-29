lazy val commonSettings = Seq(
  scalaVersion := "2.12.7",
  organization := "com.emarsys",
  name := "gcs-bigquery-scala-api",
  version := "1.1.0"
)

lazy val IntegrationTest = config("it") extend Test
lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(

libraryDependencies ++= {
  val akkaV = "2.5.18"
  Seq(
    "com.typesafe.akka"         %% "akka-actor"                         % akkaV,
    "com.google.cloud"          %  "google-cloud-bigquery"              % "1.53.0",
    "com.chuusai"               %% "shapeless"                          % "2.3.3",
    "com.typesafe.akka"         %% "akka-testkit"                       % akkaV % "test",
    "org.scalatest"             %% "scalatest"                          % "3.0.5" % "test"
  )
}

)

publishTo := Some(Resolver.file("releases", new File("releases")))
