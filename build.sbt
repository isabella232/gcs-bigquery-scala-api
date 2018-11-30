lazy val root = (project in file("."))
  .settings(
    Seq(
      scalaVersion := "2.12.7",
      organization := "com.emarsys",
      name := "gcs-bigquery-scala-api",
      scalafmtOnCompile := true
    ): _*)
  .settings(libraryDependencies ++= {
    val akkaV = "2.5.18"
    Seq(
      "com.typesafe.akka" %% "akka-actor"           % akkaV,
      "com.google.cloud"  % "google-cloud-bigquery" % "1.53.0",
      "com.chuusai"       %% "shapeless"            % "2.3.3",
      "com.typesafe.akka" %% "akka-testkit"         % akkaV % "test",
      "org.scalatest"     %% "scalatest"            % "3.0.5" % "test"
    )
  })

inThisBuild(List(
  licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/emartech/gcs-bigquery-scala-api")),
  developers := List(
    Developer("andrasp3a", "Andras Papp", "andras.papp@emarsys.com", url("https://github.com/andrasp3a")),
    Developer("bkiss1988", "Balazs Kiss", "balazs.kiss@emarsys.com", url("https://github.com/bkiss1988")),
    Developer("doczir", "Robert Doczi", "doczi.r@gmail.com", url("https://github.com/doczir")),
    Developer("mfawal", "Margit Fawal", "margit.fawal@emarsys.com", url("https://github.com/mfawal")),
    Developer("miklos-martin", "Miklos Martin", "miklos.martin@gmail.com", url("https://github.com/miklos-martin"))
  ),
  scmInfo := Some(ScmInfo(url("https://github.com/emartech/gcs-bigquery-scala-api"), "scm:git:git@github.com:emartech/gcs-bigquery-scala-api.git")),
  // These are the sbt-release-early settings to configure
  pgpPublicRing := file("./ci/local.pubring.asc"),
  pgpSecretRing := file("./ci/local.secring.asc"),
  releaseEarlyWith := SonatypePublisher
))
