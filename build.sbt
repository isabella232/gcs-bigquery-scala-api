val v2_12 = "2.12.10"
val v2_13 = "2.13.1"

lazy val root = (project in file("."))
  .settings(
    Seq(
      crossScalaVersions := List(v2_13, v2_12),
      organization := "com.emarsys",
      name := "gcs-bigquery-scala-api",
      scalafmtOnCompile := true
    ): _*
  )
  .settings(Defaults.itSettings: _*)
  .settings(
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-Yrangepos",
      "-feature",
      "-language:higherKinds",
      "-language:existentials",
      "-unchecked",
      "-Xlint:_,-type-parameter-shadow",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
      "-Ywarn-extra-implicit",
      "-Ywarn-unused:imports",
      "-opt-warnings",
      "-target:jvm-1.8"
    ),
    scalacOptions ++= versionSpecificScalacOptions(scalaVersion.value),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    libraryDependencies ++= {
      val akkaV = "2.6.3"
      Seq(
        "com.chuusai"       %% "shapeless"            % "2.3.3",
        "com.typesafe.akka" %% "akka-actor"           % akkaV,
        "com.typesafe.akka" %% "akka-testkit"         % akkaV % "test",
        "com.google.cloud"  % "google-cloud-bigquery" % "1.102.0",
        "org.scalatest"     %% "scalatest"            % "3.1.0" % "test",
        "joda-time"         % "joda-time"             % "2.10.5"
      )
    }
  )
  .configs(IntegrationTest)

lazy val IntegrationTest = config("it") extend Test

inThisBuild(
  List(
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/emartech/gcs-bigquery-scala-api")),
    developers := List(
      Developer("andrasp3a", "Andras Papp", "andras.papp@emarsys.com", url("https://github.com/andrasp3a")),
      Developer("bkiss1988", "Balazs Kiss", "balazs.kiss@emarsys.com", url("https://github.com/bkiss1988")),
      Developer("doczir", "Robert Doczi", "doczi.r@gmail.com", url("https://github.com/doczir")),
      Developer("mfawal", "Margit Fawal", "margit.fawal@emarsys.com", url("https://github.com/mfawal")),
      Developer("miklos-martin", "Miklos Martin", "miklos.martin@gmail.com", url("https://github.com/miklos-martin"))
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/emartech/gcs-bigquery-scala-api"),
        "scm:git:git@github.com:emartech/gcs-bigquery-scala-api.git"
      )
    ),
    // These are the sbt-release-early settings to configure
    pgpPublicRing := file("./ci/local.pubring.asc"),
    pgpSecretRing := file("./ci/local.secring.asc"),
    releaseEarlyWith := SonatypePublisher
  )
)

def versionSpecificScalacOptions(scalaV: String) =
  if (scalaV == v2_12)
    Seq(
      "-Xfuture",
      "-Xsource:2.13",
      "-Yno-adapted-args",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ypartial-unification"
    )
  else
    Seq(
      "-Ymacro-annotations"
    )

fork in root := true
