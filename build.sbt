import Dependencies._
import ReleaseTransformations._

lazy val commonSettings = Seq(
  organization := "it.bitrock.kafkaflightstream",
  scalaVersion := Versions.Scala
)

lazy val apiModelsCompileSettings = Seq(
  Compile / guardrailTasks := List(
    ScalaServer((Compile / resourceDirectory).value / "api.yaml", pkg = "it.bitrock.kafkaflightstream.api.routes"),
    ScalaClient((Compile / resourceDirectory).value / "api.yaml", pkg = "it.bitrock.kafkaflightstream.api.routes")
  )
)

lazy val compileSettings = Seq(
  Compile / compile := (Compile / compile)
    .dependsOn(
      Compile / scalafmtSbt,
      Compile / scalafmtAll
    )
    .value,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "utf8",
    "-Xlint:missing-interpolator",
    "-Xlint:private-shadow",
    "-Xlint:type-parameter-shadow",
    "-Ywarn-dead-code",
    "-Ywarn-unused",
    "-Ypartial-unification"
  )
)

lazy val dependenciesSettings = Seq(
  credentials ++= Seq(
    baseDirectory.value / ".sbt" / ".credentials",
    Path.userHome / ".sbt" / ".credentials.flightstream"
  ).collect {
    case c if c.exists => Credentials(c)
  },
  excludeDependencies ++= excludeDeps,
  libraryDependencies ++= prodDeps ++ testDeps,
  resolvers ++= CustomResolvers.resolvers
)

lazy val publishSettings = Seq(
  Test / publishArtifact := false,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepTask(publishLocal in Docker),
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val testSettings = Seq(
  Test / logBuffered := false,
  Test / parallelExecution := false
)

lazy val integrationTestSettings = Defaults.itSettings ++ Seq(
  IntegrationTest / logBuffered := false,
  IntegrationTest / parallelExecution := false
) ++ inConfig(IntegrationTest)(org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings)

lazy val apiModels = (project in file("api-models"))
  .settings(
    name := "kafka-flightstream-api-models",
    libraryDependencies ++= ApiModelsDependencies.prodDeps,
    Compile / logLevel := Level.Error
  )
  .settings(commonSettings: _*)
  .settings(apiModelsCompileSettings: _*)

lazy val root = (project in file("."))
  .settings(
    name := "kafka-flightstream-api"
  )
  .settings(commonSettings: _*)
  .settings(compileSettings: _*)
  .settings(dependenciesSettings: _*)
  .settings(publishSettings: _*)
  .settings(testSettings: _*)
  .configs(IntegrationTest)
  .settings(integrationTestSettings: _*)
  .dependsOn(apiModels)

/**
  * sbt-native-packager plugin
  */
enablePlugins(JavaAppPackaging, DockerPlugin, AshScriptPlugin)

dockerBaseImage := "openjdk:8-jre-alpine"
dockerRepository := Option(sys.env.getOrElse("DOCKER_REPOSITORY", "local"))
maintainer in Docker := "Alessio Nisini"

// Fix for sbt-native-packager 1.3.19
daemonUserUid in Docker := None
daemonUser in Docker := "root"
