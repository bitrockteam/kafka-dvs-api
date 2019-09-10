import sbt._

object ApiModelsDependencies {

  object Versions {

    lazy val Circe = "0.10.1"

  }

  lazy val prodDeps: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-http"     % Dependencies.Versions.AkkaHttp,
    "com.typesafe.akka" %% "akka-stream"   % Dependencies.Versions.Akka,
    "io.circe"          %% "circe-core"    % Versions.Circe,
    "io.circe"          %% "circe-generic" % Versions.Circe,
    "io.circe"          %% "circe-java8"   % Versions.Circe,
    "io.circe"          %% "circe-parser"  % Versions.Circe
  )

}
