import sbt._

object Dependencies {

  object CustomResolvers {

    lazy val BitrockNexus = "Bitrock Nexus" at "https://nexus.reactive-labs.io/repository/maven-bitrock-public/"
    lazy val Confluent    = "confluent" at "https://packages.confluent.io/maven/"

    lazy val resolvers: Seq[Resolver] = Seq(BitrockNexus, Confluent)

  }

  object Versions {
    lazy val Scala               = "2.12.15"
    lazy val Akka                = "2.6.18"
    lazy val AkkaHttp            = "10.2.7"
    lazy val AkkaHttpCors        = "1.1.2"
    lazy val ConfluentPlatform   = "5.4.6"
    lazy val EmbeddedKafka       = "5.4.1.2"
    lazy val JakartaWsRs         = "2.1.6"
    lazy val Kafka               = "2.4.1"
    lazy val KafkaDVS            = "1.0.20"
    lazy val TestCommons         = "0.0.13"
    lazy val KafkaCommons        = "0.0.13"
    lazy val LogbackClassic      = "1.2.10"
    lazy val PureConfig          = "0.17.1"
    lazy val ScalaLogging        = "3.9.4"
    lazy val Slf4j               = "1.7.32"
    lazy val Mockito             = "4.4.0"
    lazy val ScalafixSortImports = "0.6.1"
    lazy val ScalacheckShapeless = "1.3.0"
    lazy val ScalaTestAutofix    = "3.1.0.1"
    lazy val ScalaTestMockito    = "3.1.0.0"
    lazy val ScalaTestPlus       = "3.2.10.0"
    lazy val Cats                = "2.7.0"
  }

  object Logging {

    lazy val prodDeps: Seq[ModuleID] = Seq(
      "ch.qos.logback"              % "logback-classic"  % Versions.LogbackClassic, // required by scala-logging
      "com.typesafe.scala-logging" %% "scala-logging"    % Versions.ScalaLogging,
      "org.slf4j"                   % "log4j-over-slf4j" % Versions.Slf4j // mandatory when log4j gets excluded
    )

    lazy val excludeDeps: Seq[ExclusionRule] = Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("log4j", "log4j")
    )

  }

  lazy val prodDeps: Seq[ModuleID] = Seq(
    "com.typesafe.akka"     %% "akka-http"               % Versions.AkkaHttp,
    "com.typesafe.akka"     %% "akka-stream"             % Versions.Akka,
    "ch.megard"             %% "akka-http-cors"          % Versions.AkkaHttpCors,
    "com.github.pureconfig" %% "pureconfig"              % Versions.PureConfig,
    "com.typesafe.akka"     %% "akka-http-spray-json"    % Versions.AkkaHttp,
    "io.confluent"           % "kafka-avro-serializer"   % Versions.ConfluentPlatform,
    "it.bitrock.dvs"        %% "kafka-dvs-avro-schemas"  % Versions.KafkaDVS,
    "it.bitrock"            %% "kafka-commons"           % Versions.KafkaCommons,
    "org.apache.kafka"       % "kafka-clients"           % Versions.Kafka,
    "io.confluent"           % "monitoring-interceptors" % Versions.ConfluentPlatform exclude ("org.apache.kafka", "kafka-clients"),
    "org.typelevel"         %% "cats-core"               % Versions.Cats
  ) ++ Logging.prodDeps

  lazy val testDeps: Seq[ModuleID] = Seq(
    "com.typesafe.akka"          %% "akka-http-testkit"              % Versions.AkkaHttp,
    "com.typesafe.akka"          %% "akka-stream-testkit"            % Versions.Akka,
    "io.github.embeddedkafka"    %% "embedded-kafka-schema-registry" % Versions.EmbeddedKafka,
    "it.bitrock"                 %% "test-commons"                   % Versions.TestCommons,
    "jakarta.ws.rs"               % "jakarta.ws.rs-api"              % Versions.JakartaWsRs, // mandatory when javax.ws.rs-api gets excluded
    "org.mockito"                 % "mockito-core"                   % Versions.Mockito,
    "org.scalatestplus"          %% "mockito-1-10"                   % Versions.ScalaTestMockito,
    "org.scalatestplus"          %% "scalacheck-1-15"                % Versions.ScalaTestPlus,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.15"      % Versions.ScalacheckShapeless
  ).map(_ % s"$Test,$IntegrationTest")

  lazy val excludeDeps: Seq[ExclusionRule] = Seq(
    ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
  ) ++ Logging.excludeDeps

}
