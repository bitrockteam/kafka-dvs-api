package it.bitrock.kafkaflightstream.api.config

import pureconfig.generic.auto._

final case class AppConfig(
    kafka: KafkaConfig,
    server: ServerConfig
)

object AppConfig {

  def load: AppConfig = pureconfig.loadConfigOrThrow[AppConfig]

}
