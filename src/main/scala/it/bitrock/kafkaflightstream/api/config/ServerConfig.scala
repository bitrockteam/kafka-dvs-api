package it.bitrock.kafkaflightstream.api.config

import akka.http.scaladsl.model.Uri

import scala.concurrent.duration.FiniteDuration

final case class ServerConfig(
    host: String,
    port: Int,
    websocket: WebsocketConfig
)

final case class WebsocketConfig(
    throttleDuration: FiniteDuration,
    cleanupDelay: FiniteDuration,
    pathPrefix: String,
    flightsPath: String,
    flightListPath: String,
    topElementsPath: String,
    totalElementsPath: String,
    ksqlPath: String
) {
  def pathForStream(streamName: String): String =
    Uri.Path(pathPrefix)./(ksqlPath)./(streamName).toString
}
