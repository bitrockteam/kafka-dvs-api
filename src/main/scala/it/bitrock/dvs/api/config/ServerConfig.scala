package it.bitrock.dvs.api.config

import scala.concurrent.duration.FiniteDuration

final case class ServerConfig(
    host: String,
    port: Int,
    webSocket: WebSocketConfig
)

final case class WebSocketConfig(
    maxNumberFlights: Int,
    throttleDuration: FiniteDuration,
    pathPrefix: String,
    dvsPath: String
)
