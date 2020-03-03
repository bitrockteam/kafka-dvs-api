package it.bitrock.dvs.api.config

import scala.concurrent.duration.FiniteDuration

final case class ServerConfig(
    host: String,
    port: Int,
    rest: RestConfig,
    webSocket: WebSocketConfig
)

final case class RestConfig(healthPath: String)

final case class WebSocketConfig(
    maxNumberFlights: Int,
    maxNumberAirports: Int,
    throttleDuration: FiniteDuration,
    pathPrefix: String,
    dvsPath: String
)
