package it.bitrock.dvs.api.routes

import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path, pathPrefix}
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.Route
import it.bitrock.dvs.api.config.WebSocketConfig

object Routes {

  def webSocketRoutes(webSocketConfig: WebSocketConfig, flowFactory: FlowFactory): Route = get {
    pathPrefix(webSocketConfig.pathPrefix) {
      path(webSocketConfig.dvsPath) {
        handleWebSocketMessages(flowFactory.flow)
      }
    }
  }

}
