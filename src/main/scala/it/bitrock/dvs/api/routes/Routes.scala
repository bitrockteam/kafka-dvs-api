package it.bitrock.dvs.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path, pathPrefix}
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import it.bitrock.dvs.api.config.{RestConfig, WebSocketConfig}

object Routes {

  def healthRoutes(restConfig: RestConfig): Route = get {
    path(restConfig.healthPath) {
      complete(StatusCodes.OK)
    }
  }

  def webSocketRoutes(webSocketConfig: WebSocketConfig, flowFactory: FlowFactory): Route = get {
    pathPrefix(webSocketConfig.pathPrefix) {
      path(webSocketConfig.dvsPath) {
        handleWebSocketMessages(flowFactory.flow)
      }
    }
  }

}
