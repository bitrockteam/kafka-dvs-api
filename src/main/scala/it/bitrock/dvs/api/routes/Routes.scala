package it.bitrock.dvs.api.routes

import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path, pathPrefix}
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import it.bitrock.dvs.api.config.WebSocketConfig

object Routes {
  final case class FlowFactories(flightListFlowFactory: FlowFactory, topsFlowFactory: FlowFactory, totalsFlowFactory: FlowFactory)

  def webSocketRoutes(webSocketConfig: WebSocketConfig, flowFactories: FlowFactories): Route = get {
    pathPrefix(webSocketConfig.pathPrefix) {
      path(webSocketConfig.flightListPath) {
        handleWebSocketMessages(flowFactories.flightListFlowFactory.flow)
      } ~
        path(webSocketConfig.topElementsPath) {
          handleWebSocketMessages(flowFactories.topsFlowFactory.flow)
        } ~
        path(webSocketConfig.totalElementsPath) {
          handleWebSocketMessages(flowFactories.totalsFlowFactory.flow)
        }
    }
  }

}
