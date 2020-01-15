package it.bitrock.dvs.api.routes

import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path, pathPrefix}
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.tags.FlowFactoryKey
import it.bitrock.dvs.tags.TaggedTypes._

class Routes(
    flowFactories: Map[FlowFactoryKey, FlowFactory],
    websocketConfig: WebsocketConfig
) {

  val routes: Route = streams

  def streams: Route = get {
    pathPrefix(websocketConfig.pathPrefix) {
      path(websocketConfig.flightListPath) {
        handleWebSocketMessages(flowFactories(flightListFlowFactoryKey).flow)
      } ~
        path(websocketConfig.topElementsPath) {
          handleWebSocketMessages(flowFactories(topsFlowFactoryKey).flow)
        } ~
        path(websocketConfig.totalElementsPath) {
          handleWebSocketMessages(flowFactories(totalsFlowFactoryKey).flow)
        }
    }
  }

}
