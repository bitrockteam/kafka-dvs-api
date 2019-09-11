package it.bitrock.kafkaflightstream.api.routes

import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path, pathPrefix}
import akka.http.scaladsl.server.PathMatcher._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.tags.FlowFactoryKey
import it.bitrock.kafkaflightstream.tags.TaggedTypes._

class Routes(
    flowFactories: Map[FlowFactoryKey, FlowFactory],
    websocketConfig: WebsocketConfig
) {

  val routes: Route = streams

  def streams: Route = get {
    pathPrefix(websocketConfig.pathPrefix) {
      path(websocketConfig.flightsPath) {
        handleWebSocketMessages(flowFactories(flightFlowFactoryKey).flow)
      } ~
        path(websocketConfig.topElementsPath) {
          handleWebSocketMessages(flowFactories(topsFlowFactoryKey).flow)
        }
    }
  }

}
