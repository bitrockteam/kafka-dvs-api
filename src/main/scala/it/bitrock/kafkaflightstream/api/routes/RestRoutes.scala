package it.bitrock.kafkaflightstream.api.routes

import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import it.bitrock.kafkaflightstream.api.routes.internals.{InternalsHandler, InternalsResource}

class RestRoutes(internalsService: InternalsHandler)(implicit val materializer: ActorMaterializer) {

  val routes: Route = cors() { internals }

  def internals: Route = InternalsResource.routes(internalsService)

}
