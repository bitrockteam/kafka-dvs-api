package it.bitrock.kafkaflightstream.api.routes

import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import it.bitrock.kafkaflightstream.api.routes.internals.{InternalsHandler, InternalsResource}
import it.bitrock.kafkaflightstream.api.routes.ksql.{KsqlHandler, KsqlResource}
import it.bitrock.kafkaflightstream.api.routes.session.{SessionHandler, SessionResource}

class RestRoutes(internalsService: InternalsHandler, ksqlService: KsqlHandler, sessionService: SessionHandler)(
    implicit val materializer: ActorMaterializer
) {

  val routes: Route = cors() { internals }

  def internals: Route = InternalsResource.routes(internalsService)

  def ksql: Route = KsqlResource.routes(ksqlService)

  def session: Route = SessionResource.routes(sessionService)

}
