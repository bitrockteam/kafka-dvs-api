package it.bitrock.dvs.api.routes

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives.{complete, path}
import akka.http.scaladsl.server.Route

object HealthRoute {
  def healthCheckRoute: Route =
    path("health") {
      complete((OK, "ok"))
    }
}
