package it.bitrock.dvs.api.services

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives.{complete, path}
import akka.http.scaladsl.server.Route

object InternalsService {
  def healthCheckRoute: Route =
    path("health") {
      complete((OK, "ok"))
    }
}
