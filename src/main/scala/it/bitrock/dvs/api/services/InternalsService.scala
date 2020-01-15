package it.bitrock.dvs.api.services

import it.bitrock.dvs.api.routes.internals.{InternalsHandler, InternalsResource}

import scala.concurrent.Future

class InternalsService extends InternalsHandler {

  override def health(respond: InternalsResource.healthResponse.type)(): Future[InternalsResource.healthResponse] =
    Future.successful(respond.OK)

}
