package it.bitrock.kafkaflightstream.api.services

import it.bitrock.kafkaflightstream.api.routes.internals.{InternalsHandler, InternalsResource}
import it.bitrock.kafkaflightstream.api.routes.internals.InternalsResource.healthResponse

import scala.concurrent.Future

class InternalsService extends InternalsHandler {

  override def health(respond: healthResponse.type)(): Future[InternalsResource.healthResponse] =
    Future.successful(respond.OK)

}
