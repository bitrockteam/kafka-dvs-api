package it.bitrock.kafkaflightstream.api.services

import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.routes.definitions.{CleanSessionRequest, ErrorDescription}
import it.bitrock.kafkaflightstream.api.routes.session.SessionResource.cleanSessionResponse
import it.bitrock.kafkaflightstream.api.routes.session.SessionResource.cleanSessionResponse._
import it.bitrock.kafkaflightstream.api.routes.session.{SessionHandler, SessionResource}
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages.genericErrorMessage
import it.bitrock.kafkaflightstream.api.services.models.Response.{
  BadRequestResponse,
  InternalServerErrorResponse,
  NoContentResponse,
  Response
}

import scala.concurrent.{ExecutionContext, Future}

class SessionService(ksqlOps: KsqlOps)(implicit ec: ExecutionContext) extends SessionHandler with LazyLogging {

  def cleanSession(respond: SessionResource.cleanSessionResponse.type)(
      body: CleanSessionRequest
  ): Future[cleanSessionResponse] = {

    val inputStreamIds: IndexedSeq[String] = body.streamIds.map(_.toUpperCase)
    ksqlOps.cleanSession(inputStreamIds).map(toSessionResponse)
  }

  def toSessionResponse(response: Response): cleanSessionResponse = response match {
    case NoContentResponse               => NoContent
    case InternalServerErrorResponse(ed) => InternalServerError(ed)
    case BadRequestResponse(ed)          => InternalServerError(ed)
    case _                               => InternalServerError(ErrorDescription(genericErrorMessage))
  }
}
