package it.bitrock.kafkaflightstream.api.services.models

import it.bitrock.kafkaflightstream.api.routes.definitions.ErrorDescription

object Response {
  sealed trait ResponseType
  case object BadRequest          extends ResponseType
  case object InternalServerError extends ResponseType

  sealed trait Response
  case object NoContentResponse                                extends Response
  case class CreatedResponse[Content](content: Content)        extends Response
  case class BadRequestResponse(ed: ErrorDescription)          extends Response
  case class InternalServerErrorResponse(ed: ErrorDescription) extends Response
}
