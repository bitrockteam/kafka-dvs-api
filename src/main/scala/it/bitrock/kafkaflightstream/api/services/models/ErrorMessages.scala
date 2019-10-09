package it.bitrock.kafkaflightstream.api.services.models

import akka.http.scaladsl.model.StatusCode

object ErrorMessages {

  type ErrorMessage = String

  final val genericErrorMessage: ErrorMessage = "A generic error occurred"

  final val unmarshallingErrorResponse: ErrorMessage = "Unmarshalling error while processing ksql request"

  def badRequestResponse(message: String): ErrorMessage = s"Bad request error while processing ksql request: $message"

  def internalServerErrorResponse(message: String): ErrorMessage = s"Internal server error while processing ksql request: $message"

  def unexpectedStatusCodeResponse(statusCode: StatusCode): ErrorMessage =
    s"Unexpected status code: $statusCode while processing ksql request"

  def noMatchFound(stramIds: String*): ErrorMessage = s"No match was found for this streams: $stramIds"

}
