package it.bitrock.kafkaflightstream.api.services

import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.routes.definitions.{CreateKsqlRequest, ErrorDescription, KsqlSuccessResponse}
import it.bitrock.kafkaflightstream.api.routes.ksql.KsqlResource.createStreamResponse._
import it.bitrock.kafkaflightstream.api.routes.ksql.{KsqlHandler, KsqlResource}
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages.genericErrorMessage
import it.bitrock.kafkaflightstream.api.services.models.Response.{
  BadRequestResponse,
  CreatedResponse,
  InternalServerErrorResponse,
  Response
}

import scala.concurrent.{ExecutionContext, Future}

class KsqlService(ksqlOps: KsqlOps, websocketConfig: WebsocketConfig)(implicit ec: ExecutionContext) extends KsqlHandler with LazyLogging {

  def createStream(respond: KsqlResource.createStreamResponse.type)(req: CreateKsqlRequest): Future[KsqlResource.createStreamResponse] =
    ksqlOps.createStream(req.ksql, websocketConfig.pathForStream).map(toSessionResponse)

  def toSessionResponse(response: Response): KsqlResource.createStreamResponse = response match {
    case InternalServerErrorResponse(ed)          => InternalServerError(ed)
    case BadRequestResponse(ed)                   => BadRequest(ed)
    case CreatedResponse(ed: KsqlSuccessResponse) => Created(ed)
    case _                                        => InternalServerError(ErrorDescription(genericErrorMessage))
  }

}
