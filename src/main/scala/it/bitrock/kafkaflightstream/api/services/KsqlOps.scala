package it.bitrock.kafkaflightstream.api.services

import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.headers.Accept
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.client.ksqlserver.definitions.{CommandStatus, RunStatements, StatementRun, currentStatus, queries}
import it.bitrock.kafkaflightstream.api.routes.definitions.{ErrorDescription, KsqlSuccessResponse}
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages._
import it.bitrock.kafkaflightstream.api.services.models.KsqlStatements.{KsqlStatement, ShowQueries, dropStreamDeleteTopic, terminateQuery}
import it.bitrock.kafkaflightstream.api.services.models.Response._

import scala.concurrent.{ExecutionContext, Future}

trait KsqlOps {
  final case class QueryAndStream(queryId: String, streamId: String)
  def cleanSession(inputStreamIds: Seq[String]): Future[Response]
  def createStream(ksql: String, pathForStream: String => String): Future[Response]
}

class KsqlOpsImpl(ksqlClientFactory: KsqlClientFactory)(implicit ec: ExecutionContext) extends KsqlOps with LazyLogging {
  private val commandId_r  = "[^/]+/([^/]+)/.+".r
  private val streamName_r = "[^/]+/([^/]+)/.+".r

  private val accept = Accept(MediaTypes.`application/json`)

  def createStream(ksql: KsqlStatement, pathForStream: String => String): Future[Response] = {

    val mapper: Seq[StatementRun] => Future[Response] = _.collectFirst {
      case currentStatus(streamName_r(streamName), CommandStatus("SUCCESS", _)) =>
        Future.successful(CreatedResponse(KsqlSuccessResponse(streamName, pathForStream(streamName))))
      case currentStatus(_, CommandStatus(_, message)) => errorResponse(BadRequest, badRequestResponse(message))

    } getOrElse errorResponse(InternalServerError, genericErrorMessage)

    execStatement(ksql, mapper)
  }

  def cleanSession(inputStreamIds: Seq[String]): Future[Response] = {
    val correctMapper: Seq[StatementRun] => Future[Response] = _.collectFirst {

      case queries(querySeq) =>
        val queryAndStream =
          querySeq
            .filter(_.sinks.intersect(inputStreamIds).nonEmpty)
            .map(x => QueryAndStream(x.id, x.sinks.head))

        queryAndStream match {
          case Seq() => errorResponse(InternalServerError, noMatchFound(inputStreamIds: _*))
          case _ =>
            val correctMapper2: Seq[StatementRun] => Future[Response] = x => {
              val queryIds = x.collect {
                case currentStatus(commandId_r(commandId), CommandStatus("SUCCESS", _)) => commandId
              }
              val streamIds = queryAndStream.collect {
                case QueryAndStream(queryId, streamId) if queryIds.contains(queryId) => streamId
              }
              execStatement(dropStreamDeleteTopic(streamIds: _*), _ => Future.successful(NoContentResponse))
            }
            execStatement(terminateQuery(queryAndStream.map(_.queryId): _*), correctMapper2)
        }

    } getOrElse errorResponse(InternalServerError, genericErrorMessage)

    inputStreamIds match {
      case Seq() => Future.successful(NoContentResponse)
      case _     => execStatement(ShowQueries, correctMapper)
    }
  }

  def execStatement(statement: KsqlStatement, mapper: Seq[StatementRun] => Future[Response]): Future[Response] = {

    ksqlClientFactory
      .runKsqlStatements(RunStatements(statement), List(accept))
      .flatMap {
        case Left(value) =>
          value match {
            case Left(e)  => errorResponse(InternalServerError, unmarshallingErrorResponse, Some(e))
            case Right(r) => errorResponse(InternalServerError, unexpectedStatusCodeResponse(r.status))
          }
        case Right(value) =>
          value.fold(
            mapper,
            error => errorResponse(BadRequest, badRequestResponse(error.message)),
            error => errorResponse(InternalServerError, internalServerErrorResponse(error.message))
          )
      }
      .recoverWith { case _ => errorResponse(InternalServerError, genericErrorMessage) }
  }

  private def errorResponse(rt: ResponseType, message: String, e: Option[Throwable] = None): Future[Response] = {
    e.fold(logger.error(message))(logger.error(message, _))
    Future.successful(createResponse(ErrorDescription(message), rt))
  }

  private def createResponse(ed: ErrorDescription, rt: ResponseType): Response = rt match {
    case BadRequest          => BadRequestResponse(ed)
    case InternalServerError => InternalServerErrorResponse(ed)
  }
}
