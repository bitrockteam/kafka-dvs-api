package it.bitrock.kafkaflightstream.api.services

import java.io.IOException

import akka.http.scaladsl.model.{HttpHeader, HttpResponse, StatusCodes}
import it.bitrock.kafkaflightstream.api.client.ksqlserver.definitions.{
  CommandStatus,
  Query,
  RunStatements,
  RunStatementsError,
  currentStatus,
  queries
}
import it.bitrock.kafkaflightstream.api.client.ksqlserver.ksql.RunKsqlStatementsResponse
import it.bitrock.kafkaflightstream.api.client.ksqlserver.ksql.RunKsqlStatementsResponse.{BadRequest, InternalServerError, OK}
import it.bitrock.kafkaflightstream.api.routes.definitions.{ErrorDescription, KsqlSuccessResponse}
import it.bitrock.kafkaflightstream.api.services.KsqlOpsSpec.KsqlClientFactoryDouble
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages._
import it.bitrock.kafkaflightstream.api.services.models.Response.{
  BadRequestResponse,
  CreatedResponse,
  InternalServerErrorResponse,
  NoContentResponse
}
import it.bitrock.kafkaflightstream.api.{BaseAsyncSpec, TestValues}

import scala.concurrent.Future

class KsqlOpsSpec extends BaseAsyncSpec with TestValues {

  "KsqlOps exec statement" should {

    "fail when not able to unmarshal ksql response" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Left(Left(new IOException))))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(unmarshallingErrorResponse)))
    }

    "fail when ksql http response code is unexpected" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Left(Right(HttpResponse(StatusCodes.UnprocessableEntity)))))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(unexpectedStatusCodeResponse(StatusCodes.UnprocessableEntity))))
    }

    "fail when ksql response is bad request" in {
      val em = "bad_request"

      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(BadRequest(RunStatementsError(message = em)))))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe BadRequestResponse(ErrorDescription(badRequestResponse(em))))
    }

    "fail when ksql response is internal server error" in {
      val em = "internal_server_error"

      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(InternalServerError(RunStatementsError(message = em)))))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(internalServerErrorResponse(em))))
    }

    "recover with error when future fails" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future.failed(new IOException()))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(genericErrorMessage)))
    }

    "apply mapper if successful" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq()))))

      ksqlOps
        .execStatement("whatever", _ => Future.successful(NoContentResponse))
        .map(_ shouldBe NoContentResponse)
    }
  }

  "KsqlOps create stream" should {

    "fail if not statement run" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq()))))

      ksqlOps
        .createStream("whatever", identity)
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(genericErrorMessage)))
    }

    "fail if stream name does not match" in {
      val message = "success"

      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq(currentStatus("wrong", CommandStatus(message = message)))))))

      ksqlOps
        .createStream("whatever", identity)
        .map(_ shouldBe BadRequestResponse(ErrorDescription(badRequestResponse(message))))
    }

    "succed when everything is ok" in {

      val ksqlOps = new KsqlOpsImpl(
        (_, _) => Future(Right(OK(IndexedSeq(currentStatus(s"stream/$DefaultStreamId/create", CommandStatus(message = "success"))))))
      )

      ksqlOps
        .createStream("whatever", _ + "path")
        .map(_ shouldBe CreatedResponse(KsqlSuccessResponse(DefaultStreamId, DefaultStreamId + "path")))
    }
  }

  "KsqlOps clean session" should {

    "succeed if no statement run" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Left(Left(new IOException()))))

      ksqlOps
        .cleanSession(Seq())
        .map(_ shouldBe NoContentResponse)
    }

    "fail if non handled statement run" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq(currentStatus("wrong", CommandStatus(message = "unused")))))))

      ksqlOps
        .cleanSession(Seq("whatever"))
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(genericErrorMessage)))
    }

    "fail if empty queries" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq(queries())))))

      val inputIds = Seq("whatever")

      ksqlOps
        .cleanSession(inputIds)
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(noMatchFound(inputIds: _*))))
    }

    "fail if not matching queries with input" in {
      val ksqlOps = new KsqlOpsImpl((_, _) => Future(Right(OK(IndexedSeq(queries(IndexedSeq(Query("id", IndexedSeq("not_whatever")))))))))

      val inputIds = Seq("whatever")

      ksqlOps
        .cleanSession(inputIds)
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(noMatchFound(inputIds: _*))))
    }

    "fail if terminate query fails" in {
      val ksqlOps = new KsqlOpsImpl(
        new KsqlClientFactoryDouble(
          showQuery = Future(Right(OK(IndexedSeq(queries(IndexedSeq(Query("id", IndexedSeq("whatever")))))))),
          terminate = Future(Left(Right(HttpResponse(StatusCodes.UnprocessableEntity)))),
          dropStream = Future.failed(new IOException())
        )
      )

      val inputIds = Seq("whatever")

      ksqlOps
        .cleanSession(inputIds)
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(unexpectedStatusCodeResponse(StatusCodes.UnprocessableEntity))))
    }

    "fail if drop stream fails" in {
      val ksqlOps = new KsqlOpsImpl(
        new KsqlClientFactoryDouble(
          showQuery = Future(Right(OK(IndexedSeq(queries(IndexedSeq(Query(DefaultStreamId, IndexedSeq(DefaultStreamId)))))))),
          terminate = Future(Right(OK(IndexedSeq(currentStatus(s"stream/$DefaultStreamId/create", CommandStatus(message = "success")))))),
          dropStream = Future.failed(new IOException())
        )
      )

      val inputIds = Seq(DefaultStreamId)

      ksqlOps
        .cleanSession(inputIds)
        .map(_ shouldBe InternalServerErrorResponse(ErrorDescription(genericErrorMessage)))
    }

    "succeed if everything is ok" in {
      val ksqlOps = new KsqlOpsImpl(
        new KsqlClientFactoryDouble(
          showQuery = Future(Right(OK(IndexedSeq(queries(IndexedSeq(Query(DefaultStreamId, IndexedSeq(DefaultStreamId)))))))),
          terminate = Future(Right(OK(IndexedSeq(currentStatus(s"stream/$DefaultStreamId/create", CommandStatus(message = "success")))))),
          dropStream = Future(Right(OK(IndexedSeq())))
        )
      )

      val inputIds = Seq(DefaultStreamId)

      ksqlOps
        .cleanSession(inputIds)
        .map(_ shouldBe NoContentResponse)
    }
  }
}

object KsqlOpsSpec {
  class KsqlClientFactoryDouble(
      showQuery: Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]],
      terminate: Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]],
      dropStream: Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]]
  ) extends KsqlClientFactory {
    override def runKsqlStatements(
        body: RunStatements,
        headers: List[HttpHeader]
    ): Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]] =
      body match {
        case RunStatements(s) if s.startsWith("TERMINATE")   => terminate
        case RunStatements(s) if s.startsWith("DROP STREAM") => dropStream
        case _                                               => showQuery
      }
  }
}
