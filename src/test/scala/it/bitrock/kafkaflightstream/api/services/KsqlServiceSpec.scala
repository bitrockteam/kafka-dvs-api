package it.bitrock.kafkaflightstream.api.services

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpHeader, HttpResponse, StatusCodes}
import akka.testkit.TestKit
import it.bitrock.kafkaflightstream.api.client.ksqlserver.definitions.{CommandStatus, RunStatements, RunStatementsError, currentStatus}
import it.bitrock.kafkaflightstream.api.client.ksqlserver.ksql.RunKsqlStatementsResponse
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.routes.definitions.{CreateKsqlRequest, ErrorDescription, KsqlSuccessResponse}
import it.bitrock.kafkaflightstream.api.routes.ksql.KsqlResource.{
  createStreamResponse,
  createStreamResponseBadRequest,
  createStreamResponseCreated,
  createStreamResponseInternalServerError
}
import it.bitrock.kafkaflightstream.api.services.models.ErrorMessages._
import it.bitrock.kafkaflightstream.api.{BaseAsyncSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.Future
import scala.concurrent.duration._

class KsqlServiceSpec extends TestKit(ActorSystem("KsqlServiceSpec")) with BaseAsyncSpec with BeforeAndAfterAll {

  import KsqlServiceSpec._

  "Ksql Service" should {

    "return a streamId and an endpoint when creating a stream" in ResourceLoaner.withFixture {
      case Resource(ksqlService, websocketConfig) =>
        ksqlService.createStream(createStreamResponse)(CreateKsqlRequest(DefaultCurrentStatusRequest)) map { result =>
          result shouldBe createStreamResponseCreated(
            KsqlSuccessResponse(DefaultStreamId, websocketConfig.pathForStream(DefaultStreamId))
          )
        }
    }

    "return an error for a bad request" in ResourceLoaner.withFixture {
      case Resource(ksqlService, _) =>
        ksqlService.createStream(createStreamResponse)(CreateKsqlRequest(DefaultBadRequest)).map { result =>
          result shouldBe createStreamResponseBadRequest(
            ErrorDescription(badRequestResponse(DefaultErrorMessage))
          )
        }
    }

    "return an error for an internal server error" in ResourceLoaner.withFixture {
      case Resource(ksqlService, _) =>
        ksqlService.createStream(createStreamResponse)(CreateKsqlRequest(DefaultInternalServerError)).map { result =>
          result shouldBe createStreamResponseInternalServerError(
            ErrorDescription(internalServerErrorResponse(DefaultErrorMessage))
          )
        }
    }

    "return an error for an unhandled request" in ResourceLoaner.withFixture {
      case Resource(ksqlService, _) =>
        ksqlService.createStream(createStreamResponse)(CreateKsqlRequest(DefaultUnhandledRequest)).map { result =>
          result shouldBe createStreamResponseInternalServerError(
            ErrorDescription(unexpectedStatusCodeResponse(StatusCodes.Conflict))
          )
        }
    }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[Resource] {
    override def withFixture(body: Resource => Future[Assertion]): Future[Assertion] = {
      val websocketConfig = WebsocketConfig(
        throttleDuration = 1.second,
        cleanupDelay = 0.second,
        pathPrefix = "path",
        flightsPath = "flights",
        flightListPath = "flight-list",
        topElementsPath = "tops",
        totalElementsPath = "totals",
        ksqlPath = "ksql"
      )

      val ksqlClientFactory = new TestKsqlClientFactoryImpl
      val ksqlOps           = new KsqlOpsImpl(ksqlClientFactory)
      val ksqlService       = new KsqlService(ksqlOps, websocketConfig)

      body(Resource(ksqlService, websocketConfig))
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }

}

object KsqlServiceSpec extends TestValues {

  final case class Resource(ksqlService: KsqlService, websocketConfig: WebsocketConfig)

  class TestKsqlClientFactoryImpl extends KsqlClientFactory {

    def runKsqlStatements(
        body: RunStatements,
        headers: List[HttpHeader]
    ): Future[Either[Either[Throwable, HttpResponse], RunKsqlStatementsResponse]] =
      Future.successful {
        body.ksql match {
          case DefaultCurrentStatusRequest =>
            Right(
              RunKsqlStatementsResponse.OK(IndexedSeq(currentStatus(s"stream/$DefaultStreamId/create", CommandStatus(message = ""))))
            )
          case DefaultBadRequest =>
            Right(RunKsqlStatementsResponse.BadRequest(RunStatementsError(message = DefaultErrorMessage)))
          case DefaultInternalServerError =>
            Right(RunKsqlStatementsResponse.InternalServerError(RunStatementsError(message = DefaultErrorMessage)))
          case DefaultUnhandledRequest =>
            Left(Right(HttpResponse(StatusCodes.Conflict)))
        }
      }

  }

}
