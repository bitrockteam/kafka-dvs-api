package it.bitrock.kafkaflightstream.api.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpOrigin, Origin, `Access-Control-Request-Method`}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.EitherT
import it.bitrock.kafkaflightstream.api.routes.definitions._
import it.bitrock.kafkaflightstream.api.routes.internals._
import it.bitrock.kafkaflightstream.api.routes.ksql.KsqlResource.createStreamResponse.{
  BadRequest,
  Created,
  InternalServerError => KInternalServerError
}
import it.bitrock.kafkaflightstream.api.routes.ksql._
import it.bitrock.kafkaflightstream.api.routes.session.SessionResource.cleanSessionResponse.{
  NoContent,
  InternalServerError => SInternalServerError
}
import it.bitrock.kafkaflightstream.api.routes.session._
import it.bitrock.kafkaflightstream.api.services.InternalsService
import it.bitrock.kafkaflightstream.api.{BaseAsyncSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
import org.scalatest.{Assertion, EitherValues}

import scala.concurrent.Future

class RestRoutesSpec extends BaseAsyncSpec with ScalatestRouteTest with EitherValues {

  import RestRoutesSpec._

  private def unwrapValue[T](v: EitherT[Future, Either[Throwable, HttpResponse], T]): Future[T] =
    v.value.map(_.right.value)

  "RestRoutes" should {

    "return 200 OK for GET requests to the health path" in ResourceLoaner.withFixture {
      case Resource(internalsClient, _, _) =>
        val result = internalsClient.health()

        unwrapValue(result).map(_ shouldBe HealthResponse.OK)
    }

    "return a streamId and a url for a correct request to the ksql path" in ResourceLoaner.withFixture {
      case Resource(_, ksqlClient, _) =>
        val result = ksqlClient.createStream(CreateKsqlRequest(DefaultCurrentStatusRequest))

        unwrapValue(result).map(_ shouldBe CreateStreamResponse.Created(KsqlSuccessResponse(DefaultStreamId, DefaultEndpoint)))
    }

    "return an error message for an bad request to the ksql path" in ResourceLoaner.withFixture {
      case Resource(_, ksqlClient, _) =>
        val result = ksqlClient.createStream(CreateKsqlRequest(DefaultBadRequest))

        unwrapValue(result).map(_ shouldBe CreateStreamResponse.BadRequest(ErrorDescription(DefaultErrorMessage)))
    }

    "return an error message if an internal server error occur to the ksql path" in ResourceLoaner.withFixture {
      case Resource(_, ksqlClient, _) =>
        val result = ksqlClient.createStream(CreateKsqlRequest(DefaultInternalServerError))

        unwrapValue(result).map(_ shouldBe CreateStreamResponse.InternalServerError(ErrorDescription(DefaultErrorMessage)))
    }

    "return 200 OK response for pre-flight OPTIONS requests to the ksql path" in ResourceLoaner.withFixture {
      case Resource(_, ksqlClient, _) =>
        val headers = List(
          `Access-Control-Request-Method`(HttpMethods.OPTIONS),
          Origin(HttpOrigin("http://example.com"))
        )

        val routes = new RestRoutes(new InternalsService, new TestKsqlService, new TestSessionService)

        Options(Uri(path = Uri.Path(ksqlClient.basePath)./("ksql")))
          .withHeaders(headers) ~> routes.routes ~> check {
          status shouldBe StatusCodes.OK
        }
    }

    "return NoContent for a correct request to the the session path" in ResourceLoaner.withFixture {
      case Resource(_, _, sessionClient) =>
        val result = sessionClient.cleanSession(CleanSessionRequest(IndexedSeq(DefaultStreamId)))

        unwrapValue(result).map(_ shouldBe CleanSessionResponse.NoContent)
    }

    "return an error message for an incorrect request to the session path" in ResourceLoaner.withFixture {
      case Resource(_, _, sessionClient) =>
        val result = sessionClient.cleanSession(CleanSessionRequest(IndexedSeq(DefaultNotFoundedStreamId)))

        unwrapValue(result).map(_ shouldBe CleanSessionResponse.InternalServerError(ErrorDescription(DefaultErrorMessage)))
    }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[Resource] {
    override def withFixture(body: Resource => Future[Assertion]): Future[Assertion] = {
      val internalsRoutes: Route                                   = InternalsResource.routes(new InternalsService)
      val internalsHttpClient: HttpRequest => Future[HttpResponse] = Route.asyncHandler(internalsRoutes)
      val internalsClient: InternalsClient                         = InternalsClient.httpClient(internalsHttpClient)

      val ksqlRoutes: Route                                   = KsqlResource.routes(new TestKsqlService)
      val ksqlHttpClient: HttpRequest => Future[HttpResponse] = Route.asyncHandler(ksqlRoutes)
      val ksqlClient: KsqlClient                              = KsqlClient.httpClient(ksqlHttpClient)

      val sessionRoutes: Route                                   = SessionResource.routes(new TestSessionService)
      val sessionHttpClient: HttpRequest => Future[HttpResponse] = Route.asyncHandler(sessionRoutes)
      val sessionClient: SessionClient                           = SessionClient.httpClient(sessionHttpClient)

      body(Resource(internalsClient, ksqlClient, sessionClient))
    }
  }

}

object RestRoutesSpec extends TestValues {

  final case class Resource(internalsClient: InternalsClient, ksqlClient: KsqlClient, sessionClient: SessionClient)

  class TestKsqlService extends KsqlHandler {
    override def createStream(
        respond: KsqlResource.createStreamResponse.type
    )(body: CreateKsqlRequest): Future[KsqlResource.createStreamResponse] =
      Future.successful {
        body.ksql match {
          case DefaultCurrentStatusRequest => Created(KsqlSuccessResponse(DefaultStreamId, DefaultEndpoint))
          case DefaultBadRequest           => BadRequest(ErrorDescription(DefaultErrorMessage))
          case DefaultInternalServerError  => KInternalServerError(ErrorDescription(DefaultErrorMessage))
        }
      }
  }

  class TestSessionService extends SessionHandler {
    override def cleanSession(
        respond: SessionResource.cleanSessionResponse.type
    )(body: CleanSessionRequest): Future[SessionResource.cleanSessionResponse] = {
      Future.successful {
        body.streamIds match {
          case IndexedSeq(DefaultNotFoundedStreamId) => SInternalServerError(ErrorDescription(DefaultErrorMessage))
          case _                                     => NoContent
        }
      }
    }
  }

}
