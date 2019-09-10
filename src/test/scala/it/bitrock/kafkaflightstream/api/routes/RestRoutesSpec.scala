package it.bitrock.kafkaflightstream.api.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.EitherT
import it.bitrock.kafkaflightstream.api.BaseAsyncSpec
import it.bitrock.kafkaflightstream.api.routes.internals.HealthResponse.OK
import it.bitrock.kafkaflightstream.api.routes.internals.{InternalsClient, InternalsResource}
import it.bitrock.kafkaflightstream.api.services.InternalsService
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
import org.scalatest.{Assertion, EitherValues}

import scala.concurrent.Future

class RestRoutesSpec extends BaseAsyncSpec with ScalatestRouteTest with EitherValues {

  import RestRoutesSpec._

  private def unwrapValue[T](v: EitherT[Future, Either[Throwable, HttpResponse], T]): Future[T] =
    v.value.map(_.right.value)

  "RestRoutes" should {

    "return 200 OK for GET requests to the health path" in ResourceLoaner.withFixture {
      case Resource(internalsClient) =>
        val result = internalsClient.health()

        unwrapValue(result).map(_ shouldBe OK)
    }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[Resource] {
    override def withFixture(body: Resource => Future[Assertion]): Future[Assertion] = {
      val internalsRoutes: Route                                   = InternalsResource.routes(new InternalsService)
      val internalsHttpClient: HttpRequest => Future[HttpResponse] = Route.asyncHandler(internalsRoutes)
      val internalsClient: InternalsClient                         = InternalsClient.httpClient(internalsHttpClient)

      body(Resource(internalsClient))
    }
  }

}

object RestRoutesSpec {

  final case class Resource(internalsClient: InternalsClient)

}
