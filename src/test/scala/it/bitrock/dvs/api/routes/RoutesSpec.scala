package it.bitrock.dvs.api.routes

import RoutesSpec._
import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.BaseAsyncSpec
import it.bitrock.dvs.api.JsonSupport._
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.config.{RestConfig, WebSocketConfig}
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.model.{FlightReceivedList, TopDepartureAirportList, TotalAirlinesCount}
import it.bitrock.testcommons.AsyncFixtureLoaner
import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class RoutesSpec extends BaseAsyncSpec with ScalatestRouteTest with ScalaCheckPropertyChecks {

  "Routes" should {

    "open a web-socket channel and stream messages on it for WS requests" in ResourceLoaner.withFixture {
      case Resource(routes, wsProbe, webSocketConfig) =>
        WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
          webSocketExchange(wsProbe, "web-socket is working", "web-socket is working")
        }
    }

    "send a message" when {

      "a flight is requested" in ResourceLoaner.respondTo[FlightReceivedList](coordinatesBox)

      "a top is requested" in ResourceLoaner.respondTo[TopDepartureAirportList](startTop)

      "a total is requested" in ResourceLoaner.respondTo[TotalAirlinesCount](startTotal)
    }

    "respond to the health check" in {
      val restConfig = RestConfig(healthPath = "health")
      val routes     = Routes.healthRoutes(restConfig)
      Get(s"/${restConfig.healthPath}") ~> Route.seal(routes) ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }

  object ResourceLoaner extends AsyncFixtureLoaner[RoutesSpec.Resource] {
    override def withFixture(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] =
      withFlowFactory(new TestFlowFactory)(body)

    def respondTo[A: Arbitrary: JsonWriter](command: String): Future[Assertion] =
      implicitly[Arbitrary[A]].arbitrary.sample.map { expectedMsg =>
        withFlowFactory(new FlowFactoryFixture(expectedMsg).flowFactory) {
          case Resource(routes, wsProbe, webSocketConfig) =>
            WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
              webSocketExchange(wsProbe, command, expectedMsg.toJson.toString)
            }
        }
      }.getOrElse(fail())

    private def withFlowFactory(flowFactory: FlowFactory)(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] = {
      val wsProbe = WSProbe()
      val webSocketConfig = WebSocketConfig(
        maxNumberFlights = 1000,
        maxNumberAirports = 150,
        throttleDuration = 1.second,
        pathPrefix = "path",
        dvsPath = "dvs"
      )
      val routes = Routes.webSocketRoutes(webSocketConfig, flowFactory)

      body(
        Resource(
          routes,
          wsProbe,
          webSocketConfig
        )
      )
    }
  }

  private def webSocketExchange[A](wsProbe: WSProbe, message: String, expectedMsg: String): Assertion = {
    isWebSocketUpgrade shouldBe true
    wsProbe.sendMessage(TextMessage(message))
    val receivedMsg = wsProbe.expectMessage()
    wsProbe.sendCompletion()
    wsProbe.expectCompletion()
    receivedMsg shouldBe TextMessage(expectedMsg)
  }

}

object RoutesSpec {

  final case class Resource(route: Route, wsProbe: WSProbe, webSocketConfig: WebSocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow: Flow[Message, Message, NotUsed] = Flow[Message]
  }

  final class FlowFactoryFixture[A: JsonWriter](expectedMsg: A)(implicit ec: ExecutionContextExecutor, system: ActorSystem) {

    def flowFactory: FlowFactory = FlowFactory.messageExchangeFlowFactory(processor)

    private def processor: MessageDispatcherFactory =
      (sourceActorRef: ActorRef) =>
        system.actorOf(Props(new Actor {
          override def receive: Receive = {
            case _ => sourceActorRef ! expectedMsg.toJson.toString
          }
        }))
  }

}
