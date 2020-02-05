package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.BaseAsyncSpec
import it.bitrock.dvs.api.JsonSupport._
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.model.{FlightReceivedList, TopDepartureAirportList, TotalAirlinesCount}
import it.bitrock.testcommons.AsyncFixtureLoaner
import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.Assertion
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class RoutesSpec extends BaseAsyncSpec with ScalatestRouteTest {

  import RoutesSpec._

  implicit private val ec: ExecutionContextExecutor = system.dispatcher

  "Routes" should {

    "open a web-socket channel and stream messages with flight list events on it for WS requests on the streams path" in ResourceLoaner.withFixture {
      case Resource(routes, wsProbe, webSocketConfig) =>
        WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
          webSocketExchange(wsProbe, "web-socket is working", "web-socket is working")
        }
    }

    "send a message" when {

      val flightExpectedMessage = implicitly[Arbitrary[FlightReceivedList]].arbitrary.sample.get
      "a flight is requested" in ResourceLoaner.withFlowFactory(new FlowFactoryFixture(flightExpectedMessage).flowFactory) {
        case Resource(routes, wsProbe, webSocketConfig) =>
          WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
            val msg =
              """
                | {
                |   "@type": "startFlightList",
                |   "leftHighLat": 1.1297249669842589E-301,
                |   "leftHighLon": 1.9284761079986233E-133,
                |   "rightLowLat": 1.427838496966401E+60,
                |   "rightLowLon": -4.236777500327674E-217
                | }
                |""".stripMargin
            webSocketExchange(wsProbe, msg, flightExpectedMessage.toJson.toString)
          }
      }

      val topExpectedMsg = implicitly[Arbitrary[TopDepartureAirportList]].arbitrary.sample.get
      "a top is requested" in ResourceLoaner.withFlowFactory(new FlowFactoryFixture(topExpectedMsg).flowFactory) {
        case Resource(routes, wsProbe, webSocketConfig) =>
          WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
            val msg = """
                        | {
                        |   "@type": "startTop"
                        | }
                        |""".stripMargin
            webSocketExchange(wsProbe, msg, topExpectedMsg.toJson.toString)
          }
      }

      val totalExpectedMsg = implicitly[Arbitrary[TotalAirlinesCount]].arbitrary.sample.get
      "a total is requested" in ResourceLoaner.withFlowFactory(new FlowFactoryFixture(totalExpectedMsg).flowFactory) {
        case Resource(routes, wsProbe, webSocketConfig) =>
          WS(Uri(path = Uri.Path / webSocketConfig.pathPrefix / webSocketConfig.dvsPath), wsProbe.flow) ~> routes ~> check {
            val msg = """
                        | {
                        |   "@type": "startTotal"
                        | }
                        |""".stripMargin
            webSocketExchange(wsProbe, msg, totalExpectedMsg.toJson.toString)
          }
      }
    }
  }

  object ResourceLoaner extends AsyncFixtureLoaner[RoutesSpec.Resource] {
    override def withFixture(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] =
      withFlowFactory(new TestFlowFactory)(body)

    def withFlowFactory(flowFactory: FlowFactory)(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] = {
      val wsProbe = WSProbe()
      val webSocketConfig = WebSocketConfig(
        maxNumberFlights = 1000,
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
