package it.bitrock.kafkaflightstream.api.routes

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.kafkaflightstream.api.BaseAsyncSpec
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.tags.TaggedTypes._
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.concurrent.duration._

class RoutesSpec extends BaseAsyncSpec with ScalatestRouteTest {

  import RoutesSpec._

  private val checkWebsocketAndSendTestMessage = { wsProbe: WSProbe =>
    isWebSocketUpgrade shouldBe true

    val msg = TextMessage("web-socket is working")
    wsProbe.sendMessage(msg)
    val receivedMsg = wsProbe.expectMessage()
    wsProbe.sendCompletion()
    wsProbe.expectCompletion()
    receivedMsg shouldBe msg
  }

  "Routes" should {

    "open a web-socket channel and stream messages with events on it for WS requests on the streams path" in ResourceLoaner.withFixture {
      case Resource(routes, wsProbe, websocketConfig) =>
        WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.flightsPath)), wsProbe.flow) ~> routes.streams ~> check {
          checkWebsocketAndSendTestMessage(wsProbe)
        }
    }

    "open a web-socket channel and stream messages with top countries and topics on it for WS requests on the streams path" in ResourceLoaner
      .withFixture {
        case Resource(routes, wsProbe, websocketConfig) =>
          WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.topElementsPath)), wsProbe.flow) ~> routes.streams ~> check {
            checkWebsocketAndSendTestMessage(wsProbe)
          }
      }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[RoutesSpec.Resource] {
    override def withFixture(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] = {
      val wsProbe = WSProbe()
      val flowFactories = Map(
        flightFlowFactoryKey -> new TestFlowFactory,
        topsFlowFactoryKey   -> new TestFlowFactory
      )
      val websocketConfig = WebsocketConfig(
        throttleDuration = 1.second,
        cleanupDelay = 0.second,
        pathPrefix = "path",
        flightsPath = "flights",
        topElementsPath = "tops"
      )
      val routes = new Routes(flowFactories, websocketConfig)

      body(
        Resource(
          routes,
          wsProbe,
          websocketConfig
        )
      )
    }
  }

}

object RoutesSpec {

  final case class Resource(route: Routes, wsProbe: WSProbe, websocketConfig: WebsocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow: Flow[Message, Message, NotUsed] = Flow[Message].map(identity)
  }

}
