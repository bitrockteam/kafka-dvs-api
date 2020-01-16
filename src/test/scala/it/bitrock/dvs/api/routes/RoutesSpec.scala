package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.BaseAsyncSpec
import it.bitrock.dvs.api.Tags.TaggedTypes._
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.testcommons.AsyncFixtureLoaner
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

    "open a web-socket channel and stream messages with flight list events on it for WS requests on the streams path" in ResourceLoaner
      .withFixture {
        case Resource(routes, wsProbe, websocketConfig) =>
          WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.flightListPath)), wsProbe.flow) ~> routes.streams ~> check {
            checkWebsocketAndSendTestMessage(wsProbe)
          }
      }

    "open a web-socket channel and stream messages with top events on it for WS requests on the streams path" in ResourceLoaner
      .withFixture {
        case Resource(routes, wsProbe, websocketConfig) =>
          WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.topElementsPath)), wsProbe.flow) ~> routes.streams ~> check {
            checkWebsocketAndSendTestMessage(wsProbe)
          }
      }

    "open a web-socket channel and stream messages with total events on it for WS requests on the streams path" in ResourceLoaner
      .withFixture {
        case Resource(routes, wsProbe, websocketConfig) =>
          WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.totalElementsPath)), wsProbe.flow) ~> routes.streams ~> check {
            checkWebsocketAndSendTestMessage(wsProbe)
          }
      }

  }

  object ResourceLoaner extends AsyncFixtureLoaner[RoutesSpec.Resource] {
    override def withFixture(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] = {
      val wsProbe = WSProbe()
      val flowFactories = Map(
        flightListFlowFactoryKey -> new TestFlowFactory,
        topsFlowFactoryKey       -> new TestFlowFactory,
        totalsFlowFactoryKey     -> new TestFlowFactory
      )
      val websocketConfig = WebsocketConfig(
        maxNumberFlights = 1000,
        throttleDuration = 1.second,
        pathPrefix = "path",
        flightListPath = "flight-list",
        topElementsPath = "tops",
        totalElementsPath = "totals"
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
