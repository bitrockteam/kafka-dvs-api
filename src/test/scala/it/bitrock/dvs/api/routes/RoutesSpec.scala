package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.BaseAsyncSpec
import it.bitrock.dvs.api.config.WebSocketConfig
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

    "open a web-socket channel and stream messages with flight list events on it for WS requests on the streams path" in ResourceLoaner.withFixture {
      case Resource(routes, wsProbe, webSocketConfig) =>
        WS(Uri(path = Uri.Path./(webSocketConfig.pathPrefix)./(webSocketConfig.dvsPath)), wsProbe.flow) ~> routes ~> check {
          checkWebsocketAndSendTestMessage(wsProbe)
        }
    }
  }

  object ResourceLoaner extends AsyncFixtureLoaner[RoutesSpec.Resource] {
    override def withFixture(body: RoutesSpec.Resource => Future[Assertion]): Future[Assertion] = {
      val wsProbe     = WSProbe()
      val flowFactory = new TestFlowFactory
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

}

object RoutesSpec {

  final case class Resource(route: Route, wsProbe: WSProbe, webSocketConfig: WebSocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow: Flow[Message, Message, NotUsed] = Flow[Message].map(identity)
  }

}
