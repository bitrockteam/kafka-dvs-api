package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.dvs.api.model.KafkaPollerHub
import it.bitrock.dvs.api.routes.FlowFactorySpec.Resource
import it.bitrock.dvs.api.{BaseAsyncSpec, JsonSupport, TestValues}
import it.bitrock.testcommons.AsyncFixtureLoaner
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatestplus.mockito.MockitoSugar._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

abstract class FlowFactorySpec
    extends AsyncFixtureLoaner[FlowFactorySpec.Resource]
    with BaseAsyncSpec
    with ScalatestRouteTest
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  implicit private val ec: ExecutionContextExecutor = system.dispatcher

  private val kafkaConsumerWrapper        = mock[KafkaConsumerWrapper]
  private val kafkaConsumerWrapperFactory = mock[KafkaConsumerWrapperFactory]

  when(kafkaConsumerWrapperFactory.build(any[ActorRef], any[Seq[String]])).thenReturn(kafkaConsumerWrapper)

  private val checkWebsocketAndSendTestMessage = { wsProbe: WSProbe =>
    isWebSocketUpgrade shouldBe true

    val msg = TextMessage.Strict("""
                                   |{
                                   |    "leftHighLat": 1,
                                   |    "leftHighLon": 1,
                                   |    "rightLowLat": 2,
                                   |    "rightLowLon": 2
                                   |}
              """.stripMargin)
    wsProbe.sendMessage(msg)
    1 shouldBe 1
  }

  "flow" should {
    "change the box" ignore withFixture {
      case Resource(webSocketRoutes, wsProbe, config) =>
        WS(Uri(path = Uri.Path./(config.pathPrefix)./(config.dvsPath)), wsProbe.flow) ~> webSocketRoutes ~> check {
          checkWebsocketAndSendTestMessage(wsProbe)
        }
    }
  }

  override def withFixture(body: FlowFactorySpec.Resource => Future[Assertion]): Future[Assertion] = {

    val webSocketConfig = WebSocketConfig(
      maxNumberFlights = 1000,
      throttleDuration = 1.second,
      pathPrefix = "path",
      dvsPath = "dvs"
    )

    val kafkaConsumerWrapper        = mock[KafkaConsumerWrapper]
    val kafkaConsumerWrapperFactory = mock[KafkaConsumerWrapperFactory]
    when(kafkaConsumerWrapperFactory.build(any[ActorRef], any[Seq[String]])).thenReturn(kafkaConsumerWrapper)

    val kafkaPollerHub = KafkaPollerHub(mock[ActorRef], mock[ActorRef], mock[ActorRef])

    val globalMessageDispatcherFactory =
      MessageDispatcherFactory.globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)

    val flowFactory = FlowFactory.messageExchangeFlowFactory(globalMessageDispatcherFactory)

    body(
      Resource(
        Routes.webSocketRoutes(webSocketConfig, flowFactory),
        WSProbe(),
        webSocketConfig
      )
    )
  }
}

object FlowFactorySpec {

  final case class Resource(webSocketRoutes: Route, wsProbe: WSProbe, webSocketConfig: WebSocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow: Flow[Message, Message, NotUsed] = Flow[Message].map(identity)
  }
}
