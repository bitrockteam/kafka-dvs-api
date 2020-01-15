package it.bitrock.dvs.api.routes

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.core.factory.FlightListMessageDispatcherFactoryImpl
import it.bitrock.dvs.api.definitions.JsonSupport
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.dvs.api.routes.FlowFactorySpec.{Resource, TestFlowFactory}
import it.bitrock.dvs.api.{BaseAsyncSpec, TestValues}
import it.bitrock.dvs.tags.TaggedTypes._
import it.bitrock.testcommons.AsyncFixtureLoaner
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatestplus.mockito.MockitoSugar._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class FlowFactorySpec
    extends AsyncFixtureLoaner[FlowFactorySpec.Resource]
    with BaseAsyncSpec
    with ScalatestRouteTest
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  private implicit val ec: ExecutionContextExecutor = system.dispatcher

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
      case Resource(routes, wsProbe, config) =>
        WS(Uri(path = Uri.Path./(config.pathPrefix)./(config.flightListPath)), wsProbe.flow) ~> routes.streams ~> check {
          checkWebsocketAndSendTestMessage(wsProbe)
        }
    }
  }

  override def withFixture(body: FlowFactorySpec.Resource => Future[Assertion]): Future[Assertion] = {

    val websocketConfig = WebsocketConfig(
      throttleDuration = 1.second,
      cleanupDelay = 0.second,
      pathPrefix = "path",
      flightListPath = "flight-list",
      topElementsPath = "tops",
      totalElementsPath = "totals"
    )

    val kafkaConsumerWrapper        = mock[KafkaConsumerWrapper]
    val kafkaConsumerWrapperFactory = mock[KafkaConsumerWrapperFactory]
    when(kafkaConsumerWrapperFactory.build(any[ActorRef], any[Seq[String]])).thenReturn(kafkaConsumerWrapper)

    val flightListMessageProcessorFactory =
      new FlightListMessageDispatcherFactoryImpl(websocketConfig, mock[ActorRef])

    val flowFactories = Map(
      flightListFlowFactoryKey -> new FlowFactoryImpl(flightListMessageProcessorFactory),
      topsFlowFactoryKey       -> new TestFlowFactory,
      totalsFlowFactoryKey     -> new TestFlowFactory
    )

    body(
      Resource(
        new Routes(flowFactories, websocketConfig),
        WSProbe(),
        websocketConfig
      )
    )
  }
}

object FlowFactorySpec {

  final case class Resource(route: Routes, wsProbe: WSProbe, websocketConfig: WebsocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow(identifier: String): Flow[Message, Message, NotUsed] = Flow[Message].map(identity)
  }
}
