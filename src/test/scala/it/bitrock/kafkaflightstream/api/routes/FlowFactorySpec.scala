package it.bitrock.kafkaflightstream.api.routes

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.FlightListMessageProcessorFactoryImpl
import it.bitrock.kafkaflightstream.api.definitions.JsonSupport
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.routes.FlowFactorySpec.{Resource, TestFlowFactory}
import it.bitrock.kafkaflightstream.api.{BaseAsyncSpec, TestValues}
import it.bitrock.kafkaflightstream.tags.TaggedTypes._
import it.bitrock.kafkageostream.testcommons.AsyncFixtureLoaner
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
  private val websocketConfig = WebsocketConfig(
    throttleDuration = 1.second,
    cleanupDelay = 0.second,
    pathPrefix = "path",
    flightsPath = "flights",
    flightListPath = "flight-list",
    topElementsPath = "tops",
    totalElementsPath = "totals",
    ksqlPath = "ksql"
  )
  private val kafkaConfig = mock[KafkaConfig]
  private val flightListMessageProcessorFactory =
    new FlightListMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory)
  private val flightListFlowFactory: FlowFactory = new FlowFactoryImpl(flightListMessageProcessorFactory)

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

    "change the box" in withFixture {
      case Resource(routes, wsProbe, websocketConfig) =>
        WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.flightListPath)), wsProbe.flow) ~> routes.streams ~> check {
          checkWebsocketAndSendTestMessage(wsProbe)
        }
    }
  }

  override def withFixture(body: FlowFactorySpec.Resource => Future[Assertion]): Future[Assertion] = {
    val wsProbe = WSProbe()
    val flowFactories = Map(
      flightFlowFactoryKey     -> new TestFlowFactory,
      flightListFlowFactoryKey -> flightListFlowFactory,
      topsFlowFactoryKey       -> new TestFlowFactory,
      totalsFlowFactoryKey     -> new TestFlowFactory,
      ksqlFlowFactoryKey       -> new TestFlowFactory
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

object FlowFactorySpec {

  final case class Resource(route: Routes, wsProbe: WSProbe, websocketConfig: WebsocketConfig)

  class TestFlowFactory extends FlowFactory {
    override def flow(identifier: String): Flow[Message, Message, NotUsed] = Flow[Message].map(identity)
  }
}
