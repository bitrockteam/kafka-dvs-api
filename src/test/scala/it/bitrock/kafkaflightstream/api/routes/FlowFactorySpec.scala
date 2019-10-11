package it.bitrock.kafkaflightstream.api.routes

import java.net.URI

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Flow
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, KsqlConfig, WebsocketConfig}
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
import scala.concurrent.Future

class FlowFactorySpec
    extends AsyncFixtureLoaner[FlowFactorySpec.Resource]
    with BaseAsyncSpec
    with ScalatestRouteTest
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  "flow" should {
    "change the box" in withFixture {
      case Resource(routes, wsProbe, websocketConfig) =>
        WS(Uri(path = Uri.Path./(websocketConfig.pathPrefix)./(websocketConfig.flightListPath)), wsProbe.flow) ~> routes.streams ~> check {
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
    }
  }

  override def withFixture(body: FlowFactorySpec.Resource => Future[Assertion]): Future[Assertion] = {

    val websocketConfig = WebsocketConfig(
      throttleDuration = 1.second,
      cleanupDelay = 0.second,
      pathPrefix = "path",
      flightsPath = "flights",
      flightListPath = "flight-list",
      topElementsPath = "tops",
      totalElementsPath = "totals",
      ksqlPath = "ksql"
    )
    val kafkaConfig = KafkaConfig(
      "",
      URI.create("http://localhost:8080"),
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      ConsumerConfig(1.second, Duration.Zero),
      KsqlConfig(java.net.URI.create("http://www.example.com"), "")
    )

    val kafkaConsumerWrapper        = mock[KafkaConsumerWrapper]
    val kafkaConsumerWrapperFactory = mock[KafkaConsumerWrapperFactory]
    when(kafkaConsumerWrapperFactory.build(any[ActorRef], any[Seq[String]])).thenReturn(kafkaConsumerWrapper)

    val flightListMessageProcessorFactory =
      new FlightListMessageProcessorFactoryImpl(websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory)

    val flowFactories = Map(
      flightFlowFactoryKey     -> new TestFlowFactory,
      flightListFlowFactoryKey -> new FlowFactoryImpl(flightListMessageProcessorFactory),
      topsFlowFactoryKey       -> new TestFlowFactory,
      totalsFlowFactoryKey     -> new TestFlowFactory,
      ksqlFlowFactoryKey       -> new TestFlowFactory
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
