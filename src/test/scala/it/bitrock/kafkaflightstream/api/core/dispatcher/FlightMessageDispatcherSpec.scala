package it.bitrock.kafkaflightstream.api.core.dispatcher

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.CoreResources.{PollingTriggered, ResourceMessageDispatcher, TestKafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.core.FlightMessageDispatcherFactoryImpl
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll
import spray.json._

import scala.concurrent.duration._
import scala.util.Random

class FlightMessageDispatcherSpec
    extends TestKit(ActorSystem("FlightMessageProcessorSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  "Flight Message Dispatcher" should {
    "trigger Kafka Consumer polling" when {

      "it starts" in ResourceLoaner.withFixture {
        case ResourceMessageDispatcher(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          new FlightMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)

          pollProbe.expectMsg(PollingTriggered)
      }

      "a FlightReceived message is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceMessageDispatcher(websocketConfig, kafkaConfig, consumerFactory, pollProbe, sourceProbe) =>
          val messageDispatcher =
            new FlightMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)

          // First message is sent when processor starts up
          pollProbe.expectMsg(PollingTriggered)

          messageDispatcher ! FlightReceived(
            DefaultIataNumber,
            DefaultIcaoNumber,
            GeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
            DefaultSpeed,
            AirportInfo(
              DefaultCodeAirport1,
              DefaultNameAirport1,
              DefaultNameCountry1,
              DefaultCodeIso2Country1,
              DefaultTimezone1,
              DefaultGmt1
            ),
            AirportInfo(
              DefaultCodeAirport2,
              DefaultNameAirport2,
              DefaultNameCountry2,
              DefaultCodeIso2Country2,
              DefaultTimezone2,
              DefaultGmt2
            ),
            AirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
            AirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
            DefaultStatus,
            DefaultUpdated
          )

          pollProbe.expectNoMessage(websocketConfig.throttleDuration)
          pollProbe.expectMsg(PollingTriggered)
      }

    }

    "forward a JSON to source actor" when {

      "a FlightReceived message is received" in ResourceLoaner.withFixture {
        case ResourceMessageDispatcher(websocketConfig, kafkaConfig, consumerFactory, _, sourceProbe) =>
          val messageDispatcher =
            new FlightMessageDispatcherFactoryImpl(websocketConfig, kafkaConfig, consumerFactory).build(sourceProbe.ref)
          val msg = FlightReceived(
            DefaultIataNumber,
            DefaultIcaoNumber,
            GeographyInfo(DefaultLatitude, DefaultLongitude, DefaultAltitude, DefaultDirection),
            DefaultSpeed,
            AirportInfo(
              DefaultCodeAirport1,
              DefaultNameAirport1,
              DefaultNameCountry1,
              DefaultCodeIso2Country1,
              DefaultTimezone1,
              DefaultGmt1
            ),
            AirportInfo(
              DefaultCodeAirport2,
              DefaultNameAirport2,
              DefaultNameCountry2,
              DefaultCodeIso2Country2,
              DefaultTimezone2,
              DefaultGmt2
            ),
            AirlineInfo(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
            AirplaneInfo(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
            DefaultStatus,
            DefaultUpdated
          )

          messageDispatcher ! msg

          val expectedResult = msg.toJson.toString

          sourceProbe.expectMsg(expectedResult)
      }

    }

  }

  object ResourceLoaner extends FixtureLoanerAnyResult[ResourceMessageDispatcher] {
    override def withFixture(body: ResourceMessageDispatcher => Any): Any = {
      val websocketConfig = WebsocketConfig(1.second, 0.second, "not-used", "not-used", "not-used", "not-used", "not-used")
      val kafkaConfig =
        KafkaConfig(
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
          ConsumerConfig(1.second, Duration.Zero)
        )
      val pollProbe                                    = TestProbe(s"poll-probe-${Random.nextInt()}")
      val sourceProbe                                  = TestProbe(s"source-probe-${Random.nextInt()}")
      val consumerFactory: KafkaConsumerWrapperFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)

      body(
        ResourceMessageDispatcher(
          websocketConfig,
          kafkaConfig,
          consumerFactory,
          pollProbe,
          sourceProbe
        )
      )
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }

}
