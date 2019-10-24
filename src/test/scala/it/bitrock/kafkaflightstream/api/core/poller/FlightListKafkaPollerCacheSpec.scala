package it.bitrock.kafkaflightstream.api.core.poller

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig}
import it.bitrock.kafkaflightstream.api.core.CoreResources.{PollingTriggered, ResourceKafkaPollerCache, TestKafkaConsumerWrapperFactory}
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.util.Random

class FlightListKafkaPollerCacheSpec
    extends TestKit(ActorSystem("FlightListKafkaPollerCacheSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  "Flight List Kafka Poller Cache" should {

    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a FlightReceivedList message is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          val messageProcessor = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val flightListMessage = FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                GeographyInfo(DefaultInBoxLatitude, DefaultInBoxLongitude, DefaultAltitude, DefaultDirection),
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
            )
          )
          pollProbe expectMsg PollingTriggered
          messageProcessor ! flightListMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
    }
  }

  object ResourceLoaner extends FixtureLoanerAnyResult[ResourceKafkaPollerCache] {
    override def withFixture(body: ResourceKafkaPollerCache => Any): Any = {
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
      val consumerFactory: KafkaConsumerWrapperFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)

      body(
        ResourceKafkaPollerCache(
          kafkaConfig,
          consumerFactory,
          pollProbe
        )
      )
    }
  }

  override def afterAll: Unit = {
    shutdown()
    super.afterAll()
  }
}
