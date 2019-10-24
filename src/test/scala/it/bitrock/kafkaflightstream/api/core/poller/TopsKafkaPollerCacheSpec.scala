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

class TopsKafkaPollerCacheSpec
    extends TestKit(ActorSystem("TopsKafkaPollerCacheSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  "Tops Kafka Poller Cache" should {

    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a TopArrivalAirportList is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topArrivalMessage    = TopArrivalAirportList(Seq(Airport(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topArrivalMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopDepartureAirportList is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topDepartureMessage  = TopDepartureAirportList(Seq(Airport(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topDepartureMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopSpeedList is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topSpeedMessage      = TopSpeedList(Seq(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topSpeedMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopAirlineList is received, but only after a delay" in ResourceLoaner.withFixture {
        case ResourceKafkaPollerCache(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topAirlineMessage    = TopAirlineList(Seq(Airline(DefaultAirline1Name, DefaultAirline1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topAirlineMessage
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
