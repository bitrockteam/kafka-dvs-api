package it.bitrock.kafkaflightstream.api.core.poller

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.bitrock.kafkaflightstream.api.config.{ConsumerConfig, KafkaConfig, KsqlConfig}
import it.bitrock.kafkaflightstream.api.definitions.{CountAirline, CountFlight, JsonSupport}
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory
import it.bitrock.kafkaflightstream.api.{BaseSpec, TestValues}
import it.bitrock.kafkageostream.testcommons.FixtureLoanerAnyResult
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.util.Random

class TotalsKafkaPollerCacheSpec
    extends TestKit(ActorSystem("FlightListKafkaMessageProcessorSpec"))
    with BaseSpec
    with ImplicitSender
    with BeforeAndAfterAll
    with JsonSupport
    with TestValues {

  import KafkaPollerCacheSpec._

  "Totals Kafka Poller Cache" should {

    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoaner.withFixture {
        case Resource(kafkaConfig, consumerFactory, pollProbe) =>
          TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a CountFlight message is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(kafkaConfig, consumerFactory, pollProbe) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a CountAirline message is received, but only after a delay" in ResourceLoaner.withFixture {
        case Resource(kafkaConfig, consumerFactory, pollProbe) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
    }
  }

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] {
    override def withFixture(body: Resource => Any): Any = {
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
          ConsumerConfig(1.second, Duration.Zero),
          KsqlConfig(java.net.URI.create("http://www.example.com"), "")
        )
      val pollProbe                                    = TestProbe(s"poll-probe-${Random.nextInt()}")
      val consumerFactory: KafkaConsumerWrapperFactory = new TestKafkaConsumerWrapperFactory(pollProbe.ref)

      body(
        Resource(
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
