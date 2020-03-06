package it.bitrock.dvs.api.core.poller

import akka.testkit.TestProbe
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{
  TopAirlineUpdate,
  TopArrivalAirportUpdate,
  TopDepartureAirportUpdate,
  TopSpeedUpdate
}
import it.bitrock.dvs.api.model._

class TopsKafkaPollerCacheSpec extends BaseTestKit {

  "Tops Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "periodically" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
      }
    }
    "return updated cached value" when {
      "a TopArrivalAirportUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topArrivalMessage =
            TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))

          topsKafkaPollerCache ! topArrivalMessage

          val testProbe = new TestProbe(system)

          topsKafkaPollerCache.tell(TopArrivalAirportUpdate, testProbe.ref)

          testProbe expectMsg topArrivalMessage
      }
      "a TopDepartureAirportUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topDepartureMessage =
            TopDepartureAirportList(List(AirportCount(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))

          topsKafkaPollerCache ! topDepartureMessage
          val testProbe = new TestProbe(system)

          topsKafkaPollerCache.tell(TopDepartureAirportUpdate, testProbe.ref)

          testProbe expectMsg topDepartureMessage
      }
      "a TopSpeedUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topSpeedMessage      = TopSpeedList(List(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))

          topsKafkaPollerCache ! topSpeedMessage
          val testProbe = new TestProbe(system)

          topsKafkaPollerCache.tell(TopSpeedUpdate, testProbe.ref)

          testProbe expectMsg topSpeedMessage
      }
      "a TopAirlineUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topAirlineMessage    = TopAirlineList(List(AirlineCount(DefaultAirline1Name, DefaultAirline1Amount)))

          topsKafkaPollerCache ! topAirlineMessage

          val testProbe = new TestProbe(system)

          topsKafkaPollerCache.tell(TopAirlineUpdate, testProbe.ref)

          testProbe expectMsg topAirlineMessage
      }
    }
  }

}
