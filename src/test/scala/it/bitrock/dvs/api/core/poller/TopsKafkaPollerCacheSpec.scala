package it.bitrock.dvs.api.core.poller

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.model._

class TopsKafkaPollerCacheSpec extends BaseTestKit {

  "Tops Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a TopArrivalAirportList is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topArrivalMessage    = TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topArrivalMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopDepartureAirportList is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topDepartureMessage =
            TopDepartureAirportList(List(AirportCount(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topDepartureMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopSpeedList is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topSpeedMessage      = TopSpeedList(List(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topSpeedMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a TopAirlineList is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val topsKafkaPollerCache = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topAirlineMessage    = TopAirlineList(List(AirlineCount(DefaultAirline1Name, DefaultAirline1Amount)))

          pollProbe expectMsg PollingTriggered
          topsKafkaPollerCache ! topAirlineMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
    }
  }

}
