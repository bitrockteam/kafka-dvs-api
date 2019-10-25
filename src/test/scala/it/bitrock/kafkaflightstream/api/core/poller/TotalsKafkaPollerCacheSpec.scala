package it.bitrock.kafkaflightstream.api.core.poller

import it.bitrock.kafkaflightstream.api.BaseTestKit
import it.bitrock.kafkaflightstream.api.definitions.{CountAirline, CountFlight}

class TotalsKafkaPollerCacheSpec extends BaseTestKit {

  "Totals Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a CountFlight message is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
      "a CountAirline message is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectNoMessage kafkaConfig.consumer.pollInterval
          pollProbe expectMsg PollingTriggered
      }
    }
  }

}
