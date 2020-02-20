package it.bitrock.dvs.api.core.poller

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.model.{TotalAirlinesCount, TotalFlightsCount}

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
          val countFlightMessage = TotalFlightsCount(DefaultStartTimeWindow, DefaultCountFlightAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectMsg PollingTriggered
      }
      "a CountAirline message is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          pollProbe expectMsg PollingTriggered
          messagePollerCache ! countFlightMessage
          pollProbe expectMsg PollingTriggered
      }
    }
  }

}
