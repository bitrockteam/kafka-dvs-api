package it.bitrock.dvs.api.core.poller

import akka.testkit.TestProbe
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{TotalAirlineUpdate, TotalFlightUpdate}
import it.bitrock.dvs.api.model.{TotalAirlinesCount, TotalFlightsCount}

class TotalsKafkaPollerCacheSpec extends BaseTestKit {

  "Totals Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "periodically" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
      }
    }
    "return updated cached value" when {
      "a TotalFlightUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val messagePollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countFlightMessage = TotalFlightsCount(DefaultStartTimeWindow, DefaultCountFlightAmount)

          messagePollerCache ! countFlightMessage

          val testProbe = new TestProbe(system)

          messagePollerCache.tell(TotalFlightUpdate, testProbe.ref)

          testProbe expectMsg countFlightMessage
      }
      "a TotalAirlineUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val messagePollerCache   = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val countAirlinesMessage = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)

          messagePollerCache ! countAirlinesMessage

          val testProbe = new TestProbe(system)

          messagePollerCache.tell(TotalAirlineUpdate, testProbe.ref)

          testProbe expectMsg countAirlinesMessage
      }
    }
  }

}
