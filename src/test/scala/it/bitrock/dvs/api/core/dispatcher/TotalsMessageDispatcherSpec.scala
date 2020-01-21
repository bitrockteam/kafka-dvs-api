package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.TotalsKafkaPollerCache
import it.bitrock.dvs.api.model._
import spray.json._

class TotalsMessageDispatcherSpec extends BaseTestKit {

  "Totals Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a CountFlight is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val totalsKafkaPollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory.totalsMessageDispatcherFactory(totalsKafkaPollerCache, webSocketConfig).build(sourceProbe.ref)
          val msg = TotalFlightsCount(DefaultStartTimeWindow, DefaultCountFlightAmount)
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a CountAirline is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val totalsKafkaPollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher =
            MessageDispatcherFactory.totalsMessageDispatcherFactory(totalsKafkaPollerCache, webSocketConfig).build(sourceProbe.ref)
          val msg = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
    }

  }

}
