package it.bitrock.kafkaflightstream.api.core.dispatcher

import it.bitrock.kafkaflightstream.api.BaseTestKit
import it.bitrock.kafkaflightstream.api.core.factory.TotalsMessageDispatcherFactoryImpl
import it.bitrock.kafkaflightstream.api.core.poller.TotalsKafkaPollerCache
import it.bitrock.kafkaflightstream.api.definitions._
import spray.json._

class TotalsMessageDispatcherSpec extends BaseTestKit {

  "Totals Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a CountFlight is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val totalsKafkaPollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher      = new TotalsMessageDispatcherFactoryImpl(websocketConfig, totalsKafkaPollerCache).build(sourceProbe.ref)
          val msg                    = CountFlight(DefaultStartTimeWindow, DefaultCountFlightAmount)
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
      "a CountAirline is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(websocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val totalsKafkaPollerCache = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageDispatcher      = new TotalsMessageDispatcherFactoryImpl(websocketConfig, totalsKafkaPollerCache).build(sourceProbe.ref)
          val msg                    = CountAirline(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          messageDispatcher ! msg
          val expectedResult = ApiEvent(msg.getClass.getSimpleName, msg).toJson.toString
          sourceProbe.expectMsg(expectedResult)
      }
    }

  }

}
