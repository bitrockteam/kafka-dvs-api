package it.bitrock.dvs.api.core.poller

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.model._
import it.bitrock.dvs.api.BaseTestKit._

class FlightListKafkaPollerCacheSpec extends BaseTestKit {

  "Flight List Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "a FlightReceivedList message is received, but only after a delay" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
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

}
