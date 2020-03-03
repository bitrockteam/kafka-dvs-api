package it.bitrock.dvs.api.core.poller

import java.time.temporal.ChronoUnit

import akka.testkit.TestProbe
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{AirportListUpdate, FlightListUpdate}
import it.bitrock.dvs.api.model._

class FlightListKafkaPollerCacheSpec extends BaseTestKit {
  "Flight List Kafka Poller Cache" should {
    "trigger Kafka Consumer polling" when {
      "it starts" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe expectMsg PollingTriggered
      }
      "periodically" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, pollProbe) =>
          FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
          pollProbe.expectMsg(kafkaConfig.consumer.pollInterval * 2, PollingTriggered)
      }
    }
    "send flight list updated" when {
      "a FlightListUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val messageProcessor = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val flightReceived = FlightReceived(
            DefaultIataNumber,
            DefaultIcaoNumber,
            Geography(DefaultInBoxLatitude, DefaultInBoxLongitude, DefaultAltitude, DefaultDirection),
            DefaultSpeed,
            Airport(
              DefaultCodeAirport1,
              DefaultNameAirport1,
              DefaultNameCountry1,
              DefaultCodeIso2Country1,
              DefaultTimezone1,
              DefaultLatitude1,
              DefaultLongitude1,
              DefaultGmt1
            ),
            Airport(
              DefaultCodeAirport2,
              DefaultNameAirport2,
              DefaultNameCountry2,
              DefaultCodeIso2Country2,
              DefaultTimezone2,
              DefaultLatitude2,
              DefaultLongitude2,
              DefaultGmt2
            ),
            Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
            Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
            DefaultStatus,
            DefaultUpdated.toEpochMilli
          )
          val flightListMessage = FlightReceivedList(
            List(
              flightReceived,
              flightReceived.copy(updated = DefaultUpdated.minus(5, ChronoUnit.MINUTES).toEpochMilli),
              flightReceived.copy(updated = DefaultUpdated.plus(3, ChronoUnit.HOURS).toEpochMilli)
            )
          )

          messageProcessor ! flightListMessage

          val testProbe = new TestProbe(system)

          messageProcessor.tell(FlightListUpdate, testProbe.ref)

          testProbe expectMsg flightListMessage
      }
    }

    "send airport list updated" when {
      "a AirportListUpdate message is received" in ResourceLoanerPoller.withFixture {
        case ResourcePoller(kafkaConfig, consumerFactory, _) =>
          val messageProcessor = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val flightReceived = FlightReceived(
            DefaultIataNumber,
            DefaultIcaoNumber,
            Geography(DefaultInBoxLatitude, DefaultInBoxLongitude, DefaultAltitude, DefaultDirection),
            DefaultSpeed,
            Airport(
              DefaultCodeAirport1,
              DefaultNameAirport1,
              DefaultNameCountry1,
              DefaultCodeIso2Country1,
              DefaultTimezone1,
              DefaultLatitude1,
              DefaultLongitude1,
              DefaultGmt1
            ),
            Airport(
              DefaultCodeAirport2,
              DefaultNameAirport2,
              DefaultNameCountry2,
              DefaultCodeIso2Country2,
              DefaultTimezone2,
              DefaultLatitude2,
              DefaultLongitude2,
              DefaultGmt2
            ),
            Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
            Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
            DefaultStatus,
            DefaultUpdated.toEpochMilli
          )
          val flightListMessage = FlightReceivedList(
            List(
              flightReceived,
              flightReceived.copy(updated = DefaultUpdated.minus(5, ChronoUnit.MINUTES).toEpochMilli),
              flightReceived.copy(updated = DefaultUpdated.plus(3, ChronoUnit.HOURS).toEpochMilli)
            )
          )

          messageProcessor ! flightListMessage

          val expectedMessage = AirportList(
            List(
              Airport(
                DefaultCodeAirport1,
                DefaultNameAirport1,
                DefaultNameCountry1,
                DefaultCodeIso2Country1,
                DefaultTimezone1,
                DefaultLatitude1,
                DefaultLongitude1,
                DefaultGmt1
              ),
              Airport(
                DefaultCodeAirport2,
                DefaultNameAirport2,
                DefaultNameCountry2,
                DefaultCodeIso2Country2,
                DefaultTimezone2,
                DefaultLatitude2,
                DefaultLongitude2,
                DefaultGmt2
              )
            )
          )

          val testProbe = new TestProbe(system)

          messageProcessor.tell(AirportListUpdate, testProbe.ref)

          testProbe expectMsg expectedMessage
      }
    }
  }

}
