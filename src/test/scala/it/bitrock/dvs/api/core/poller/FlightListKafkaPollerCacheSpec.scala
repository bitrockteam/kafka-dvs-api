package it.bitrock.dvs.api.core.poller

import java.time.temporal.ChronoUnit

import akka.testkit.TestProbe
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.FlightListUpdate
import it.bitrock.dvs.api.model._

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
            Seq(
              flightReceived,
              flightReceived.copy(updated = DefaultUpdated.minus(5, ChronoUnit.MINUTES).toEpochMilli),
              flightReceived.copy(updated = DefaultUpdated.plus(3, ChronoUnit.HOURS).toEpochMilli)
            )
          )
          pollProbe expectMsg PollingTriggered
          messageProcessor ! flightListMessage
          pollProbe expectMsg PollingTriggered

          val testProbe = new TestProbe(system)

          messageProcessor.tell(FlightListUpdate, testProbe.ref)

          testProbe expectMsg flightListMessage.copy(elements = flightListMessage.elements.sortBy(_.updated).reverse)
      }
    }
  }

}
