package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit._
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.FlightListKafkaPollerCache
import it.bitrock.dvs.api.model._
import spray.json._

class FlightListMessageDispatcherSpec extends BaseTestKit {

  "Flight List Message Dispatcher" should {

    "forward a JSON to source actor" when {
      "a correct FlightReceivedList message is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            MessageDispatcherFactory
              .flightListMessageDispatcherFactory(flightListKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = FlightReceivedList(
            Seq(
              FlightReceived(
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
                  DefaultGmt1
                ),
                Airport(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated
              )
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          messageProcessor ! msg
          sourceProbe expectMsg msg.toJson.toString
      }
      "the flights in the list are inside the box after its change" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            MessageDispatcherFactory
              .flightListMessageDispatcherFactory(flightListKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                Geography(DefaultChangedInBoxLatitude, DefaultChangedInBoxLongitude, DefaultAltitude, DefaultDirection),
                DefaultSpeed,
                Airport(
                  DefaultCodeAirport1,
                  DefaultNameAirport1,
                  DefaultNameCountry1,
                  DefaultCodeIso2Country1,
                  DefaultTimezone1,
                  DefaultGmt1
                ),
                Airport(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated
              )
            )
          )
          messageProcessor ! changedBox
          messageProcessor ! msg
          sourceProbe expectMsg msg.toJson.toString
      }
    }

    "forward an empty message to source actor" when {
      "the flights in the list are out of the box" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val messageProcessor =
            MessageDispatcherFactory
              .flightListMessageDispatcherFactory(flightListKafkaPollerCache, webSocketConfig)
              .build(sourceProbe.ref)
          messageProcessor ! FlightReceivedList(
            Seq(
              FlightReceived(
                DefaultIataNumber,
                DefaultIcaoNumber,
                Geography(DefaultOutBoxLatitude, DefaultOutBoxLongitude, DefaultAltitude, DefaultDirection),
                DefaultSpeed,
                Airport(
                  DefaultCodeAirport1,
                  DefaultNameAirport1,
                  DefaultNameCountry1,
                  DefaultCodeIso2Country1,
                  DefaultTimezone1,
                  DefaultGmt1
                ),
                Airport(
                  DefaultCodeAirport2,
                  DefaultNameAirport2,
                  DefaultNameCountry2,
                  DefaultCodeIso2Country2,
                  DefaultTimezone2,
                  DefaultGmt2
                ),
                Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
                Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
                DefaultStatus,
                DefaultUpdated
              )
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          sourceProbe expectMsg FlightReceivedList(Seq()).toJson.toString
      }
    }

  }

}
