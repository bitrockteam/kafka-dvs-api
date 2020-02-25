package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit.ResourceDispatcher
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.{FlightListKafkaPollerCache, TopsKafkaPollerCache, TotalsKafkaPollerCache}
import it.bitrock.dvs.api.model._
import spray.json._

import scala.concurrent.duration._

class GlobalMessageDispatcherSpec extends BaseTestKit {

  "GlobalMessageDispatcher" should {
    val timeout = 6.seconds

    "forward a JSON to source actor" when {
      "a correct FlightReceivedList message is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
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
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          messageProcessor ! msg
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("FlightList", msg).toJson.toString
          }
      }
      "the flights in the list are inside the box after its change" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
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
            )
          )
          messageProcessor ! changedBox
          messageProcessor ! msg
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("FlightList", msg).toJson.toString
          }
      }
    }

    "forward an empty message to source actor" when {
      "the flights in the list are out of the box" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
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
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("FlightList", FlightReceivedList(Seq())).toJson.toString
          }
      }
    }

    "not forward a JSON to source actor" when {
      "the start message was not sent" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          messageProcessor ! msg
          sourceProbe.expectNoMessage
      }
    }

    "forward a JSON to source actor" when {
      "a TopArrivalAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          messageProcessor ! StartTops
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopArrivalAirportList", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopDepartureAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg =
            TopDepartureAirportList(List(AirportCount(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))
          messageProcessor ! StartTops
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopDepartureAirportList", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopSpeedList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopSpeedList(List(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))
          messageProcessor ! StartTops
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopSpeedList", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopAirlineList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TopAirlineList(List(AirlineCount(DefaultAirline1Name, DefaultAirline1Amount)))
          messageProcessor ! StartTops
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopAirlineList", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
    }

    "forward a JSON to source actor" when {
      "a CountFlight is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TotalFlightsCount(DefaultStartTimeWindow, DefaultCountFlightAmount)
          messageProcessor ! StartTotals
          messageProcessor ! msg
          val expectedResult = ApiEvent("TotalFlightsCount", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a CountAirline is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache       = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache     = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub             = KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val msg = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          messageProcessor ! StartTotals
          messageProcessor ! msg
          val expectedResult = ApiEvent("TotalAirlinesCount", msg).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
    }

  }
}
