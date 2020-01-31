package it.bitrock.dvs.api.core.dispatcher

import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit.ResourceDispatcher
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.{FlightListKafkaPollerCache, TopsKafkaPollerCache, TotalsKafkaPollerCache}
import it.bitrock.dvs.api.model._
import org.scalatest.concurrent.Eventually
import spray.json._

class GlobalMessageDispatcherSpec extends BaseTestKit with Eventually {

  "GlobalMessageDispatcher" should {

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
                DefaultUpdated.toEpochMilli
              )
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          messageProcessor ! msg
          eventually {
            sourceProbe expectMsg msg.toJson.toString
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
                DefaultUpdated.toEpochMilli
              )
            )
          )
          messageProcessor ! changedBox
          messageProcessor ! msg
          eventually {
            sourceProbe expectMsg msg.toJson.toString
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
                DefaultUpdated.toEpochMilli
              )
            )
          )
          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6)
          eventually {
            sourceProbe expectMsg FlightReceivedList(Seq()).toJson.toString
          }
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopArrivalAirportList", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopDepartureAirportList", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopSpeedList", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TopAirlineList", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TotalFlightsCount", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
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
          messageProcessor ! msg
          val expectedResult = ApiEvent("TotalAirlinesCount", msg).toJson.toString
          eventually {
            sourceProbe.expectMsg(expectedResult)
          }
      }
    }

  }
}
