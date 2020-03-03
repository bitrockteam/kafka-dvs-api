package it.bitrock.dvs.api.core.dispatcher

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit.ResourceDispatcher
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.core.poller.{FlightListKafkaPollerCache, TopsKafkaPollerCache, TotalsKafkaPollerCache}
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.model.{TotalFlightsCount, _}
import spray.json._

import scala.concurrent.duration._

class GlobalMessageDispatcherSpec extends BaseTestKit {

  "GlobalMessageDispatcher" should {
    val timeout = 6.seconds

    "forward a JSON to source actor" when {
      "a correct FlightReceivedList message is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val flightReceivedList = FlightReceivedList(
            List(
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
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (FlightListUpdate == msg) {
              sender ! flightReceivedList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None)

          val expectedMessage = ApiEvent("FlightList", flightReceivedList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
      }
      "the flights in the list are inside the box after its change" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val flightReceivedList = FlightReceivedList(
            List(
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
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (FlightListUpdate == msg) {
              sender ! flightReceivedList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! changedBox

          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("FlightList", flightReceivedList).toJson.toString
          }
      }

      "the flights in the list are inside the box after its change with update rate" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val flightReceivedList = FlightReceivedList(
            List(
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
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (FlightListUpdate == msg) {
              sender ! flightReceivedList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! changedBox1Minute

          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("FlightList", flightReceivedList).toJson.toString
          }
          sourceProbe.expectNoMessage(timeout)
      }
    }

    "forward an empty message to source actor" when {
      "the flights in the list are out of the box" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val flightReceivedList = FlightReceivedList(
            List(
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
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (FlightListUpdate == msg) {
              sender ! flightReceivedList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None)

          val expectedMessage = ApiEvent("FlightList", FlightReceivedList(List())).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
      }
    }

    "forward a JSON to source actor" when {
      "a correct AirportList message is received, just once per CoordinateBox received message" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val airportList = AirportList(
            List(
              Airport(
                DefaultCodeAirport1,
                DefaultNameAirport1,
                DefaultNameCountry1,
                DefaultCodeIso2Country1,
                DefaultTimezone1,
                DefaultInBoxLatitude,
                DefaultInBoxLongitude,
                DefaultGmt1
              ),
              Airport(
                DefaultCodeAirport2,
                DefaultNameAirport2,
                DefaultNameCountry2,
                DefaultCodeIso2Country2,
                DefaultTimezone2,
                DefaultInBoxLatitude,
                DefaultInBoxLongitude,
                DefaultGmt2
              )
            )
          )
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (AirportListUpdate == msg) {
              sender ! airportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None)

          val expectedMessage = ApiEvent("AirportList", airportList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
          sourceProbe.expectNoMessage(timeout)
      }
      "the airports in the list are inside the box after its change, just once per CoordinateBox received message" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val airportList = AirportList(
            List(
              Airport(
                DefaultCodeAirport1,
                DefaultNameAirport1,
                DefaultNameCountry1,
                DefaultCodeIso2Country1,
                DefaultTimezone1,
                DefaultChangedInBoxLatitude,
                DefaultChangedInBoxLongitude,
                DefaultGmt1
              ),
              Airport(
                DefaultCodeAirport2,
                DefaultNameAirport2,
                DefaultNameCountry2,
                DefaultCodeIso2Country2,
                DefaultTimezone2,
                DefaultChangedInBoxLatitude,
                DefaultChangedInBoxLongitude,
                DefaultGmt2
              )
            )
          )
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (AirportListUpdate == msg) {
              sender ! airportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! changedBox

          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("AirportList", airportList).toJson.toString
          }
          sourceProbe.expectNoMessage(timeout)
      }

      "the airports in the list are inside the box after its change with update rate, just once per CoordinateBox received message" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val airportList = AirportList(
            List(
              Airport(
                DefaultCodeAirport1,
                DefaultNameAirport1,
                DefaultNameCountry1,
                DefaultCodeIso2Country1,
                DefaultTimezone1,
                DefaultChangedInBoxLatitude,
                DefaultChangedInBoxLongitude,
                DefaultGmt1
              ),
              Airport(
                DefaultCodeAirport2,
                DefaultNameAirport2,
                DefaultNameCountry2,
                DefaultCodeIso2Country2,
                DefaultTimezone2,
                DefaultChangedInBoxLatitude,
                DefaultChangedInBoxLongitude,
                DefaultGmt2
              )
            )
          )
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (AirportListUpdate == msg) {
              sender ! airportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! changedBox1Minute

          sourceProbe.fishForMessage(timeout) {
            case m: String => m === ApiEvent("AirportList", airportList).toJson.toString
          }
          sourceProbe.expectNoMessage(timeout)
      }
    }

    "forward an empty message to source actor" when {
      "the airports in the list are out of the box, just once per CoordinateBox received message" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCacheTestProbe = TestProbe()
          val topsKafkaPollerCache                = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCache              = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, topsKafkaPollerCache, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val airportList = AirportList(
            List(
              Airport(
                DefaultCodeAirport1,
                DefaultNameAirport1,
                DefaultNameCountry1,
                DefaultCodeIso2Country1,
                DefaultTimezone1,
                DefaultOutBoxLatitude,
                DefaultOutBoxLongitude,
                DefaultGmt1
              ),
              Airport(
                DefaultCodeAirport2,
                DefaultNameAirport2,
                DefaultNameCountry2,
                DefaultCodeIso2Country2,
                DefaultTimezone2,
                DefaultOutBoxLatitude,
                DefaultOutBoxLongitude,
                DefaultGmt2
              )
            )
          )
          flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (AirportListUpdate == msg) {
              sender ! airportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None)

          val expectedMessage = ApiEvent("AirportList", AirportList(List())).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedMessage
          }
          sourceProbe.expectNoMessage(timeout)
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
          val flightListKafkaPollerCache    = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCacheTestProbe = TestProbe()
          val totalsKafkaPollerCache        = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCacheTestProbe.ref, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val topArrivalAirportList =
            TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          topsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TopArrivalAirportUpdate == msg) {
              sender ! topArrivalAirportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTops(None)

          val expectedResult = ApiEvent("TopArrivalAirportList", topArrivalAirportList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopArrivalAirportList is received with update rate" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache    = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCacheTestProbe = TestProbe()
          val totalsKafkaPollerCache        = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCacheTestProbe.ref, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig.copy(throttleDuration = 1 minute))
              .build(sourceProbe.ref)
          val topArrivalAirportList =
            TopArrivalAirportList(List(AirportCount(DefaultArrivalAirport1Name, DefaultArrivalAirport1Amount)))
          topsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TopArrivalAirportUpdate == msg) {
              sender ! topArrivalAirportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTops(Some(1 seconds))

          val expectedResult = ApiEvent("TopArrivalAirportList", topArrivalAirportList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopDepartureAirportList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache    = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCacheTestProbe = TestProbe()
          val totalsKafkaPollerCache        = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCacheTestProbe.ref, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val topDepartureAirportList =
            TopDepartureAirportList(List(AirportCount(DefaultDepartureAirport1Name, DefaultDepartureAirport1Amount)))
          topsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TopDepartureAirportUpdate == msg) {
              sender ! topDepartureAirportList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTops(None)

          val expectedResult = ApiEvent("TopDepartureAirportList", topDepartureAirportList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopSpeedList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache    = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCacheTestProbe = TestProbe()
          val totalsKafkaPollerCache        = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCacheTestProbe.ref, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val topSpeedList = TopSpeedList(List(SpeedFlight(DefaultFlightCode1, DefaultSpeed)))
          topsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TopDepartureAirportUpdate == msg) {
              sender ! topSpeedList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTops(None)

          val expectedResult = ApiEvent("TopSpeedList", topSpeedList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a TopAirlineList is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache    = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCacheTestProbe = TestProbe()
          val totalsKafkaPollerCache        = TotalsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCacheTestProbe.ref, totalsKafkaPollerCache)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val topAirlineList = TopAirlineList(List(AirlineCount(DefaultAirline1Name, DefaultAirline1Amount)))
          topsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TopArrivalAirportUpdate == msg) {
              sender ! topAirlineList
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTops(None)

          val expectedResult = ApiEvent("TopAirlineList", topAirlineList).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
    }

    "forward a JSON to source actor" when {
      "a CountFlight is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache      = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache            = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCacheTestProbe = TestProbe()
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCacheTestProbe.ref)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val totalFlightsCount = TotalFlightsCount(DefaultStartTimeWindow, DefaultCountFlightAmount)
          totalsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TotalFlightUpdate == msg) {
              sender ! totalFlightsCount
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTotals(None)

          val expectedResult = ApiEvent("TotalFlightsCount", totalFlightsCount).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a CountAirline is received" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache      = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache            = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCacheTestProbe = TestProbe()
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCacheTestProbe.ref)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
              .build(sourceProbe.ref)
          val totalAirlinesCount = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          totalsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TotalAirlineUpdate == msg) {
              sender ! totalAirlinesCount
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTotals(None)

          val expectedResult = ApiEvent("TotalAirlinesCount", totalAirlinesCount).toJson.toString
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
      "a CountAirline is received with update rate" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, kafkaConfig, consumerFactory, sourceProbe) =>
          val flightListKafkaPollerCache      = FlightListKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val topsKafkaPollerCache            = TopsKafkaPollerCache.build(kafkaConfig, consumerFactory)
          val totalsKafkaPollerCacheTestProbe = TestProbe()
          val kafkaPollerHub =
            KafkaPollerHub(flightListKafkaPollerCache, topsKafkaPollerCache, totalsKafkaPollerCacheTestProbe.ref)
          val messageProcessor =
            MessageDispatcherFactory
              .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig.copy(throttleDuration = 1 minute))
              .build(sourceProbe.ref)
          val totalAirlinesCount = TotalAirlinesCount(DefaultStartTimeWindow, DefaultCountAirlineAmount)
          totalsKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
            if (TotalFlightUpdate == msg) {
              sender ! totalAirlinesCount
            }
            TestActor.KeepRunning
          }

          messageProcessor ! StartTotals(Some(-2 second))

          val expectedResult = ApiEvent("TotalAirlinesCount", totalAirlinesCount).toJson.toString
          sourceProbe.fishForMessage(1 second) {
            case m: String => m === expectedResult
          }
          sourceProbe.fishForMessage(timeout) {
            case m: String => m === expectedResult
          }
      }
    }

  }
}
