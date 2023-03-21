package it.bitrock.dvs.api.core.dispatcher

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import it.bitrock.dvs.api.BaseTestKit
import it.bitrock.dvs.api.BaseTestKit.ResourceDispatcher
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.factory.MessageDispatcherFactory
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.model._
import spray.json._

import scala.concurrent.duration._

class GlobalMessageDispatcherPrecedenceSpec extends BaseTestKit {

  private val flightReceived = FlightReceived(
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
      DefaultGmt1,
      defaultCityName1
    ),
    Airport(
      DefaultCodeAirport2,
      DefaultNameAirport2,
      DefaultNameCountry2,
      DefaultCodeIso2Country2,
      DefaultTimezone2,
      DefaultLatitude2,
      DefaultLongitude2,
      DefaultGmt2,
      defaultCityName2
    ),
    Airline(DefaultCodeAirline, DefaultNameAirline, DefaultSizeAirline),
    Airplane(DefaultNumberRegistration, DefaultProductionLine, DefaultModelCode),
    DefaultStatus,
    DefaultUpdated.toEpochMilli
  )

  "GlobalMessageDispatcher with Precedence" should {
    val timeout = 6.seconds

    "forward a JSON with same order" when {
      "a correct FlightReceivedList message is received and no Precedence" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              flightReceived.copy(iataNumber = "iatacode2")
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None, List.empty)

          val expectedMessage = ApiEvent("FlightList", flightReceivedList).toJson.toString
          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence is empty" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              flightReceived.copy(iataNumber = "iatacode2")
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None, List(Precedence(None, None, None)))

          val expectedMessage = ApiEvent("FlightList", flightReceivedList).toJson.toString
          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence with multiple fields but none matching" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived.copy(airportArrival = flightReceived.airportArrival.copy(codeAirport = "codeAirportArrival")),
              flightReceived.copy(airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "codeAirportDeparture")),
              flightReceived.copy(airline = flightReceived.airline.copy(codeAirline = "codeAirline"))
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(
            49.8,
            -3.7,
            39.7,
            23.6,
            None,
            List(Precedence(Some("codeAirportDeparture"), Some("codeAirportArrival"), Some("codeAirline")))
          )

          val expectedMessage =
            ApiEvent("FlightList", flightReceivedList).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

    }

    "forward a JSON with precedence order" when {

      "a correct FlightReceivedList message is received and Precedence with arrival airport" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(airportArrival = flightReceived.airportArrival.copy(codeAirport = "code1"))
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              withPrecedence,
              flightReceived
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None, List(Precedence(None, Some("code1"), None)))

          val expectedMessage =
            ApiEvent("FlightList", FlightReceivedList(List(withPrecedence, flightReceived, flightReceived))).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence with departure airport" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "code1"))
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              flightReceived,
              withPrecedence
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None, List(Precedence(Some("code1"), None, None)))

          val expectedMessage =
            ApiEvent("FlightList", FlightReceivedList(List(withPrecedence, flightReceived, flightReceived))).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence with airline" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(airline = flightReceived.airline.copy(codeAirline = "code1"))
          val flightReceivedList = FlightReceivedList(
            List(
              withPrecedence,
              flightReceived,
              flightReceived
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(49.8, -3.7, 39.7, 23.6, None, List(Precedence(None, None, Some("code1"))))

          val expectedMessage =
            ApiEvent("FlightList", FlightReceivedList(List(withPrecedence, flightReceived, flightReceived))).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence with multiple fields" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(
            airportArrival = flightReceived.airportArrival.copy(codeAirport = "codeAirportArrival"),
            airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "codeAirportDeparture"),
            airline = flightReceived.airline.copy(codeAirline = "codeAirline")
          )
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              flightReceived,
              withPrecedence
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(
            49.8,
            -3.7,
            39.7,
            23.6,
            None,
            List(Precedence(Some("codeAirportDeparture"), Some("codeAirportArrival"), Some("codeAirline")))
          )

          val expectedMessage =
            ApiEvent("FlightList", FlightReceivedList(List(withPrecedence, flightReceived, flightReceived))).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and Precedence with multiple fields and many matches" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(
            airportArrival = flightReceived.airportArrival.copy(codeAirport = "codeAirportArrival"),
            airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "codeAirportDeparture"),
            airline = flightReceived.airline.copy(codeAirline = "codeAirline")
          )
          val secondWithPrecedence = withPrecedence.copy(icaoNumber = "newicao")
          val thirdWithPrecedence  = withPrecedence.copy(iataNumber = "newiata")
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              withPrecedence,
              secondWithPrecedence,
              flightReceived,
              thirdWithPrecedence
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(
            49.8,
            -3.7,
            39.7,
            23.6,
            None,
            List(Precedence(Some("codeAirportDeparture"), Some("codeAirportArrival"), Some("codeAirline")))
          )

          val expectedMessage =
            ApiEvent(
              "FlightList",
              FlightReceivedList(List(withPrecedence, secondWithPrecedence, thirdWithPrecedence, flightReceived, flightReceived))
            ).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and two precedences: arrival airport and airline" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence =
            flightReceived.copy(airportArrival = flightReceived.airportArrival.copy(codeAirport = "airportArrival"))
          val withPrecedence2 =
            flightReceived.copy(airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "airportDeparture"))
          val withPrecedence3 =
            withPrecedence.copy(airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "airportDeparture"))
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              withPrecedence,
              flightReceived,
              withPrecedence2,
              withPrecedence3
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(
            49.8,
            -3.7,
            39.7,
            23.6,
            None,
            List(Precedence(None, Some("airportArrival"), None), Precedence(Some("airportDeparture"), None, None))
          )

          val expectedMessage =
            ApiEvent(
              "FlightList",
              FlightReceivedList(List(withPrecedence, withPrecedence2, withPrecedence3, flightReceived, flightReceived))
            ).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

      "a correct FlightReceivedList message is received and multiple precedences with multiple fields and many matches" in ResourceLoanerDispatcher.withFixture {
        case ResourceDispatcher(webSocketConfig, _, _, sourceProbe) =>
          val withPrecedence = flightReceived.copy(
            airportArrival = flightReceived.airportArrival.copy(codeAirport = "codeAirportArrival"),
            airportDeparture = flightReceived.airportDeparture.copy(codeAirport = "codeAirportDeparture"),
            airline = flightReceived.airline.copy(codeAirline = "codeAirline")
          )
          val secondWithPrecedence = withPrecedence.copy(icaoNumber = "newicao")
          val thirdWithPrecedence  = withPrecedence.copy(iataNumber = "newiata")
          val flightReceivedList = FlightReceivedList(
            List(
              flightReceived,
              withPrecedence,
              secondWithPrecedence,
              flightReceived,
              thirdWithPrecedence
            )
          )

          val messageProcessor: ActorRef = testSetup(webSocketConfig, sourceProbe, flightReceivedList)

          messageProcessor ! CoordinatesBox(
            49.8,
            -3.7,
            39.7,
            23.6,
            None,
            List(
              Precedence(Some("codeAirportDeparture"), Some("codeAirportArrival"), None),
              Precedence(None, None, Some("codeAirline")),
              Precedence(Some("NotMatching"), Some("NotMatching"), Some("NotMatching"))
            )
          )

          val expectedMessage =
            ApiEvent(
              "FlightList",
              FlightReceivedList(List(withPrecedence, secondWithPrecedence, thirdWithPrecedence, flightReceived, flightReceived))
            ).toJson.toString

          sourceProbe.expectMsg(timeout, expectedMessage)

      }

    }
  }

  private def testSetup(webSocketConfig: WebSocketConfig, sourceProbe: TestProbe, flightReceivedList: FlightReceivedList) = {
    val flightListKafkaPollerCacheTestProbe = TestProbe()
    val kafkaPollerHub =
      KafkaPollerHub(flightListKafkaPollerCacheTestProbe.ref, TestProbe().ref, TestProbe().ref)
    val messageProcessor =
      MessageDispatcherFactory
        .globalMessageDispatcherFactory(kafkaPollerHub, webSocketConfig)
        .build(sourceProbe.ref)

    flightListKafkaPollerCacheTestProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
      if (FlightListUpdate == msg) {
        sender ! flightReceivedList
      }
      TestActor.KeepRunning
    }

    messageProcessor
  }
}
