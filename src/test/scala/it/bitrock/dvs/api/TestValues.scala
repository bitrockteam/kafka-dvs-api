package it.bitrock.dvs.api

import java.time.Instant

import it.bitrock.dvs.api.model.CoordinatesBox

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object TestValues {

  final val DefaultLatitude  = Random.nextDouble
  final val DefaultLongitude = Random.nextDouble
  final val DefaultAltitude  = Random.nextDouble
  final val DefaultDirection = Random.nextDouble

  final val DefaultInBoxLatitude   = 47
  final val DefaultInBoxLongitude  = 10
  final val DefaultOutBoxLatitude  = 0
  final val DefaultOutBoxLongitude = 0

  final val changedBox                   = CoordinatesBox(101, 99, 99, 101, None, List.empty)
  final val changedBox1Minute            = CoordinatesBox(101, 99, 99, 101, Some(1 minute), List.empty)
  final val DefaultChangedInBoxLatitude  = 100
  final val DefaultChangedInBoxLongitude = 100

  final val DefaultCodeAirport1     = randomString(10)
  final val DefaultNameAirport1     = randomString(10)
  final val DefaultNameCountry1     = randomString(10)
  final val DefaultCodeIso2Country1 = randomString(10)
  final val DefaultTimezone1        = randomString(10)
  final val DefaultLatitude1        = Random.nextDouble
  final val DefaultLongitude1       = Random.nextDouble
  final val DefaultGmt1             = randomString(10)

  final val DefaultCodeAirport2     = randomString(10)
  final val DefaultNameAirport2     = randomString(10)
  final val DefaultNameCountry2     = randomString(10)
  final val DefaultCodeIso2Country2 = randomString(10)
  final val DefaultTimezone2        = randomString(10)
  final val DefaultLatitude2        = Random.nextDouble
  final val DefaultLongitude2       = Random.nextDouble
  final val DefaultGmt2             = randomString(10)

  final val defaultCityName1 = "Milan"
  final val defaultCityName2 = "London"

  final val DefaultCodeAirline = randomString(10)
  final val DefaultNameAirline = randomString(10)
  final val DefaultSizeAirline = Random.nextLong

  final val DefaultNumberRegistration = randomString(10)
  final val DefaultProductionLine     = randomString(10)
  final val DefaultModelCode          = randomString(10)

  final val DefaultIataNumber        = randomString(10)
  final val DefaultIcaoNumber        = randomString(10)
  final val DefaultSpeed             = Random.nextDouble
  final val DefaultStatus            = randomString(10)
  final val DefaultInterpolatedUntil = Instant.now()
  final val DefaultUpdated           = DefaultInterpolatedUntil.minusSeconds(5)

  final val DefaultArrivalAirport1Name     = randomString(10)
  final val DefaultArrivalAirport1Amount   = Random.nextLong
  final val DefaultArrivalAirport2Name     = randomString(10)
  final val DefaultArrivalAirport2Amount   = Random.nextLong
  final val DefaultArrivalAirport3Name     = randomString(10)
  final val DefaultArrivalAirport3Amount   = Random.nextLong
  final val DefaultArrivalAirport4Name     = randomString(10)
  final val DefaultArrivalAirport4Amount   = Random.nextLong
  final val DefaultArrivalAirport5Name     = randomString(10)
  final val DefaultArrivalAirport5Amount   = Random.nextLong
  final val DefaultDepartureAirport1Name   = randomString(10)
  final val DefaultDepartureAirport1Amount = Random.nextLong
  final val DefaultDepartureAirport2Name   = randomString(10)
  final val DefaultDepartureAirport2Amount = Random.nextLong
  final val DefaultDepartureAirport3Name   = randomString(10)
  final val DefaultDepartureAirport3Amount = Random.nextLong
  final val DefaultDepartureAirport4Name   = randomString(10)
  final val DefaultDepartureAirport4Amount = Random.nextLong
  final val DefaultDepartureAirport5Name   = randomString(10)
  final val DefaultDepartureAirport5Amount = Random.nextLong

  final val DefaultFlightCode1 = randomString(10)
  final val DefaultSpeed1      = Random.nextDouble
  final val DefaultFlightCode2 = randomString(10)
  final val DefaultSpeed2      = Random.nextDouble
  final val DefaultFlightCode3 = randomString(10)
  final val DefaultSpeed3      = Random.nextDouble
  final val DefaultFlightCode4 = randomString(10)
  final val DefaultSpeed4      = Random.nextDouble
  final val DefaultFlightCode5 = randomString(10)
  final val DefaultSpeed5      = Random.nextDouble

  final val DefaultAirline1Name   = randomString(10)
  final val DefaultAirline1Amount = Random.nextLong
  final val DefaultAirline2Name   = randomString(10)
  final val DefaultAirline2Amount = Random.nextLong
  final val DefaultAirline3Name   = randomString(10)
  final val DefaultAirline3Amount = Random.nextLong
  final val DefaultAirline4Name   = randomString(10)
  final val DefaultAirline4Amount = Random.nextLong
  final val DefaultAirline5Name   = randomString(10)
  final val DefaultAirline5Amount = Random.nextLong

  final val DefaultCountFlightAmount  = Math.abs(Random.nextLong)
  final val DefaultCountAirlineAmount = Math.abs(Random.nextLong)
  final val DefaultStartTimeWindow    = randomString(10)

  final val coordinatesBox: String =
    """
      | {
      |   "@type": "startFlightList",
      |   "leftHighLat": 23.6,
      |   "leftHighLon": 67.9,
      |   "rightLowLat": 37.98,
      |   "rightLowLon": 43.45,
      |   "precedences": []
      | }
      |""".stripMargin
  final val coordinatesBoxWithRate: String =
    """
      | {
      |   "@type": "startFlightList",
      |   "leftHighLat": 23.6,
      |   "leftHighLon": 67.9,
      |   "rightLowLat": 37.98,
      |   "rightLowLon": 43.45,
      |   "rightLowLon": 43.45,
      |   "updateRate": 60,
      |   "precedences": []
      | }
      |""".stripMargin
  final val coordinatesBoxWithPrecedence: String =
    """
      | {
      |   "@type": "startFlightList",
      |   "leftHighLat": 23.6,
      |   "leftHighLon": 67.9,
      |   "rightLowLat": 37.98,
      |   "rightLowLon": 43.45,
      |   "rightLowLon": 43.45,
      |   "precedences": [{
      |     "arrivalAirport": "Malpensa",
      |     "departureAirport": "Fiumicino"
      |   }]
      | }
      |""".stripMargin
  final val stopFlightList =
    """
      |{
      |  "@type": "stopFlightList"
      |}
      |""".stripMargin
  final val startTop =
    """
      |{
      |  "@type": "startTop"
      |}
      |""".stripMargin
  final val startTopWithRate =
    """
      |{
      |  "@type": "startTop",
      |  "updateRate": 30
      |}
      |""".stripMargin
  final val stopTop =
    """
      |{
      | "@type": "stopTop"
      |}
      |""".stripMargin
  final val startTotal =
    """
      |{
      | "@type": "startTotal"
      |}
      |""".stripMargin
  final val startTotalWithRate =
    """
      |{
      | "@type": "startTotal",
      | "updateRate": -15
      |}
      |""".stripMargin
  final val stopTotal =
    """
      |{
      | "@type": "stopTotal"
      |}
      |""".stripMargin

  private def randomString(size: Int) = Random.alphanumeric.take(size).mkString
}
