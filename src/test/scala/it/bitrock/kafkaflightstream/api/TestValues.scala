package it.bitrock.kafkaflightstream.api

import scala.util.Random

trait TestValues {

  final val DefaultLatitude  = Random.nextDouble
  final val DefaultLongitude = Random.nextDouble
  final val DefaultAltitude  = Random.nextDouble
  final val DefaultDirection = Random.nextDouble

  final val DefaultCodeAirport1     = Random.nextString(10)
  final val DefaultNameAirport1     = Random.nextString(10)
  final val DefaultNameCountry1     = Random.nextString(10)
  final val DefaultCodeIso2Country1 = Random.nextString(10)
  final val DefaultTimezone1        = Random.nextString(10)
  final val DefaultGmt1             = Random.nextString(10)

  final val DefaultCodeAirport2     = Random.nextString(10)
  final val DefaultNameAirport2     = Random.nextString(10)
  final val DefaultNameCountry2     = Random.nextString(10)
  final val DefaultCodeIso2Country2 = Random.nextString(10)
  final val DefaultTimezone2        = Random.nextString(10)
  final val DefaultGmt2             = Random.nextString(10)

  final val DefaultCodeAirline = Random.nextString(10)
  final val DefaultNameAirline = Random.nextString(10)
  final val DefaultSizeAirline = Random.nextString(10)

  final val DefaultNumberRegistration = Random.nextString(10)
  final val DefaultProductionLine     = Random.nextString(10)
  final val DefaultModelCode          = Random.nextString(10)

  final val DefaultIataNumber = Random.nextString(10)
  final val DefaultIcaoNumber = Random.nextString(10)
  final val DefaultSpeed      = Random.nextDouble
  final val DefaultStatus     = Random.nextString(10)
  final val DefaultUpdated    = Random.nextString(10)

  final val DefaultArrivalAirport1Name     = Random.nextString(10)
  final val DefaultArrivalAirport1Amount   = Random.nextLong
  final val DefaultArrivalAirport2Name     = Random.nextString(10)
  final val DefaultArrivalAirport2Amount   = Random.nextLong
  final val DefaultArrivalAirport3Name     = Random.nextString(10)
  final val DefaultArrivalAirport3Amount   = Random.nextLong
  final val DefaultArrivalAirport4Name     = Random.nextString(10)
  final val DefaultArrivalAirport4Amount   = Random.nextLong
  final val DefaultArrivalAirport5Name     = Random.nextString(10)
  final val DefaultArrivalAirport5Amount   = Random.nextLong
  final val DefaultDepartureAirport1Name   = Random.nextString(10)
  final val DefaultDepartureAirport1Amount = Random.nextLong
  final val DefaultDepartureAirport2Name   = Random.nextString(10)
  final val DefaultDepartureAirport2Amount = Random.nextLong
  final val DefaultDepartureAirport3Name   = Random.nextString(10)
  final val DefaultDepartureAirport3Amount = Random.nextLong
  final val DefaultDepartureAirport4Name   = Random.nextString(10)
  final val DefaultDepartureAirport4Amount = Random.nextLong
  final val DefaultDepartureAirport5Name   = Random.nextString(10)
  final val DefaultDepartureAirport5Amount = Random.nextLong

  final val DefaultFlightCode1 = Random.nextString(10)
  final val DefaultSpeed1      = Random.nextDouble
  final val DefaultFlightCode2 = Random.nextString(10)
  final val DefaultSpeed2      = Random.nextDouble
  final val DefaultFlightCode3 = Random.nextString(10)
  final val DefaultSpeed3      = Random.nextDouble
  final val DefaultFlightCode4 = Random.nextString(10)
  final val DefaultSpeed4      = Random.nextDouble
  final val DefaultFlightCode5 = Random.nextString(10)
  final val DefaultSpeed5      = Random.nextDouble

  final val DefaultAirline1Name   = Random.nextString(10)
  final val DefaultAirline1Amount = Random.nextLong
  final val DefaultAirline2Name   = Random.nextString(10)
  final val DefaultAirline2Amount = Random.nextLong
  final val DefaultAirline3Name   = Random.nextString(10)
  final val DefaultAirline3Amount = Random.nextLong
  final val DefaultAirline4Name   = Random.nextString(10)
  final val DefaultAirline4Amount = Random.nextLong
  final val DefaultAirline5Name   = Random.nextString(10)
  final val DefaultAirline5Amount = Random.nextLong

  final val DefaultCountFlightAmount  = Random.nextLong
  final val DefaultCountAirlineAmount = Random.nextLong
  final val DefaultStartTimeWindow    = Random.nextString(10)

}
