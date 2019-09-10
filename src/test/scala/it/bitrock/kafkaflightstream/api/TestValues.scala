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

  final val DefaultCodeAirport2     = Random.nextString(10)
  final val DefaultNameAirport2     = Random.nextString(10)
  final val DefaultNameCountry2     = Random.nextString(10)
  final val DefaultCodeIso2Country2 = Random.nextString(10)

  final val DefaultNameAirline = Random.nextString(10)
  final val DefaultSizeAirline = Random.nextString(10)

  final val DefaultProductionLine = Random.nextString(10)
  final val DefaultModelCode      = Random.nextString(10)

  final val DefaultSpeed  = Random.nextDouble
  final val DefaultStatus = Random.nextString(10)

}
