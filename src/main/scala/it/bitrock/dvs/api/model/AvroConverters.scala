package it.bitrock.dvs.api.model

import it.bitrock.dvs.model.avro.{
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  AirportInfo => KAirportInfo,
  CountAirline => KCountAirline,
  CountFlight => KCountFlight,
  FlightInterpolated => KFlightReceivedEvent,
  FlightInterpolatedList => KFlightReceivedListEvent,
  GeographyInfo => KGeographyInfo,
  TopAirline => KTopAirline,
  TopAirport => KTopAirport,
  TopSpeed => KTopSpeed,
  TopAirlineList => KTopAirlineList,
  TopArrivalAirportList => KTopArrivalAirportList,
  TopDepartureAirportList => KTopDepartureAirportList,
  TopSpeedList => KTopSpeedList
}

object AvroConverters {

  implicit class TopAirlineListOps(x: KTopAirlineList) {
    def toTopAirlineList: TopAirlineList = TopAirlineList(x.elements.toList.map(toAirline))
  }

  implicit class CountFlightOps(x: KCountFlight) {
    def toCountFlight: TotalFlightsCount = TotalFlightsCount(x.windowStartTime, x.eventCount)
  }

  implicit class CountAirlineOps(x: KCountAirline) {
    def toCountAirline: TotalAirlinesCount = TotalAirlinesCount(x.windowStartTime, x.eventCount)
  }

  implicit class FlightReceivedOps(x: KFlightReceivedEvent) {
    def toFlightReceived: FlightReceived =
      FlightReceived(
        x.iataNumber,
        x.icaoNumber,
        toGeographyInfo(x.geography),
        x.speed,
        toAirportInfo(x.departureAirport),
        toAirportInfo(x.arrivalAirport),
        toAirlineInfo(x.airline),
        toAirplaneInfo(x.airplane),
        x.status,
        x.updated.toEpochMilli,
        x.interpolatedUntil.toEpochMilli
      )
  }

  implicit class FlightReceivedListOps(x: KFlightReceivedListEvent) {
    def toFlightReceivedList: FlightReceivedList =
      FlightReceivedList(x.elements.map(_.toFlightReceived))
  }

  implicit class TopArrivalAirportListOps(x: KTopArrivalAirportList) {
    def toTopArrivalAirportList: TopArrivalAirportList = TopArrivalAirportList(x.elements.toList.map(toAirport))
  }

  implicit class TopDepartureAirportListOps(x: KTopDepartureAirportList) {
    def toTopDepartureAirportList: TopDepartureAirportList = TopDepartureAirportList(x.elements.toList.map(toAirport))
  }

  implicit class TopSpeedListOps(x: KTopSpeedList) {
    def toTopSpeedList: TopSpeedList = TopSpeedList(x.elements.toList.map(toSpeedFlight))
  }

  private def toGeographyInfo(x: KGeographyInfo): Geography =
    Geography(x.latitude, x.longitude, x.altitude, x.direction)

  private def toAirportInfo(x: KAirportInfo): Airport =
    Airport(x.code, x.name, x.countryName, x.countryIsoCode2, x.timezone, x.latitude, x.longitude, x.gmt)

  private def toAirlineInfo(x: KAirlineInfo): Airline =
    Airline(x.code, x.name, x.size)

  private def toAirplaneInfo(x: KAirplaneInfo): Airplane =
    Airplane(x.registrationNumber, x.productionLine, x.modelCode)

  private def toAirport(x: KTopAirport): AirportCount = AirportCount(x.code, x.eventCount)

  private def toSpeedFlight(x: KTopSpeed): SpeedFlight = SpeedFlight(x.flightCode, x.speed)

  private def toAirline(x: KTopAirline): AirlineCount = AirlineCount(x.name, x.eventCount)

}
