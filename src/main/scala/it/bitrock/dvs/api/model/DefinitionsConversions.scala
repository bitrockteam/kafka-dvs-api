package it.bitrock.dvs.api.model

import it.bitrock.dvs.model.avro.{
  Airline => KAirline,
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  Airport => KAirport,
  AirportInfo => KAirportInfo,
  CountAirline => KCountAirline,
  CountFlight => KCountFlight,
  FlightReceived => KFlightReceivedEvent,
  FlightReceivedList => KFlightReceivedListEvent,
  GeographyInfo => KGeographyInfo,
  SpeedFlight => KSpeedFlight,
  TopAirlineList => KTopAirlineList,
  TopArrivalAirportList => KTopArrivalAirportList,
  TopDepartureAirportList => KTopDepartureAirportList,
  TopSpeedList => KTopSpeedList
}

object DefinitionsConversions {

  implicit class TopAirlineListOps(x: KTopAirlineList) {
    def toTopAirlineList: TopAirlineList = TopAirlineList(x.elements.map(toAirline))
  }

  implicit class CountFlightOps(x: KCountFlight) {
    def toCountFlight: CountFlight = CountFlight(x.windowStartTime, x.eventCount)
  }

  implicit class CountAirlineOps(x: KCountAirline) {
    def toCountAirline: CountAirline = CountAirline(x.windowStartTime, x.eventCount)
  }

  implicit class FlightReceivedOps(x: KFlightReceivedEvent) {
    def toFlightReceived: FlightReceived =
      FlightReceived(
        x.iataNumber,
        x.icaoNumber,
        toGeographyInfo(x.geography),
        x.speed,
        toAirportInfo(x.airportDeparture),
        toAirportInfo(x.airportArrival),
        toAirlineInfo(x.airline),
        toAirplaneInfo(x.airplane),
        x.status,
        x.updated
      )
  }

  implicit class FlightReceivedListOps(x: KFlightReceivedListEvent) {
    def toFlightReceivedList: FlightReceivedList =
      FlightReceivedList(x.elements.map(_.toFlightReceived))
  }

  implicit class TopArrivalAirportListOps(x: KTopArrivalAirportList) {
    def toTopArrivalAirportList: TopArrivalAirportList = TopArrivalAirportList(x.elements.map(toAirport))
  }

  implicit class TopDepartureAirportListOps(x: KTopDepartureAirportList) {
    def toTopDepartureAirportList: TopDepartureAirportList = TopDepartureAirportList(x.elements.map(toAirport))
  }

  implicit class TopSpeedListOps(x: KTopSpeedList) {
    def toTopSpeedList: TopSpeedList = TopSpeedList(x.elements.map(toSpeedFlight))
  }

  private def toGeographyInfo(x: KGeographyInfo): GeographyInfo =
    GeographyInfo(x.latitude, x.longitude, x.altitude, x.direction)

  private def toAirportInfo(x: KAirportInfo): AirportInfo =
    AirportInfo(x.codeAirport, x.nameAirport, x.nameCountry, x.codeIso2Country, x.timezone, x.gmt)

  private def toAirlineInfo(x: KAirlineInfo): AirlineInfo =
    AirlineInfo(x.codeAirline, x.nameAirline, x.sizeAirline)

  private def toAirplaneInfo(x: KAirplaneInfo): AirplaneInfo =
    AirplaneInfo(x.numberRegistration, x.productionLine, x.modelCode)

  private def toAirport(x: KAirport): Airport = Airport(x.airportCode, x.eventCount)

  private def toSpeedFlight(x: KSpeedFlight): SpeedFlight = SpeedFlight(x.flightCode, x.speed)

  private def toAirline(x: KAirline): Airline = Airline(x.airlineName, x.eventCount)

}
