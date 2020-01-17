package it.bitrock.dvs.api.model

final case class CoordinatesBox(leftHighLat: Double, leftHighLon: Double, rightLowLat: Double, rightLowLon: Double)

final case class GeographyInfo(latitude: Double, longitude: Double, altitude: Double, direction: Double)
final case class AirportInfo(
    codeAirport: String,
    nameAirport: String,
    nameCountry: String,
    codeIso2Country: String,
    timezone: String,
    gmt: String
)
final case class AirlineInfo(codeAirline: String, nameAirline: String, sizeAirline: String)
final case class AirplaneInfo(numberRegistration: String, productionLine: String, modelCode: String)
final case class FlightReceived(
    iataNumber: String,
    icaoNumber: String,
    geography: GeographyInfo,
    speed: Double,
    airportDeparture: AirportInfo,
    airportArrival: AirportInfo,
    airline: AirlineInfo,
    airplane: AirplaneInfo,
    status: String,
    updated: String
)
final case class FlightReceivedList(elements: Seq[FlightReceived])

sealed trait EventPayload

final case class Airport(airportCode: String, eventCount: Long)
final case class TopArrivalAirportList(elements: Seq[Airport] = Nil)   extends EventPayload
final case class TopDepartureAirportList(elements: Seq[Airport] = Nil) extends EventPayload

final case class SpeedFlight(flightCode: String, speed: Double)
final case class TopSpeedList(elements: Seq[SpeedFlight] = Nil) extends EventPayload

final case class Airline(airlineName: String, eventCount: Long)
final case class TopAirlineList(elements: Seq[Airline] = Nil) extends EventPayload

final case class CountFlight(windowStartTime: String, eventCount: Long)  extends EventPayload
final case class CountAirline(windowStartTime: String, eventCount: Long) extends EventPayload

final case class ApiEvent[T <: EventPayload](eventType: String, eventPayload: T)
