package it.bitrock.dvs.api.model

sealed abstract class WebSocketIncomeMessage(val `@type`: String)
final case class CoordinatesBox(leftHighLat: Double, leftHighLon: Double, rightLowLat: Double, rightLowLon: Double)
    extends WebSocketIncomeMessage("startFlightList")
case object StopFlightList extends WebSocketIncomeMessage("stopFlightList")
case object StartTop       extends WebSocketIncomeMessage("startTop")
case object StopTop        extends WebSocketIncomeMessage("stopTop")
case object StartTotal     extends WebSocketIncomeMessage("startTotal")
case object StopTotal      extends WebSocketIncomeMessage("stopTotal")

final case class Geography(latitude: Double, longitude: Double, altitude: Double, direction: Double)
final case class Airport(
    codeAirport: String,
    nameAirport: String,
    nameCountry: String,
    codeIso2Country: String,
    timezone: String,
    gmt: String
)
final case class Airline(codeAirline: String, nameAirline: String, sizeAirline: Long)
final case class Airplane(numberRegistration: String, productionLine: String, modelCode: String)
final case class FlightReceived(
    iataNumber: String,
    icaoNumber: String,
    geography: Geography,
    speed: Double,
    airportDeparture: Airport,
    airportArrival: Airport,
    airline: Airline,
    airplane: Airplane,
    status: String,
    updated: Long
)
sealed trait EventPayload

final case class FlightReceivedList(elements: Seq[FlightReceived]) extends EventPayload

final case class AirportCount(airportCode: String, eventCount: Long)
final case class TopArrivalAirportList(elements: List[AirportCount])   extends EventPayload
final case class TopDepartureAirportList(elements: List[AirportCount]) extends EventPayload

final case class SpeedFlight(flightCode: String, speed: Double)
final case class TopSpeedList(elements: List[SpeedFlight]) extends EventPayload

final case class AirlineCount(airlineName: String, eventCount: Long)
final case class TopAirlineList(elements: List[AirlineCount]) extends EventPayload

final case class TotalFlightsCount(windowStartTime: String, eventCount: Long)  extends EventPayload
final case class TotalAirlinesCount(windowStartTime: String, eventCount: Long) extends EventPayload

final case class ApiEvent[T <: EventPayload](eventType: String, eventPayload: T)

object EventType {

  def from(eventPayload: EventPayload): String =
    eventPayload match {
      case _: TopArrivalAirportList   => "TopArrivalAirportList"
      case _: TopDepartureAirportList => "TopDepartureAirportList"
      case _: TopSpeedList            => "TopSpeedList"
      case _: TopAirlineList          => "TopAirlineList"
      case _: TotalFlightsCount       => "TotalFlightsCount"
      case _: TotalAirlinesCount      => "TotalAirlinesCount"
      case _: FlightReceivedList      => "FlightList"
    }
}
