package it.bitrock.kafkaflightstream.api.definitions

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.kafkaflightstream.model.{
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
import spray.json._

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

final case class KsqlStreamDataResponse(data: String)

final case class ApiEvent[T <: EventPayload](eventType: String, eventPayload: T)

object DefinitionsConversions {

  def toGeographyInfo(x: KGeographyInfo): GeographyInfo =
    GeographyInfo(x.latitude, x.longitude, x.altitude, x.direction)

  def toAirportInfo(x: KAirportInfo): AirportInfo =
    AirportInfo(x.codeAirport, x.nameAirport, x.nameCountry, x.codeIso2Country, x.timezone, x.gmt)

  def toAirlineInfo(x: KAirlineInfo): AirlineInfo =
    AirlineInfo(x.codeAirline, x.nameAirline, x.sizeAirline)

  def toAirplaneInfo(x: KAirplaneInfo): AirplaneInfo =
    AirplaneInfo(x.numberRegistration, x.productionLine, x.modelCode)

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

  def toAirport(x: KAirport) = Airport(x.airportCode, x.eventCount)

  implicit class TopArrivalAirportListOps(x: KTopArrivalAirportList) {
    def toTopArrivalAirportList = TopArrivalAirportList(x.elements.map(toAirport))
  }

  implicit class TopDepartureAirportListOps(x: KTopDepartureAirportList) {
    def toTopDepartureAirportList = TopDepartureAirportList(x.elements.map(toAirport))
  }

  def toSpeedFlight(x: KSpeedFlight) = SpeedFlight(x.flightCode, x.speed)

  implicit class TopSpeedListOps(x: KTopSpeedList) {
    def toTopSpeedList = TopSpeedList(x.elements.map(toSpeedFlight))
  }

  def toAirline(x: KAirline) = Airline(x.airlineName, x.eventCount)

  implicit class TopAirlineListOps(x: KTopAirlineList) {
    def toTopAirlineList = TopAirlineList(x.elements.map(toAirline))
  }

  implicit class CountFlightOps(x: KCountFlight) {
    def toCountFlight = CountFlight(x.windowStartTime, x.eventCount)
  }

  implicit class CountAirlineOps(x: KCountAirline) {
    def toCountAirline = CountAirline(x.windowStartTime, x.eventCount)
  }

}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val geographyInfoJsonFormat: RootJsonFormat[GeographyInfo]           = jsonFormat4(GeographyInfo.apply)
  implicit val airportInfoJsonFormat: RootJsonFormat[AirportInfo]               = jsonFormat6(AirportInfo.apply)
  implicit val airlineInfoJsonFormat: RootJsonFormat[AirlineInfo]               = jsonFormat3(AirlineInfo.apply)
  implicit val airplaneInfoInfoJsonFormat: RootJsonFormat[AirplaneInfo]         = jsonFormat3(AirplaneInfo.apply)
  implicit val flightReceivedJsonFormat: RootJsonFormat[FlightReceived]         = jsonFormat10(FlightReceived.apply)
  implicit val flightReceivedListJsonFormat: RootJsonFormat[FlightReceivedList] = jsonFormat1(FlightReceivedList.apply)

  implicit val airportJsonFormat: RootJsonFormat[Airport]                             = jsonFormat2(Airport.apply)
  implicit val topArrivalAirportListJsonFormat: RootJsonFormat[TopArrivalAirportList] = jsonFormat1(TopArrivalAirportList.apply)
  implicit val topDepartureAirportJsonFormat: RootJsonFormat[TopDepartureAirportList] = jsonFormat1(TopDepartureAirportList.apply)
  implicit val speedFlghtJsonFormat: RootJsonFormat[SpeedFlight]                      = jsonFormat2(SpeedFlight.apply)
  implicit val topSpeedListJsonFormat: RootJsonFormat[TopSpeedList]                   = jsonFormat1(TopSpeedList.apply)
  implicit val airlineJsonFormat: RootJsonFormat[Airline]                             = jsonFormat2(Airline.apply)
  implicit val topAirlineListJsonFormat: RootJsonFormat[TopAirlineList]               = jsonFormat1(TopAirlineList.apply)

  implicit val countFlightStatusJsonFormat: RootJsonFormat[CountFlight] = jsonFormat2(CountFlight.apply)
  implicit val countAirlineJsonFormat: RootJsonFormat[CountAirline]     = jsonFormat2(CountAirline.apply)

  implicit val ksqlStreamDataResponseFormat: RootJsonFormat[KsqlStreamDataResponse] = jsonFormat1(KsqlStreamDataResponse)

  implicit def apiEventJsonFormat[T <: EventPayload: JsonFormat]: RootJsonFormat[ApiEvent[T]] = jsonFormat2(ApiEvent.apply[T])

}
