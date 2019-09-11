package it.bitrock.kafkaflightstream.api.definitions

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.kafkaflightstream.model.{
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  Airport => KAirport,
  AirportInfo => KAirportInfo,
  FlightEnrichedEvent => KFlightEnrichedEvent,
  GeographyInfo => KGeographyInfo,
  TopArrivalAirportList => KTopArrivalAirportList,
  TopDepartureAirportList => KTopDepartureAirportList
}
import spray.json._

final case class GeographyInfo(latitude: Double, longitude: Double, altitude: Double, direction: Double)
final case class AirportInfo(codeAirport: String, nameAirport: String, nameCountry: String, codeIso2Country: String)
final case class AirlineInfo(nameAirline: String, sizeAirline: String)
final case class AirplaneInfo(productionLine: String, modelCode: String)
final case class FlightReceived(
    geography: GeographyInfo,
    speed: Double,
    airportDeparture: AirportInfo,
    airportArrival: AirportInfo,
    airline: AirlineInfo,
    airplane: Option[AirplaneInfo],
    status: String
)

sealed trait EventPayload
final case class Airport(airportCode: String, eventCount: Long)
final case class TopArrivalAirportList(elements: Seq[Airport] = Nil)   extends EventPayload
final case class TopDepartureAirportList(elements: Seq[Airport] = Nil) extends EventPayload
final case class ApiEvent[T <: EventPayload](eventType: String, eventPayload: T)

object DefinitionsConversions {

  def toGeographyInfo(x: KGeographyInfo): GeographyInfo =
    GeographyInfo(x.latitude, x.longitude, x.altitude, x.direction)

  def toAirportInfo(x: KAirportInfo): AirportInfo =
    AirportInfo(x.codeAirport, x.nameAirport, x.nameCountry, x.codeIso2Country)

  def toAirlineInfo(x: KAirlineInfo): AirlineInfo =
    AirlineInfo(x.nameAirline, x.sizeAirline)

  def toAirplaneInfo(x: KAirplaneInfo): AirplaneInfo =
    AirplaneInfo(x.productionLine, x.modelCode)

  implicit class FlightReceivedOps(x: KFlightEnrichedEvent) {
    def toFlightReceived: FlightReceived =
      FlightReceived(
        toGeographyInfo(x.geography),
        x.speed,
        toAirportInfo(x.airportDeparture),
        toAirportInfo(x.airportArrival),
        toAirlineInfo(x.airline),
        x.airplane.map(toAirplaneInfo),
        x.status
      )
  }

  def toAirport(kAirport: KAirport) = Airport(kAirport.airportCode, kAirport.eventCount)

  implicit class TopArrivalAirportListOps(kArrivalAirport: KTopArrivalAirportList) {
    def toTopArrivalAirportList = TopArrivalAirportList(kArrivalAirport.elements.map(toAirport))
  }

  implicit class TopDepartureAirportListOps(kDepartureAirport: KTopDepartureAirportList) {
    def toTopDepartureAirportList = TopDepartureAirportList(kDepartureAirport.elements.map(toAirport))
  }

}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val geographyInfoJsonFormat: RootJsonFormat[GeographyInfo]   = jsonFormat4(GeographyInfo.apply)
  implicit val airportInfoJsonFormat: RootJsonFormat[AirportInfo]       = jsonFormat4(AirportInfo.apply)
  implicit val airlineInfoJsonFormat: RootJsonFormat[AirlineInfo]       = jsonFormat2(AirlineInfo.apply)
  implicit val airplaneInfoInfoJsonFormat: RootJsonFormat[AirplaneInfo] = jsonFormat2(AirplaneInfo.apply)
  implicit val flightReceivedJsonFormat: RootJsonFormat[FlightReceived] = jsonFormat7(FlightReceived.apply)

  implicit val airportJsonFormat: RootJsonFormat[Airport]                             = jsonFormat2(Airport.apply)
  implicit val topArrivalAirportListJsonFormat: RootJsonFormat[TopArrivalAirportList] = jsonFormat1(TopArrivalAirportList.apply)
  implicit val topDepartureAirportJsonFormat: RootJsonFormat[TopDepartureAirportList] = jsonFormat1(TopDepartureAirportList.apply)

  implicit def apiEventJsonFormat[T <: EventPayload: JsonFormat]: RootJsonFormat[ApiEvent[T]] = jsonFormat2(ApiEvent.apply[T])

}
