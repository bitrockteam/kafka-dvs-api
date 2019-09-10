package it.bitrock.kafkaflightstream.api.definitions

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.kafkaflightstream.model.{
  GeographyInfo => KGeographyInfo,
  AirportInfo => KAirportInfo,
  AirlineInfo => KAirlineInfo,
  AirplaneInfo => KAirplaneInfo,
  FlightEnrichedEvent => KFlightEnrichedEvent
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

}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val geographyInfoJsonFormat: RootJsonFormat[GeographyInfo]   = jsonFormat4(GeographyInfo.apply)
  implicit val airportInfoJsonFormat: RootJsonFormat[AirportInfo]       = jsonFormat4(AirportInfo.apply)
  implicit val airlineInfoJsonFormat: RootJsonFormat[AirlineInfo]       = jsonFormat2(AirlineInfo.apply)
  implicit val airplaneInfoInfoJsonFormat: RootJsonFormat[AirplaneInfo] = jsonFormat2(AirplaneInfo.apply)
  implicit val flightReceivedJsonFormat: RootJsonFormat[FlightReceived] = jsonFormat7(FlightReceived.apply)
}
