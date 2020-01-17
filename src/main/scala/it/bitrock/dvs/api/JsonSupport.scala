package it.bitrock.dvs.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.dvs.api.model._
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val coordinatesBoxJsonFormat: RootJsonFormat[CoordinatesBox] = jsonFormat4(CoordinatesBox.apply)

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

  implicit def apiEventJsonFormat[T <: EventPayload: JsonFormat]: RootJsonFormat[ApiEvent[T]] = jsonFormat2(ApiEvent.apply[T])

}

object JsonSupport extends JsonSupport
