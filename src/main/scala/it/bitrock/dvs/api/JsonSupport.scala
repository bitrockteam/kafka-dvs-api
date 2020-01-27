package it.bitrock.dvs.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.dvs.api.model._
import spray.json.{serializationError, DefaultJsonProtocol, JsString, JsValue, JsonFormat, RootJsonFormat, RootJsonReader}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val coordinatesBoxJsonFormat: RootJsonFormat[CoordinatesBox] = jsonFormat4(CoordinatesBox.apply)

  implicit val geographyJsonFormat: RootJsonFormat[Geography]                   = jsonFormat4(Geography.apply)
  implicit val airportJsonFormat: RootJsonFormat[Airport]                       = jsonFormat6(Airport.apply)
  implicit val airlineJsonFormat: RootJsonFormat[Airline]                       = jsonFormat3(Airline.apply)
  implicit val airplaneJsonFormat: RootJsonFormat[Airplane]                     = jsonFormat3(Airplane.apply)
  implicit val flightReceivedJsonFormat: RootJsonFormat[FlightReceived]         = jsonFormat10(FlightReceived.apply)
  implicit val flightReceivedListJsonFormat: RootJsonFormat[FlightReceivedList] = jsonFormat1(FlightReceivedList.apply)

  implicit val airportCountJsonFormat: RootJsonFormat[AirportCount] = jsonFormat2(AirportCount.apply)
  implicit val topArrivalAirportListJsonFormat: RootJsonFormat[TopArrivalAirportList] = jsonFormat1(
    TopArrivalAirportList.apply
  )
  implicit val topDepartureAirportJsonFormat: RootJsonFormat[TopDepartureAirportList] = jsonFormat1(
    TopDepartureAirportList.apply
  )
  implicit val speedFlghtJsonFormat: RootJsonFormat[SpeedFlight]        = jsonFormat2(SpeedFlight.apply)
  implicit val topSpeedListJsonFormat: RootJsonFormat[TopSpeedList]     = jsonFormat1(TopSpeedList.apply)
  implicit val airlineCountJsonFormat: RootJsonFormat[AirlineCount]     = jsonFormat2(AirlineCount.apply)
  implicit val topAirlineListJsonFormat: RootJsonFormat[TopAirlineList] = jsonFormat1(TopAirlineList.apply)

  implicit val totalFlightsCountStatusJsonFormat: RootJsonFormat[TotalFlightsCount] = jsonFormat2(
    TotalFlightsCount.apply
  )
  implicit val totalAirlinesCountJsonFormat: RootJsonFormat[TotalAirlinesCount] = jsonFormat2(TotalAirlinesCount.apply)

  implicit def apiEventJsonFormat[T <: EventPayload: JsonFormat]: RootJsonFormat[ApiEvent[T]] =
    jsonFormat2(ApiEvent.apply[T])

  implicit object WebSocketIncomeMessageFormat extends RootJsonReader[WebSocketIncomeMessage] {
    override def read(json: JsValue): WebSocketIncomeMessage =
      json.asJsObject.getFields("@type") match {
        case Seq(JsString("startFlightList")) => json.convertTo[CoordinatesBox]
        case Seq(JsString("stopFlightList"))  => StopFlightList
        case Seq(JsString("startTop"))        => StartTop
        case Seq(JsString("stopTop"))         => StopTop
        case Seq(JsString("startTotal"))      => StartTotal
        case Seq(JsString("stopTotal"))       => StopTotal
        case unrecognized                     => serializationError(s"json serialization error $unrecognized")
      }
  }
}

object JsonSupport extends JsonSupport
