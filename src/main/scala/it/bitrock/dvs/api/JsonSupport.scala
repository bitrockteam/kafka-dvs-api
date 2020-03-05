package it.bitrock.dvs.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.bitrock.dvs.api.model._
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val finiteDurationJsonFormat: RootJsonFormat[FiniteDuration] = new RootJsonFormat[FiniteDuration] {
    override def write(obj: FiniteDuration): JsValue = JsNumber(obj.toSeconds)

    override def read(json: JsValue): FiniteDuration = json.convertTo[Int] seconds
  }
  implicit val coordinatesBoxJsonFormat: RootJsonFormat[CoordinatesBox] = jsonFormat5(CoordinatesBox.apply)
  implicit val startTopsJsonFormat: RootJsonFormat[StartTops]           = jsonFormat1(StartTops.apply)
  implicit val startTotalsJsonFormat: RootJsonFormat[StartTotals]       = jsonFormat1(StartTotals.apply)

  implicit val geographyJsonFormat: RootJsonFormat[Geography]                   = jsonFormat4(Geography.apply)
  implicit val airportJsonFormat: RootJsonFormat[Airport]                       = jsonFormat9(Airport.apply)
  implicit val airlineJsonFormat: RootJsonFormat[Airline]                       = jsonFormat3(Airline.apply)
  implicit val airplaneJsonFormat: RootJsonFormat[Airplane]                     = jsonFormat3(Airplane.apply)
  implicit val flightReceivedJsonFormat: RootJsonFormat[FlightReceived]         = jsonFormat10(FlightReceived.apply)
  implicit val flightReceivedListJsonFormat: RootJsonFormat[FlightReceivedList] = jsonFormat1(FlightReceivedList.apply)
  implicit val airportListJsonFormat: RootJsonFormat[AirportList]               = jsonFormat1(AirportList.apply)

  implicit val airportCountJsonFormat: RootJsonFormat[AirportCount] = jsonFormat2(AirportCount.apply)
  implicit val topArrivalAirportListJsonFormat: RootJsonFormat[TopArrivalAirportList] = jsonFormat1(
    TopArrivalAirportList.apply
  )
  implicit val topDepartureAirportJsonFormat: RootJsonFormat[TopDepartureAirportList] = jsonFormat1(
    TopDepartureAirportList.apply
  )
  implicit val speedFlightJsonFormat: RootJsonFormat[SpeedFlight]       = jsonFormat2(SpeedFlight.apply)
  implicit val topSpeedListJsonFormat: RootJsonFormat[TopSpeedList]     = jsonFormat1(TopSpeedList.apply)
  implicit val airlineCountJsonFormat: RootJsonFormat[AirlineCount]     = jsonFormat2(AirlineCount.apply)
  implicit val topAirlineListJsonFormat: RootJsonFormat[TopAirlineList] = jsonFormat1(TopAirlineList.apply)

  implicit val totalFlightsCountStatusJsonFormat: RootJsonFormat[TotalFlightsCount] = jsonFormat2(
    TotalFlightsCount.apply
  )
  implicit val totalAirlinesCountJsonFormat: RootJsonFormat[TotalAirlinesCount] = jsonFormat2(TotalAirlinesCount.apply)

  implicit val topEventPayloadFormat: RootJsonFormat[TopEventPayload] = new RootJsonFormat[TopEventPayload] {
    override def write(eventPayload: TopEventPayload): JsValue =
      eventPayload match {
        case e: TopArrivalAirportList   => e.toJson
        case e: TopDepartureAirportList => e.toJson
        case e: TopSpeedList            => e.toJson
        case e: TopAirlineList          => e.toJson
      }

    override def read(json: JsValue): TopEventPayload =
      Try(json.convertTo[TopArrivalAirportList]).recover { case _ => json.convertTo[TopDepartureAirportList] }.recover {
        case _ => json.convertTo[TopSpeedList]
      }.recover { case _ => json.convertTo[TopAirlineList] }
        .getOrElse(serializationError(s"json serialization error $json"))
  }

  implicit val totalEventPayloadFormat: RootJsonFormat[TotalEventPayload] = new RootJsonFormat[TotalEventPayload] {
    override def write(eventPayload: TotalEventPayload): JsValue =
      eventPayload match {
        case e: TotalFlightsCount  => e.toJson
        case e: TotalAirlinesCount => e.toJson
      }

    override def read(json: JsValue): TotalEventPayload =
      Try(json.convertTo[TotalFlightsCount]).recover { case _ => json.convertTo[TotalAirlinesCount] }
        .getOrElse(serializationError(s"json serialization error $json"))
  }

  implicit def apiEventJsonFormat[T <: EventPayload: JsonFormat]: RootJsonFormat[ApiEvent[T]] =
    jsonFormat2(ApiEvent.apply[T])

  implicit object WebSocketIncomeMessageFormat extends RootJsonReader[WebSocketIncomeMessage] {
    override def read(json: JsValue): WebSocketIncomeMessage =
      json.asJsObject.getFields("@type") match {
        case Seq(JsString("startFlightList")) => json.convertTo[CoordinatesBox]
        case Seq(JsString("stopFlightList"))  => StopFlightList
        case Seq(JsString("startTop"))        => json.convertTo[StartTops]
        case Seq(JsString("stopTop"))         => StopTops
        case Seq(JsString("startTotal"))      => json.convertTo[StartTotals]
        case Seq(JsString("stopTotal"))       => StopTotals
        case unrecognized                     => serializationError(s"json serialization error $unrecognized")
      }
  }
}

object JsonSupport extends JsonSupport
