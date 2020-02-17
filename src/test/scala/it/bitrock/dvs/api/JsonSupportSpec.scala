package it.bitrock.dvs.api

import it.bitrock.dvs.api.JsonSupport.WebSocketIncomeMessageFormat
import it.bitrock.dvs.api.TestValues._
import it.bitrock.dvs.api.model._
import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.Assertion
import spray.json._

import scala.reflect.ClassTag

class JsonSupportSpec extends BaseSpec {

  "eventPayloadWriter" should {

    "parse FlightReceivedList" in {
      writeReadEquals[FlightReceivedList]
    }

    "parse TopArrivalAirportList" in {
      writeReadEquals[TopArrivalAirportList]
    }

    "parse TopDepartureAirportList" in {
      writeReadEquals[TopDepartureAirportList]
    }

    "parse TopSpeedList" in {
      writeReadEquals[TopSpeedList]
    }

    "parse TopAirlineList" in {
      writeReadEquals[TopAirlineList]
    }

    "parse TotalFlightsCount" in {
      writeReadEquals[TotalFlightsCount]
    }

    "parse TotalAirlinesCount" in {
      writeReadEquals[TotalAirlinesCount]
    }
  }

  "WebSocketIncomeMessageFormat" should {
    "parse CoordinatesBox" in {
      read(coordinatesBox) { result: CoordinatesBox =>
        result.`@type` shouldBe "startFlightList"
        result.leftHighLat shouldBe 23.6
        result.leftHighLon shouldBe 67.9
        result.rightLowLat shouldBe 37.98
        result.rightLowLon shouldBe 43.45
      }
    }

    "parse StopFlightList" in {
      read(stopFlightList) { result: StopFlightList.type => result.`@type` shouldBe "stopFlightList" }
    }

    "parse StartTop" in {
      read(startTop) { result: StartTops.type => result.`@type` shouldBe "startTop" }
    }

    "parse StopTop" in {
      read(stopTop) { result: StopTops.type => result.`@type` shouldBe "stopTop" }
    }

    "parse StartTotal" in {
      read(startTotal) { result: StartTotals.type => result.`@type` shouldBe "startTotal" }
    }

    "parse StopTotal" in {
      read(stopTotal) { result: StopTotals.type => result.`@type` shouldBe "stopTotal" }
    }
  }

  private def read[A <: WebSocketIncomeMessage](message: String)(test: A => Any): Any =
    test(WebSocketIncomeMessageFormat.read(message.parseJson).asInstanceOf[A])

  private def writeReadEquals[A <: EventPayload: ClassTag](implicit value: Arbitrary[A]): Assertion =
    value.arbitrary
      .flatMap(v => JsonSupport.eventPayloadWriter.read(JsonSupport.eventPayloadWriter.write(v)) should not be a[Exception])
      .sample
      .get
}
