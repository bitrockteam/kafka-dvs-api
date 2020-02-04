package it.bitrock.dvs.api

import it.bitrock.dvs.api.JsonSupport.WebSocketIncomeMessageFormat
import it.bitrock.dvs.api.model._
import org.scalacheck.{Arbitrary, Gen}
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
      val json =
        """
          | {
          |   "@type": "startFlightList",
          |   "leftHighLat": 23.6,
          |   "leftHighLon": 67.9,
          |   "rightLowLat": 37.98,
          |   "rightLowLon": 43.45
          | }
          |""".stripMargin

      read(json) { result: CoordinatesBox =>
        result.`@type` shouldBe "startFlightList"
        result.leftHighLat shouldBe 23.6
        result.leftHighLon shouldBe 67.9
        result.rightLowLat shouldBe 37.98
        result.rightLowLon shouldBe 43.45
      }
    }

    "parse StopFlightList" in {
      val json =
        """
          | {
          |   "@type": "stopFlightList"
          | }
          |""".stripMargin

      read(json) { result: StopFlightList.type =>
        result.`@type` shouldBe "stopFlightList"
      }
    }

    "parse StartTop" in {
      val json =
        """
          | {
          |   "@type": "startTop"
          | }
          |""".stripMargin

      read(json) { result: StartTops.type =>
        result.`@type` shouldBe "startTop"
      }
    }

    "parse StopTop" in {
      val json =
        """
          | {
          |   "@type": "stopTop"
          | }
          |""".stripMargin

      read(json) { result: StopTops.type =>
        result.`@type` shouldBe "stopTop"
      }
    }

    "parse StartTotal" in {
      val json =
        """
          | {
          |   "@type": "startTotal"
          | }
          |""".stripMargin

      read(json) { result: StartTotals.type =>
        result.`@type` shouldBe "startTotal"
      }
    }

    "parse StopTotal" in {
      val json =
        """
          | {
          |   "@type": "stopTotal"
          | }
          |""".stripMargin

      read(json) { result: StopTotals.type =>
        result.`@type` shouldBe "stopTotal"
      }
    }
  }

  private def read[A <: WebSocketIncomeMessage](message: String)(test: A => Any): Any =
    test(WebSocketIncomeMessageFormat.read(message.parseJson).asInstanceOf[A])

  private def writeReadEquals[A <: EventPayload: ClassTag](implicit value: Arbitrary[A]): Gen[Assertion] =
    value.arbitrary.flatMap(v => JsonSupport.eventPayloadWriter.read(JsonSupport.eventPayloadWriter.write(v)) shouldBe a[A])
}
