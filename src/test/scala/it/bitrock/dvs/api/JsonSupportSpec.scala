package it.bitrock.dvs.api

import it.bitrock.dvs.api.JsonSupport.WebSocketIncomeMessageFormat
import it.bitrock.dvs.api.model._
import spray.json._

class JsonSupportSpec extends BaseSpec {

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

      read(json) { result: StartTop.type =>
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

      read(json) { result: StopTop.type =>
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

      read(json) { result: StartTotal.type =>
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

      read(json) { result: StopTotal.type =>
        result.`@type` shouldBe "stopTotal"
      }
    }
  }

  private def read[A <: WebSocketIncomeMessage](message: String)(test: A => Any): Any =
    test(WebSocketIncomeMessageFormat.read(message.parseJson).asInstanceOf[A])
}
