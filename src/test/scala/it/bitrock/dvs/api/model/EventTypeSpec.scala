package it.bitrock.dvs.api.model

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EventTypeSpec extends AnyWordSpec with Matchers {

  "EventTypeSpec" should {

    "crete the right event type" when {

      "event payload is TopArrivalAirportList" in {
        EventType.from(TopArrivalAirportList(List.empty)) shouldBe "TopArrivalAirportList"
      }

      "event payload is TopDepartureAirportList" in {
        EventType.from(TopDepartureAirportList(List.empty)) shouldBe "TopDepartureAirportList"

      }

      "event payload is TopSpeedList" in {
        EventType.from(TopSpeedList(List.empty)) shouldBe "TopSpeedList"

      }

      "event payload is TopAirlineList" in {
        EventType.from(TopAirlineList(List.empty)) shouldBe "TopAirlineList"

      }

      "event payload is TotalFlightsCount" in {
        EventType.from(TotalFlightsCount("startTime", 123)) shouldBe "TotalFlightsCount"

      }

      "event payload is TotalAirlinesCount" in {
        EventType.from(TotalAirlinesCount("startTime", 123)) shouldBe "TotalAirlinesCount"

      }
    }
  }
}
