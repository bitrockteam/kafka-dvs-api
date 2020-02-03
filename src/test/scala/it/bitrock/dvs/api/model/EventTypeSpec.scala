package it.bitrock.dvs.api.model

import org.scalatest.ParallelTestExecution
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EventTypeSpec extends AnyWordSpec with Matchers with ParallelTestExecution {

  "EventTypeSpec" should {

    "crete the right event type" when {

      "event payload is TopArrivalAirportList" in {
        EventType.from(TopArrivalAirportList(Nil)) shouldBe "TopArrivalAirportList"
      }

      "event payload is TopDepartureAirportList" in {
        EventType.from(TopDepartureAirportList(Nil)) shouldBe "TopDepartureAirportList"
      }

      "event payload is TopSpeedList" in {
        EventType.from(TopSpeedList(Nil)) shouldBe "TopSpeedList"
      }

      "event payload is TopAirlineList" in {
        EventType.from(TopAirlineList(Nil)) shouldBe "TopAirlineList"
      }

      "event payload is TotalFlightsCount" in {
        EventType.from(TotalFlightsCount("startTime", 123)) shouldBe "TotalFlightsCount"
      }

      "event payload is TotalAirlinesCount" in {
        EventType.from(TotalAirlinesCount("startTime", 123)) shouldBe "TotalAirlinesCount"
      }

      "event payload is FlightReceivedList" in {
        EventType.from(FlightReceivedList(Nil)) shouldBe "FlightList"
      }
    }
  }
}
