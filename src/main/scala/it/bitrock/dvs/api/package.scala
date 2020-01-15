package it.bitrock.dvs

import it.bitrock.dvs.api.routes.FlowFactory
import shapeless.tag
import shapeless.tag.@@

import scala.concurrent.duration.FiniteDuration

package object api {

  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

}

package object tags {
  trait FlowFactoryKeyTag
  trait FlowFactoryValueTag

  type FlowFactoryKey   = String @@ FlowFactoryKeyTag
  type FlowFactoryValue = FlowFactory @@ FlowFactoryValueTag

  object TaggedTypes {
    val flightListFlowFactoryKey: FlowFactoryKey = tag[FlowFactoryKeyTag][String]("flight-list")
    val topsFlowFactoryKey: FlowFactoryKey       = tag[FlowFactoryKeyTag][String]("tops")
    val totalsFlowFactoryKey: FlowFactoryKey     = tag[FlowFactoryKeyTag][String]("totals")
  }
}
