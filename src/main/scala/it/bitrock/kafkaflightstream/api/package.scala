package it.bitrock.kafkaflightstream

import it.bitrock.kafkaflightstream.api.routes.FlowFactory

import scala.concurrent.duration.FiniteDuration
import shapeless.tag
import shapeless.tag.@@

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
    val flightFlowFactoryKey: FlowFactoryKey     = tag[FlowFactoryKeyTag][String]("flight")
    val flightListFlowFactoryKey: FlowFactoryKey = tag[FlowFactoryKeyTag][String]("flight-list")
    val topsFlowFactoryKey: FlowFactoryKey       = tag[FlowFactoryKeyTag][String]("tops")
    val totalsFlowFactoryKey: FlowFactoryKey     = tag[FlowFactoryKeyTag][String]("totals")
  }
}
