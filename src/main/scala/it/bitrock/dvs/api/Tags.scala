package it.bitrock.dvs.api

import shapeless.tag
import shapeless.tag.@@

object Tags {
  trait FlowFactoryKeyTag
  trait FlowFactoryValueTag

  type FlowFactoryKey = String @@ FlowFactoryKeyTag

  object TaggedTypes {
    val flightListFlowFactoryKey: FlowFactoryKey = tag[FlowFactoryKeyTag][String]("flight-list")
    val topsFlowFactoryKey: FlowFactoryKey       = tag[FlowFactoryKeyTag][String]("tops")
    val totalsFlowFactoryKey: FlowFactoryKey     = tag[FlowFactoryKeyTag][String]("totals")
  }
}
