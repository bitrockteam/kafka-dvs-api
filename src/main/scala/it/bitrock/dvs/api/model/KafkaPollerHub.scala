package it.bitrock.dvs.api.model

import akka.actor.ActorRef

case class KafkaPollerHub(flightListPoller: ActorRef, topsPoller: ActorRef, totalsPoller: ActorRef)
