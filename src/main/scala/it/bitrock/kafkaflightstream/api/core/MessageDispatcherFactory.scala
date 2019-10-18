package it.bitrock.kafkaflightstream.api.core

import akka.actor.ActorRef

trait MessageDispatcherFactory {

  def build(sourceActorRef: ActorRef, identifier: String): ActorRef

}
