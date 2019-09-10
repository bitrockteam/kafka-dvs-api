package it.bitrock.kafkaflightstream.api.core

import akka.actor.ActorRef

trait MessageProcessorFactory {

  def build(sourceActorRef: ActorRef): ActorRef

}
