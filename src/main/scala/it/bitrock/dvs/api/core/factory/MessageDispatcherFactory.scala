package it.bitrock.dvs.api.core.factory

import akka.actor.ActorRef

trait MessageDispatcherFactory {

  def build(sourceActorRef: ActorRef, identifier: String): ActorRef

}
