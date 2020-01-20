package it.bitrock.dvs.api.core.factory

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.dispatcher.{FlightListMessageDispatcher, TopsMessageDispatcher, TotalsMessageDispatcher}

trait MessageDispatcherFactory {
  def build(sourceActorRef: ActorRef): ActorRef
}

object MessageDispatcherFactory {
  def flightListMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, webSocketConfig: WebSocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(FlightListMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, webSocketConfig))

  def topsMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, webSocketConfig: WebSocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(TopsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, webSocketConfig))

  def totalsMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, webSocketConfig: WebSocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(TotalsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, webSocketConfig))
}
