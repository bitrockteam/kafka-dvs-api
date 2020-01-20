package it.bitrock.dvs.api.core.factory

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.core.dispatcher.{FlightListMessageDispatcher, TopsMessageDispatcher, TotalsMessageDispatcher}

trait MessageDispatcherFactory {
  def build(sourceActorRef: ActorRef): ActorRef
}

object MessageDispatcherFactory {
  def flightListMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, websocketConfig: WebsocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(FlightListMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

  def topsMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, websocketConfig: WebsocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(TopsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

  def totalsMessageDispatcherFactory(kafkaMessageDispatcher: ActorRef, websocketConfig: WebsocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) => system.actorOf(TotalsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))
}
