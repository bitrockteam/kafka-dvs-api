package it.bitrock.dvs.api.core.factory

import java.util.UUID

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.dispatcher.GlobalMessageDispatcher
import it.bitrock.dvs.api.model.KafkaPollerHub

trait MessageDispatcherFactory {
  def build(sourceActorRef: ActorRef): ActorRef
}

object MessageDispatcherFactory {

  def globalMessageDispatcherFactory(kafkaMessageDispatcherHub: KafkaPollerHub, webSocketConfig: WebSocketConfig)(
      implicit system: ActorRefFactory
  ): MessageDispatcherFactory =
    (sourceActorRef: ActorRef) =>
      system.actorOf(
        GlobalMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcherHub, webSocketConfig),
        s"global-message-dispatcher-${UUID.randomUUID().toString}"
      )

}
