package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.core.dispatcher.TopsMessageDispatcher

class TopsMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaMessageDispatcher: ActorRef
)(implicit system: ActorRefFactory)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(TopsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

}
