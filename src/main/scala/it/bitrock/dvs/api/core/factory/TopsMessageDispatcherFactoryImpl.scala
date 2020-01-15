package it.bitrock.dvs.api.core.factory

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.core.dispatcher.TopsMessageDispatcher

class TopsMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaMessageDispatcher: ActorRef
)(implicit system: ActorRefFactory)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef): ActorRef =
    system.actorOf(TopsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

}
