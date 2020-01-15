package it.bitrock.dvs.api.core.factory

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.core.dispatcher.TotalsMessageDispatcher

class TotalsMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaMessageDispatcher: ActorRef
)(implicit system: ActorRefFactory)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef): ActorRef =
    system.actorOf(TotalsMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

}
