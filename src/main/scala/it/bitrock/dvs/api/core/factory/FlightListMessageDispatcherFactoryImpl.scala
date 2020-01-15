package it.bitrock.dvs.api.core.factory

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.core.dispatcher.FlightListMessageDispatcher

class FlightListMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaMessageDispatcher: ActorRef
)(implicit system: ActorRefFactory)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(FlightListMessageDispatcher.props(sourceActorRef, kafkaMessageDispatcher, websocketConfig))

}
