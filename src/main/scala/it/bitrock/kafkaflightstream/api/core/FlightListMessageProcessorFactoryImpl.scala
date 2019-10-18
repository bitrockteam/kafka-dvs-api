package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorRefFactory}
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.core.processor.FlightListMessageProcessor

class FlightListMessageProcessorFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaMessageProcessor: ActorRef
)(implicit system: ActorRefFactory)
    extends MessageProcessorFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(FlightListMessageProcessor.props(sourceActorRef, kafkaMessageProcessor, websocketConfig))

}
