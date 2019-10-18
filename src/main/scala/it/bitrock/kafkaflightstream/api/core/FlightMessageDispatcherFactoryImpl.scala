package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorSystem}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.poller.FlightPoller
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory

class FlightMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
)(implicit system: ActorSystem)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(FlightPoller.props(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}
