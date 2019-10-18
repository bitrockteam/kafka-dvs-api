package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorSystem}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.poller.TopsPoller
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory

class TopsMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
)(implicit system: ActorSystem)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(TopsPoller.props(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}
