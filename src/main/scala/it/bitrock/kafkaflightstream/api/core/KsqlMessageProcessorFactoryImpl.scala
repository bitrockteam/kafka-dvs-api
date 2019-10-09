package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorSystem}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory

class KsqlMessageProcessorFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
)(implicit system: ActorSystem)
    extends MessageProcessorFactory {

  override def build(sourceActorRef: ActorRef, topic: String): ActorRef =
    system.actorOf(KsqlMessageProcessor.props(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory, topic))

}
