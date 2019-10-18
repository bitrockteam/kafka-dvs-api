package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorSystem}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.processor.TopsMessageProcessor
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory

class TopsMessageProcessorFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
)(implicit system: ActorSystem)
    extends MessageProcessorFactory {

  override def build(sourceActorRef: ActorRef, identifier: String = ""): ActorRef =
    system.actorOf(TopsMessageProcessor.props(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}
