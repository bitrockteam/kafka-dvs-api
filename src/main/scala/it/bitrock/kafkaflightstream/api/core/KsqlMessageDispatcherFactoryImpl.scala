package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorSystem}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.poller.KsqlPoller
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapperFactory

class KsqlMessageDispatcherFactoryImpl(
    websocketConfig: WebsocketConfig,
    kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
)(implicit system: ActorSystem)
    extends MessageDispatcherFactory {

  override def build(sourceActorRef: ActorRef, topic: String): ActorRef =
    system.actorOf(KsqlPoller.props(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory, topic))

}
