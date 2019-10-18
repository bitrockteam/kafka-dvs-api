package it.bitrock.kafkaflightstream.api.core.processor

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.definitions.KsqlStreamDataResponse
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import spray.json._

object KsqlMessageProcessor {

  def props(
      sourceActorRef: ActorRef,
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory,
      topic: String
  ): Props =
    Props(new KsqlMessageProcessor(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory, topic))

}

class KsqlMessageProcessor(
    val sourceActorRef: ActorRef,
    val websocketConfig: WebsocketConfig,
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory,
    topic: String
) extends MessageProcessor
    with KafkaMessageProcessor {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper = kafkaConsumerWrapperFactory.build(self, List(topic))

  override def receive: Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")

      kafkaConsumerWrapper.pollMessages()

    case obj: KsqlStreamDataResponse =>
      logger.debug(s"Got a $obj from Kafka Consumer")
      forwardMessage(obj.toJson.toString)

      throttle(kafkaConsumerWrapper.pollMessages())

    case Terminated => self ! PoisonPill
  }

}
