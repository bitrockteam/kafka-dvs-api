package it.bitrock.kafkaflightstream.api.core.processor

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import spray.json._

object TotalsMessageProcessor {

  def props(
      sourceActorRef: ActorRef,
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
  ): Props =
    Props(new TotalsMessageProcessor(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}

class TotalsMessageProcessor(
    val sourceActorRef: ActorRef,
    val websocketConfig: WebsocketConfig,
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
) extends MessageProcessor
    with KafkaMessageProcessor {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(
      self,
      List(kafkaConfig.totalFlightTopic, kafkaConfig.totalAirlineTopic)
    )

  override def receive: Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")
      kafkaConsumerWrapper.pollMessages()

    case total: CountFlight =>
      logger.debug(s"Got a $total from Kafka Consumer")
      forwardMessage(ApiEvent(total.getClass.getSimpleName, total).toJson.toString)
      throttle(kafkaConsumerWrapper.pollMessages())

    case total: CountAirline =>
      logger.debug(s"Got a $total from Kafka Consumer")
      forwardMessage(ApiEvent(total.getClass.getSimpleName, total).toJson.toString)
      throttle(kafkaConsumerWrapper.pollMessages())

    case Terminated => self ! PoisonPill
  }

}
