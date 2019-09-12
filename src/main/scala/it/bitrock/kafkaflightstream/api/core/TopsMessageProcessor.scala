package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import spray.json._

object TopsMessageProcessor {

  def props(
      sourceActorRef: ActorRef,
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
  ): Props =
    Props(new TopsMessageProcessor(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}

class TopsMessageProcessor(
    val sourceActorRef: ActorRef,
    val websocketConfig: WebsocketConfig,
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
) extends MessageProcessor {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(
      self,
      List(kafkaConfig.topArrivalAirportTopic, kafkaConfig.topDepartureAirportTopic, kafkaConfig.topSpeedTopic)
    )

  override def receive: Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")

      kafkaConsumerWrapper.pollMessages()

    case top: TopArrivalAirportList =>
      logger.debug(s"Got a $top from Kafka Consumer")
      forwardMessage(ApiEvent(top.getClass.getSimpleName, top).toJson.toString)

      throttle(kafkaConsumerWrapper.pollMessages())

    case top: TopDepartureAirportList =>
      logger.debug(s"Got a $top from Kafka Consumer")
      forwardMessage(ApiEvent(top.getClass.getSimpleName, top).toJson.toString)

      throttle(kafkaConsumerWrapper.pollMessages())

    case top: TopSpeedList =>
      logger.debug(s"Got a $top from Kafka Consumer")
      forwardMessage(ApiEvent(top.getClass.getSimpleName, top).toJson.toString)

      throttle(kafkaConsumerWrapper.pollMessages())

    case Terminated => self ! PoisonPill
  }

}
