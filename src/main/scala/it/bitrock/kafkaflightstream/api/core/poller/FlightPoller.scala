package it.bitrock.kafkaflightstream.api.core.poller

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.core.dispatcher.MessageDispatcher
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.NoMessage
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import spray.json._

object FlightPoller {

  def props(
      sourceActorRef: ActorRef,
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
  ): Props =
    Props(new FlightPoller(sourceActorRef, websocketConfig, kafkaConfig, kafkaConsumerWrapperFactory))

}

class FlightPoller(
    val sourceActorRef: ActorRef,
    val websocketConfig: WebsocketConfig,
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
) extends MessageDispatcher
    with KafkaPoller {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper = kafkaConsumerWrapperFactory.build(self, List(kafkaConfig.flightReceivedTopic))

  override def receive: Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")

      kafkaConsumerWrapper.pollMessages()

    case flight: FlightReceived =>
      logger.debug(s"Got a $flight from Kafka Consumer")
      forwardMessage(flight.toJson.toString)

      throttle(kafkaConsumerWrapper.pollMessages())

    case Terminated => self ! PoisonPill
  }

}