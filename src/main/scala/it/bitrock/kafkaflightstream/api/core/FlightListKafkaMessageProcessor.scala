package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.KafkaConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.{NoMessage, UpdateRequested}
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}

object FlightListKafkaMessageProcessor {

  def build(kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory)(
      implicit parentSystem: ActorRefFactory
  ): ActorRef =
    parentSystem.actorOf(Props(new FlightListKafkaMessageProcessor(kafkaConfig, kafkaConsumerWrapperFactory)))
}

class FlightListKafkaMessageProcessor(
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
) extends KafkaMessageProcessor {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(self, List(kafkaConfig.flightReceivedListTopic))

  override def receive: Receive = active(FlightReceivedList(Seq()))

  def active(cache: FlightReceivedList): Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")
      kafkaConsumerWrapper.pollMessages()

    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      if (flights.elements.nonEmpty) context.become(active(flights))
      throttle(kafkaConsumerWrapper.pollMessages())

    case UpdateRequested => sender ! cache

    case Terminated => self ! PoisonPill
  }
}
