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

  private val cache: FlightReceivedList = FlightReceivedList(Seq())

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(self, List(kafkaConfig.flightReceivedListTopic))

  override def receive: Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")
      kafkaConsumerWrapper.pollMessages()

    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      cache.copy(elements = flights.elements)
      throttle(kafkaConsumerWrapper.pollMessages())

    case UpdateRequested => sender ! cache

    case Terminated => self ! PoisonPill
  }

  def child(props: Props): ActorRef = context.actorOf(props)
}
