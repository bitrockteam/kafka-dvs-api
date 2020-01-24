package it.bitrock.dvs.api.core.poller

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{FlightListUpdate, NoMessage}
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.dvs.api.model._

object FlightListKafkaPollerCache {

  def build(kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory)(
      implicit parentSystem: ActorRefFactory
  ): ActorRef =
    parentSystem.actorOf(Props(new FlightListKafkaPollerCache(kafkaConfig, kafkaConsumerWrapperFactory)))
}

class FlightListKafkaPollerCache(
    val kafkaConfig: KafkaConfig,
    kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory
) extends KafkaPoller {

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

    case FlightListUpdate => sender ! cache

    case Terminated => self ! PoisonPill
  }
}
