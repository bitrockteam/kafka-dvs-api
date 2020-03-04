package it.bitrock.dvs.api.core.poller

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{AirportListUpdate, FlightListUpdate}
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
    kafkaConsumerWrapperFactory.build(self, List(kafkaConfig.flightInterpolatedListTopic))

  override def receive: Receive = active(FlightReceivedList(List()))

  def active(cache: FlightReceivedList): Receive = {
    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      if (flights.elements.nonEmpty) context.become(active(flights))

    case FlightListUpdate => sender ! cache

    case AirportListUpdate => sender ! AirportList.from(cache)

    case Terminated => self ! PoisonPill
  }

}
