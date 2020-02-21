package it.bitrock.dvs.api.core.poller

import akka.actor._
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.dvs.api.model._

object TopsKafkaPollerCache {
  def build(kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory)(
      implicit parentSystem: ActorRefFactory
  ): ActorRef = parentSystem.actorOf(Props(new TopsKafkaPollerCache(kafkaConfig, kafkaConsumerWrapperFactory)))
}

class TopsKafkaPollerCache(val kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory)
    extends KafkaPoller {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(
      self,
      List(
        kafkaConfig.topArrivalAirportTopic,
        kafkaConfig.topDepartureAirportTopic,
        kafkaConfig.topSpeedTopic,
        kafkaConfig.topAirlineTopic
      )
    )

  override def receive: Receive = active(
    TopArrivalAirportList(Nil),
    TopDepartureAirportList(Nil),
    TopSpeedList(Nil),
    TopAirlineList(Nil)
  )

  def active(
      arrivalCache: TopArrivalAirportList,
      departureCache: TopDepartureAirportList,
      speedCache: TopSpeedList,
      airlineCache: TopAirlineList
  ): Receive = {

    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")
      schedulePoll()

    case topArrivalAirportList: TopArrivalAirportList =>
      logger.debug(s"Got a $topArrivalAirportList from Kafka Consumer")
      if (topArrivalAirportList.elements.nonEmpty)
        context.become(active(topArrivalAirportList, departureCache, speedCache, airlineCache))
      schedulePoll()

    case topDepartureAirportList: TopDepartureAirportList =>
      logger.debug(s"Got a $topDepartureAirportList from Kafka Consumer")
      if (topDepartureAirportList.elements.nonEmpty)
        context.become(active(arrivalCache, topDepartureAirportList, speedCache, airlineCache))
      schedulePoll()

    case topSpeedList: TopSpeedList =>
      logger.debug(s"Got a $topSpeedList from Kafka Consumer")
      if (topSpeedList.elements.nonEmpty)
        context.become(active(arrivalCache, departureCache, topSpeedList, airlineCache))
      schedulePoll()

    case topAirlineList: TopAirlineList =>
      logger.debug(s"Got a $topAirlineList from Kafka Consumer")
      if (topAirlineList.elements.nonEmpty)
        context.become(active(arrivalCache, departureCache, speedCache, topAirlineList))
      schedulePoll()

    case TopArrivalAirportUpdate   => sender ! arrivalCache
    case TopDepartureAirportUpdate => sender ! departureCache
    case TopSpeedUpdate            => sender ! speedCache
    case TopAirlineUpdate          => sender ! airlineCache

    case Terminated => self ! PoisonPill
  }

}
