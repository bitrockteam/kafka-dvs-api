package it.bitrock.dvs.api.core.poller

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.definitions._
import it.bitrock.dvs.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._

object TotalsKafkaPollerCache {

  def build(kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory)(
      implicit parentSystem: ActorRefFactory
  ): ActorRef = parentSystem.actorOf(Props(new TotalsKafkaPollerCache(kafkaConfig, kafkaConsumerWrapperFactory)))

}

class TotalsKafkaPollerCache(val kafkaConfig: KafkaConfig, kafkaConsumerWrapperFactory: KafkaConsumerWrapperFactory) extends KafkaPoller {

  override val kafkaConsumerWrapper: KafkaConsumerWrapper =
    kafkaConsumerWrapperFactory.build(
      self,
      List(kafkaConfig.totalFlightTopic, kafkaConfig.totalAirlineTopic)
    )

  override def receive: Receive = active(CountFlight("", 0), CountAirline("", 0))

  def active(flightCache: CountFlight, airlineCache: CountAirline): Receive = {
    case NoMessage =>
      logger.debug("Got no-message notice from Kafka Consumer, going to poll again")
      kafkaConsumerWrapper.pollMessages()

    case countFlight: CountFlight =>
      logger.debug(s"Got a $countFlight from Kafka Consumer")
      if (countFlight.eventCount > 0) context.become(active(countFlight, airlineCache))
      throttle(kafkaConsumerWrapper.pollMessages())

    case countAirline: CountAirline =>
      logger.debug(s"Got a $countAirline from Kafka Consumer")
      if (countAirline.eventCount > 0) context.become(active(flightCache, countAirline))
      throttle(kafkaConsumerWrapper.pollMessages())

    case TotalFlightUpdate => sender ! flightCache

    case TotalAirlineUpdate => sender ! airlineCache

    case Terminated => self ! PoisonPill
  }

}
