package it.bitrock.kafkaflightstream.api.core.poller

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.KafkaConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper._
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}

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

    case total: CountFlight =>
      logger.debug(s"Got a $total from Kafka Consumer")
      if (total.eventCount > 0) context.become(active(total, airlineCache))
      throttle(kafkaConsumerWrapper.pollMessages())

    case total: CountAirline =>
      logger.debug(s"Got a $total from Kafka Consumer")
      if (total.eventCount > 0) context.become(active(flightCache, total))
      throttle(kafkaConsumerWrapper.pollMessages())

    case TotalFlightUpdate => sender ! flightCache

    case TotalAirlineUpdate => sender ! airlineCache

    case Terminated => self ! PoisonPill
  }

}
