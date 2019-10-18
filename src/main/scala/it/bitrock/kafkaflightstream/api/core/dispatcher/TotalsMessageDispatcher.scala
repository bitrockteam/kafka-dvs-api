package it.bitrock.kafkaflightstream.api.core.dispatcher

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.{TotalAirlineUpdate, TotalFlightUpdate}
import spray.json._

class TotalsMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaMessageProcessor: ActorRef,
    val websocketConfig: WebsocketConfig
) extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = {

    case Terminated => self ! PoisonPill

    case TotalFlightUpdate =>
      kafkaMessageProcessor ! TotalFlightUpdate
      context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(self ! TotalFlightUpdate)

    case TotalAirlineUpdate =>
      kafkaMessageProcessor ! TotalAirlineUpdate
      context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(self ! TotalAirlineUpdate)

    case totalFlights: CountFlight =>
      logger.debug(s"Got $totalFlights from Kafka Consumer")
      forwardMessage(totalFlights.toJson.toString)

    case totalAirlines: CountAirline =>
      logger.debug(s"Got $totalAirlines from Kafka Consumer")
      forwardMessage(totalAirlines.toJson.toString)

  }

}

object TotalsMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaMessageProcessor: ActorRef,
      websocketConfig: WebsocketConfig
  ): Props =
    Props(new TotalsMessageDispatcher(sourceActorRef, kafkaMessageProcessor, websocketConfig))

}
