package it.bitrock.dvs.api.core.dispatcher

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.config.WebsocketConfig
import it.bitrock.dvs.api.model._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.{TotalAirlineUpdate, TotalFlightUpdate}
import spray.json._
import it.bitrock.dvs.api.ActorSystemOps

class TotalsMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaPoller: ActorRef,
    val websocketConfig: WebsocketConfig
) extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = {

    case Terminated => self ! PoisonPill

    case TotalFlightUpdate =>
      kafkaPoller ! TotalFlightUpdate
      context.system.scheduleOnce(websocketConfig.throttleDuration)(self ! TotalFlightUpdate)

    case TotalAirlineUpdate =>
      kafkaPoller ! TotalAirlineUpdate
      context.system.scheduleOnce(websocketConfig.throttleDuration)(self ! TotalAirlineUpdate)

    case totalFlights: CountFlight =>
      logger.debug(s"Got $totalFlights from Kafka Consumer")
      forwardMessage(ApiEvent(totalFlights.getClass.getSimpleName, totalFlights).toJson.toString)

    case totalAirlines: CountAirline =>
      logger.debug(s"Got $totalAirlines from Kafka Consumer")
      forwardMessage(ApiEvent(totalAirlines.getClass.getSimpleName, totalAirlines).toJson.toString)

  }

  override def preStart(): Unit = {
    super.preStart()
    self ! TotalFlightUpdate
    self ! TotalAirlineUpdate
  }

}

object TotalsMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaPoller: ActorRef,
      websocketConfig: WebsocketConfig
  ): Props = Props(new TotalsMessageDispatcher(sourceActorRef, kafkaPoller, websocketConfig))
}
