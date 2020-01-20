package it.bitrock.dvs.api.core.dispatcher

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.ActorSystemOps
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper.FlightListUpdate
import it.bitrock.dvs.api.model._
import spray.json._

class FlightListMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaPoller: ActorRef,
    val webSocketConfig: WebSocketConfig
) extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = commands

  private def boxing(box: CoordinatesBox): Receive = commands.orElse(query(box))

  private def commands: Receive = {
    case Terminated => self ! PoisonPill

    case FlightListUpdate =>
      kafkaPoller ! FlightListUpdate
      context.system.scheduleOnce(webSocketConfig.throttleDuration)(self ! FlightListUpdate)

    case box: CoordinatesBox =>
      context.become(boxing(box))
      kafkaPoller ! FlightListUpdate
  }

  private def query(box: CoordinatesBox): Receive = {
    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      forwardMessage(getBoxedFlights(flights, box).toJson.toString)
  }

  private def getBoxedFlights(flights: FlightReceivedList, box: CoordinatesBox): FlightReceivedList = {
    val filteredList = flights.elements.view
      .filter { flight =>
        val coordinate = flight.geography
        coordinate.latitude < box.leftHighLat &&
        coordinate.latitude > box.rightLowLat &&
        coordinate.longitude > box.leftHighLon &&
        coordinate.longitude < box.rightLowLon
      }
      .take(webSocketConfig.maxNumberFlights)
      .force
    FlightReceivedList(filteredList)
  }

  override def preStart(): Unit = {
    super.preStart()
    self ! FlightListUpdate
  }

}

object FlightListMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaPoller: ActorRef,
      webSocketConfig: WebSocketConfig
  ): Props =
    Props(new FlightListMessageDispatcher(sourceActorRef, kafkaPoller, webSocketConfig))
}
