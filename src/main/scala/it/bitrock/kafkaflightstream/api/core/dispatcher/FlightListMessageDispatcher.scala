package it.bitrock.kafkaflightstream.api.core.dispatcher

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.FlightListUpdate
import spray.json._

class FlightListMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaMessageProcessor: ActorRef,
    val websocketConfig: WebsocketConfig
) extends MessageDispatcher {

  import context.dispatcher
  val maxNumberFlights = 1000
  val initialBox       = CoordinatesBox(49.8, -3.7, 39.7, 23.6)

  override def receive: Receive = boxing(initialBox)

  def boxing(box: CoordinatesBox): Receive = {

    case Terminated => self ! PoisonPill

    case FlightListUpdate =>
      kafkaMessageProcessor ! FlightListUpdate
      context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(self ! FlightListUpdate)

    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      forwardMessage(getBoxedFlights(flights, box).toJson.toString)

    case box: CoordinatesBox =>
      context.become(boxing(box))
      self ! FlightListUpdate

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
      .take(maxNumberFlights)
      .force
    FlightReceivedList(filteredList)
  }

}

object FlightListMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaMessageProcessor: ActorRef,
      websocketConfig: WebsocketConfig
  ): Props =
    Props(new FlightListMessageDispatcher(sourceActorRef, kafkaMessageProcessor, websocketConfig))
}
