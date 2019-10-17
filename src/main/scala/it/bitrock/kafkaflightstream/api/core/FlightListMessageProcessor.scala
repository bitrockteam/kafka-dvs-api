package it.bitrock.kafkaflightstream.api.core

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper.UpdateRequested
import spray.json._

object FlightListMessageProcessor {
  def props(
      sourceActorRef: ActorRef,
      kafkaMessageProcessor: ActorRef,
      websocketConfig: WebsocketConfig
  ): Props =
    Props(new FlightListMessageProcessor(sourceActorRef, kafkaMessageProcessor, websocketConfig))
}

class FlightListMessageProcessor(
    val sourceActorRef: ActorRef,
    kafkaMessageProcessor: ActorRef,
    val websocketConfig: WebsocketConfig
) extends MessageProcessor {

  import context.dispatcher
  val maxNumberFlights = 1000
  val initialBox       = CoordinatesBox(49.8, -3.7, 39.7, 23.6)

  override def receive: Receive = boxing(initialBox)

  def boxing(box: CoordinatesBox): Receive = {

    case Terminated => self ! PoisonPill

    case UpdateRequested =>
      kafkaMessageProcessor ! UpdateRequested
      context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(self ! UpdateRequested)

    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      forwardMessage(getBoxedFlights(flights, box).toJson.toString)

    case box: CoordinatesBox =>
      context.become(boxing(box))
      self ! UpdateRequested

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
