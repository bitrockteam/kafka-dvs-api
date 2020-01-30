package it.bitrock.dvs.api.core.dispatcher

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.ActorSystemOps
import it.bitrock.dvs.api.JsonSupport._
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.model.{ApiEvent, CoordinatesBox, EventPayload, FlightReceivedList, KafkaPollerHub}
import spray.json._

class GlobalMessageDispatcher(val sourceActorRef: ActorRef, kafkaPollerHub: KafkaPollerHub, val webSocketConfig: WebSocketConfig)
    extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = unboxedBehavior

  private def unboxedBehavior: Receive = {
    case Terminated => self ! PoisonPill

    case FlightListUpdate =>
      kafkaPollerHub.flightListPoller ! FlightListUpdate
      context.system.scheduleOnce(webSocketConfig.throttleDuration)(self ! FlightListUpdate)

    case topUpdate @ (TopArrivalAirportUpdate | TopDepartureAirportUpdate | TopSpeedUpdate | TopAirlineUpdate) =>
      kafkaPollerHub.topsPoller ! topUpdate
      context.system.scheduleOnce(webSocketConfig.throttleDuration)(self ! topUpdate)

    case totalUpdate @ (TotalFlightUpdate | TotalAirlineUpdate) =>
      kafkaPollerHub.totalsPoller ! totalUpdate
      context.system.scheduleOnce(webSocketConfig.throttleDuration)(self ! totalUpdate)

    case box: CoordinatesBox =>
      context.become(boxedBehavior(box))
      kafkaPollerHub.flightListPoller ! FlightListUpdate

    case e: EventPayload =>
      logger.debug(s"Got a $e from Kafka Consumer")
      forwardMessage(ApiEvent(e.getClass.getSimpleName, e).toJson.toString)
  }

  private def boxedBehavior(box: CoordinatesBox): Receive = unboxedBehavior.orElse {
    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      forwardMessage(getBoxedFlights(flights, box).toJson.toString)
  }

  private def getBoxedFlights(flights: FlightReceivedList, box: CoordinatesBox): FlightReceivedList = {
    val filteredList = flights.elements.view.filter { flight =>
      val coordinate = flight.geography
      coordinate.latitude < box.leftHighLat &&
      coordinate.latitude > box.rightLowLat &&
      coordinate.longitude > box.leftHighLon &&
      coordinate.longitude < box.rightLowLon
    }.take(webSocketConfig.maxNumberFlights).force
    FlightReceivedList(filteredList)
  }

  override def preStart(): Unit = {
    super.preStart()
    GlobalMessageDispatcher.StartupMessages.foreach(self ! _)
  }

}

object GlobalMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaPollerHub: KafkaPollerHub,
      webSocketConfig: WebSocketConfig
  ): Props =
    Props(new GlobalMessageDispatcher(sourceActorRef, kafkaPollerHub, webSocketConfig))

  private val FlightListRequestMessages    = List(FlightListUpdate)
  private val TopsRequestMessages          = List(TopArrivalAirportUpdate, TopDepartureAirportUpdate, TopSpeedUpdate, TopAirlineUpdate)
  private val TotalsRequestMessages        = List(TotalFlightUpdate, TotalAirlineUpdate)
  val StartupMessages: List[MessageUpdate] = FlightListRequestMessages ++ TopsRequestMessages ++ TotalsRequestMessages

}
