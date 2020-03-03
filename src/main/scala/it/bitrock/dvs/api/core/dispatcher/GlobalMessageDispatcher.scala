package it.bitrock.dvs.api.core.dispatcher

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Cancellable, PoisonPill, Props, Terminated}
import it.bitrock.dvs.api.ActorSystemOps
import it.bitrock.dvs.api.JsonSupport._
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.core.dispatcher.GlobalMessageDispatcher._
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.model._
import spray.json._

class GlobalMessageDispatcher(val sourceActorRef: ActorRef, kafkaPollerHub: KafkaPollerHub, val webSocketConfig: WebSocketConfig)
    extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = behaviorFor(Map.empty)

  private def behaviorFor(behaviors: Map[BehaviorType, BehaviorState]): Receive =
    behaviors.values.map(_.handler).fold(commands(behaviors))((pf1, pf2) => pf1.orElse(pf2))

  private def behaviorForFlights(box: CoordinatesBox): Receive = {
    case flights: FlightReceivedList =>
      logger.debug(s"Got a $flights from Kafka Consumer")
      forwardMessage(ApiEvent(EventType.from(flights), getBoxedFlights(flights, box)).toJson.toString)
    case airport: AirportList =>
      logger.debug(s"Got a $airport from Kafka Consumer")
      forwardMessage(ApiEvent(EventType.from(airport), getBoxedAirport(airport, box)).toJson.toString)
  }

  private def behaviorForTops: Receive = {
    case e: TopEventPayload =>
      logger.debug(s"Got a $e from Kafka Consumer")
      forwardMessage(ApiEvent(EventType.from(e), e.asInstanceOf[EventPayload]).toJson.toString)
  }

  private def behaviorForTotals: Receive = {
    case e: TotalEventPayload =>
      logger.debug(s"Got a $e from Kafka Consumer")
      forwardMessage(ApiEvent(EventType.from(e), e.asInstanceOf[EventPayload]).toJson.toString)
  }

  private def commands(currentBehaviors: Map[BehaviorType, BehaviorState]): Receive = {
    case box: CoordinatesBox =>
      currentBehaviors.get(FlightsBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(box.updateRate.getOrElse(webSocketConfig.throttleDuration))(
        kafkaPollerHub.flightListPoller ! FlightListUpdate
      )
      kafkaPollerHub.flightListPoller ! AirportListUpdate
      context.become(behaviorFor(currentBehaviors + (FlightsBehavior -> BehaviorState(behaviorForFlights(box), s))))
    case StopFlightList =>
      currentBehaviors.get(FlightsBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - FlightsBehavior))

    case StartTops(updateRate) =>
      currentBehaviors.get(TopsBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(updateRate.getOrElse(webSocketConfig.throttleDuration))(
        GlobalMessageDispatcher.TopsRequestMessages.foreach(kafkaPollerHub.topsPoller ! _)
      )
      context.become(behaviorFor(currentBehaviors + (TopsBehavior -> BehaviorState(behaviorForTops, s))))
    case StopTops =>
      currentBehaviors.get(TopsBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - TopsBehavior))

    case StartTotals(updateRate) =>
      currentBehaviors.get(TotalsBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(updateRate.getOrElse(webSocketConfig.throttleDuration))(
        GlobalMessageDispatcher.TotalsRequestMessages.foreach(kafkaPollerHub.totalsPoller ! _)
      )
      context.become(behaviorFor(currentBehaviors + (TotalsBehavior -> BehaviorState(behaviorForTotals, s))))
    case StopTotals =>
      currentBehaviors.get(TotalsBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - TotalsBehavior))

    case Terminated => self ! PoisonPill
  }

  private def getBoxedFlights(flights: FlightReceivedList, box: CoordinatesBox): FlightReceivedList = {
    val filteredList = flights.elements.view.filter { flight =>
      coordinateBoxContainsCoordinates(box, flight.geography.latitude, flight.geography.longitude)
    }.take(webSocketConfig.maxNumberFlights).force
    FlightReceivedList(filteredList)
  }

  private def getBoxedAirport(airports: AirportList, box: CoordinatesBox): AirportList = {
    val filteredList = airports.elements.view.filter { airport =>
      coordinateBoxContainsCoordinates(box, airport.latitude, airport.longitude)
    }.take(webSocketConfig.maxNumberAirports).force
    AirportList(filteredList)
  }

  private def coordinateBoxContainsCoordinates(box: CoordinatesBox, lat: Double, lng: Double): Boolean =
    lat < box.leftHighLat && lat > box.rightLowLat && lng > box.leftHighLon && lng < box.rightLowLon

}

object GlobalMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaPollerHub: KafkaPollerHub,
      webSocketConfig: WebSocketConfig
  ): Props =
    Props(new GlobalMessageDispatcher(sourceActorRef, kafkaPollerHub, webSocketConfig))

  private val TopsRequestMessages   = List(TopArrivalAirportUpdate, TopDepartureAirportUpdate, TopSpeedUpdate, TopAirlineUpdate)
  private val TotalsRequestMessages = List(TotalFlightUpdate, TotalAirlineUpdate)

  sealed trait BehaviorType
  case object FlightsBehavior extends BehaviorType
  case object TopsBehavior    extends BehaviorType
  case object TotalsBehavior  extends BehaviorType

  case class ActorState(currentBehaviors: Map[BehaviorType, BehaviorState])
  case class BehaviorState(handler: Receive, scheduler: Cancellable)
}
