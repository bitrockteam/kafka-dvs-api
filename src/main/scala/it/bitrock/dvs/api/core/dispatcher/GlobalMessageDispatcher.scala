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
      currentBehaviors.get(FlightBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(webSocketConfig.throttleDuration)(kafkaPollerHub.flightListPoller ! FlightListUpdate)
      context.become(behaviorFor(currentBehaviors + (FlightBehavior -> BehaviorState(behaviorForFlights(box), s))))
    case StopFlightList =>
      currentBehaviors.get(FlightBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - FlightBehavior))

    case StartTop =>
      currentBehaviors.get(TopsBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(webSocketConfig.throttleDuration)(
        GlobalMessageDispatcher.TopsRequestMessages.foreach(kafkaPollerHub.topsPoller ! _)
      )
      context.become(behaviorFor(currentBehaviors + (TopsBehavior -> BehaviorState(behaviorForTops, s))))
    case StopTop =>
      currentBehaviors.get(TopsBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - TopsBehavior))

    case StartTotal =>
      currentBehaviors.get(TotalBehavior).foreach(_.scheduler.cancel())
      val s = context.system.scheduleEvery(webSocketConfig.throttleDuration)(
        GlobalMessageDispatcher.TotalsRequestMessages.foreach(kafkaPollerHub.totalsPoller ! _)
      )
      context.become(behaviorFor(currentBehaviors + (TotalBehavior -> BehaviorState(behaviorForTotals, s))))
    case StopTotal =>
      currentBehaviors.get(TotalBehavior).foreach(_.scheduler.cancel())
      context.become(behaviorFor(currentBehaviors - TotalBehavior))

    case Terminated => self ! PoisonPill
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
  case object FlightBehavior extends BehaviorType
  case object TopsBehavior   extends BehaviorType
  case object TotalBehavior  extends BehaviorType

  case class ActorState(currentBehaviors: Map[BehaviorType, BehaviorState])
  case class BehaviorState(handler: Receive, scheduler: Cancellable)
}
