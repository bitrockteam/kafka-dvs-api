package it.bitrock.dvs.api.core.dispatcher

import akka.actor._
import it.bitrock.dvs.api.ActorSystemOps
import it.bitrock.dvs.api.config.WebSocketConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper._
import it.bitrock.dvs.api.model._
import spray.json._

class TopsMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaPoller: ActorRef,
    val webSocketConfig: WebSocketConfig
) extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = {
    case Terminated => self ! PoisonPill

    case topUpdate @ (TopArrivalAirportUpdate | TopDepartureAirportUpdate | TopSpeedUpdate | TopAirlineUpdate) =>
      kafkaPoller ! topUpdate
      context.system.scheduleOnce(webSocketConfig.throttleDuration)(self ! topUpdate)

    case topList: TopArrivalAirportList =>
      logger.debug(s"Got a $topList from Kafka Consumer")
      forwardMessage(ApiEvent(topList.getClass.getSimpleName, topList).toJson.toString)

    case topList: TopDepartureAirportList =>
      logger.debug(s"Got a $topList from Kafka Consumer")
      forwardMessage(ApiEvent(topList.getClass.getSimpleName, topList).toJson.toString)

    case topList: TopSpeedList =>
      logger.debug(s"Got a $topList from Kafka Consumer")
      forwardMessage(ApiEvent(topList.getClass.getSimpleName, topList).toJson.toString)

    case topList: TopAirlineList =>
      logger.debug(s"Got a $topList from Kafka Consumer")
      forwardMessage(ApiEvent(topList.getClass.getSimpleName, topList).toJson.toString)
  }

  override def preStart(): Unit = {
    super.preStart()
    self ! TopArrivalAirportUpdate
    self ! TopDepartureAirportUpdate
    self ! TopSpeedUpdate
    self ! TopAirlineUpdate
  }

}

object TopsMessageDispatcher {
  def props(
      sourceActorRef: ActorRef,
      kafkaPoller: ActorRef,
      webSocketConfig: WebSocketConfig
  ): Props = Props(new TopsMessageDispatcher(sourceActorRef, kafkaPoller, webSocketConfig))
}
