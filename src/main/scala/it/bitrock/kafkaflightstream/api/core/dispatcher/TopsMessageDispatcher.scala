package it.bitrock.kafkaflightstream.api.core.dispatcher

import akka.actor._
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.definitions._
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper._
import spray.json._

class TopsMessageDispatcher(
    val sourceActorRef: ActorRef,
    kafkaPoller: ActorRef,
    val websocketConfig: WebsocketConfig
) extends MessageDispatcher {

  import context.dispatcher

  override def receive: Receive = {
    case Terminated => self ! PoisonPill

    case topUpdate @ (TopArrivalAirportUpdate | TopDepartureAirportUpdate | TopSpeedUpdate | TopAirlineUpdate) =>
      kafkaPoller ! topUpdate
      context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(self ! topUpdate)

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
      websocketConfig: WebsocketConfig
  ): Props = Props(new TopsMessageDispatcher(sourceActorRef, kafkaPoller, websocketConfig))
}
