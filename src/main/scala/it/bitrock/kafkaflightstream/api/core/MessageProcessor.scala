package it.bitrock.kafkaflightstream.api.core

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.WebsocketConfig
import it.bitrock.kafkaflightstream.api.definitions.JsonSupport

trait MessageProcessor extends Actor with JsonSupport with LazyLogging {

  val sourceActorRef: ActorRef

  val websocketConfig: WebsocketConfig

  def forwardMessage(event: String): Unit = {
    sourceActorRef ! event
  }

  override def preStart(): Unit = {
    super.preStart()

    // This actor lifecycle is bound to the sourceActor
    context watch sourceActorRef
    logger.debug("Starting message processor")
  }

  override def postStop(): Unit = {
    logger.debug("Stopping message processor")
    super.postStop()
  }
}
