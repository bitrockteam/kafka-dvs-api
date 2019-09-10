package it.bitrock.kafkaflightstream.api.core

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.definitions.JsonSupport
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper

trait MessageProcessor extends Actor with JsonSupport with LazyLogging {

  import context.dispatcher

  val sourceActorRef: ActorRef

  val websocketConfig: WebsocketConfig

  val kafkaConfig: KafkaConfig

  val kafkaConsumerWrapper: KafkaConsumerWrapper

  def throttle(f: => Unit): Unit =
    context.system.scheduler.scheduleOnce(websocketConfig.throttleDuration)(f)

  def forwardMessage(event: String): Unit = {
    sourceActorRef ! event
  }

  override def preStart(): Unit = {
    super.preStart()

    // This actor lifecycle is bound to the sourceActor
    context watch sourceActorRef

    logger.debug("Starting message processor")

    kafkaConsumerWrapper.pollMessages()
  }

  override def postStop(): Unit = {
    logger.debug("Stopping message processor")

    kafkaConsumerWrapper.close()
    super.postStop()
  }

}
