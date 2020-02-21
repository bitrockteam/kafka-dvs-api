package it.bitrock.dvs.api.core.poller

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper
import it.bitrock.dvs.api.ActorSystemOps

trait KafkaPoller extends Actor with LazyLogging {

  import context.dispatcher

  val kafkaConfig: KafkaConfig

  val kafkaConsumerWrapper: KafkaConsumerWrapper

  def schedulePoll(): Unit =
    context.system.scheduleOnce(kafkaConfig.consumer.pollInterval)(kafkaConsumerWrapper.pollMessages())

  override def preStart(): Unit = {
    super.preStart()
    logger.debug("Starting kafka message processor")
    kafkaConsumerWrapper.pollMessages()
  }

  override def postStop(): Unit = {
    logger.debug("Stopping kafka message processor")
    kafkaConsumerWrapper.close()
    super.postStop()
  }
}
