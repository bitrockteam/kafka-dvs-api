package it.bitrock.kafkaflightstream.api.core

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.api.config.KafkaConfig
import it.bitrock.kafkaflightstream.api.kafka.KafkaConsumerWrapper

trait KafkaMessageProcessor extends Actor with LazyLogging {

  import context.dispatcher

  val kafkaConfig: KafkaConfig

  val kafkaConsumerWrapper: KafkaConsumerWrapper

  def throttle(f: => Unit): Unit =
    context.system.scheduler.scheduleOnce(kafkaConfig.consumer.pollInterval)(f)

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
