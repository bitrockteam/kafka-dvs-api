package it.bitrock.dvs.api.core.poller

import akka.actor.{Actor, Cancellable}
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.kafka.KafkaConsumerWrapper

trait KafkaPoller extends Actor with LazyLogging {

  import context.dispatcher

  val kafkaConfig: KafkaConfig

  val kafkaConsumerWrapper: KafkaConsumerWrapper

  private lazy val scheduledPoll: Cancellable =
    context.system.scheduler.scheduleAtFixedRate(kafkaConfig.consumer.pollInterval, kafkaConfig.consumer.pollInterval)(() =>
      kafkaConsumerWrapper.pollMessages()
    )

  override def preStart(): Unit = {
    super.preStart()
    logger.debug("Starting kafka message processor")
    scheduledPoll
  }

  override def postStop(): Unit = {
    logger.debug("Stopping kafka message processor")
    scheduledPoll.cancel()
    kafkaConsumerWrapper.close()
    super.postStop()
  }
}
