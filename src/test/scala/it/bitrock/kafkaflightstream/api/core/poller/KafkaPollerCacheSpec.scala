package it.bitrock.kafkaflightstream.api.core.poller

import akka.actor.ActorRef
import akka.testkit.TestProbe
import it.bitrock.kafkaflightstream.api.config.KafkaConfig
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}

object KafkaPollerCacheSpec {

  final case class Resource(
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      pollProbe: TestProbe
  )

  case object PollingTriggered

  class TestKafkaConsumerWrapperFactory(pollActorRef: ActorRef) extends KafkaConsumerWrapperFactory {

    override def build(processor: ActorRef, topics: Seq[String] = List()): KafkaConsumerWrapper = new KafkaConsumerWrapper {

      override def pollMessages(): Unit =
        pollActorRef ! PollingTriggered

      override def close(): Unit = ()

      override val maxPollRecords: Int = 1

      override def moveTo(epoch: Long): Unit = ()

      override def pause(): Unit = ()

      override def resume(): Unit = ()
    }

  }
}
