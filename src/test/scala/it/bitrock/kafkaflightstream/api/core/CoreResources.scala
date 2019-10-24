package it.bitrock.kafkaflightstream.api.core

import akka.actor.ActorRef
import akka.testkit.TestProbe
import it.bitrock.kafkaflightstream.api.config.{KafkaConfig, WebsocketConfig}
import it.bitrock.kafkaflightstream.api.kafka.{KafkaConsumerWrapper, KafkaConsumerWrapperFactory}

object CoreResources {

  final case class ResourceMessageDispatcher(
      websocketConfig: WebsocketConfig,
      kafkaConfig: KafkaConfig,
      consumerFactory: KafkaConsumerWrapperFactory,
      pollProbe: TestProbe,
      sourceProbe: TestProbe
  )

  final case class ResourceKafkaPollerCache(
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
