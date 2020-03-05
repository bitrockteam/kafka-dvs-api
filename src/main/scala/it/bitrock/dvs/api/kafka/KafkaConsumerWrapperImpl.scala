package it.bitrock.dvs.api.kafka

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.api._
import it.bitrock.dvs.api.config.KafkaConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class KafkaConsumerWrapperImpl[K: Deserializer, V: Deserializer](
    conf: KafkaConfig,
    processor: ActorRef,
    topics: Seq[String],
    transformer: V => Any
) extends KafkaConsumerWrapper
    with LazyLogging {

  val kafkaConsumer: KafkaConsumer[K, V] = getKafkaConsumer(conf, topics)

  // Startup rewind
  moveTo(System.currentTimeMillis - conf.consumer.startupRewind.toMillis)

  override def pollMessages(): Unit = {
    logger.debug("Going to poll for records")
    val results = kafkaConsumer.poll(duration2JavaDuration(conf.consumer.pollInterval)).asScala
    logger.debug(s"Got ${results.size} records")
    results
      .map(record => transformer(record.value))
      .foreach(processor ! _)
  }

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  override def moveTo(epoch: Long): Unit = {
    @tailrec
    def waitForAssignment(retries: Int): Map[TopicPartition, Long] = {
      kafkaConsumer.poll(duration2JavaDuration(conf.consumer.pollInterval))
      kafkaConsumer.assignment.asScala.map((_, epoch)).toMap match {
        case queryForTimestamp if queryForTimestamp.nonEmpty => queryForTimestamp
        case _ if retries == 0 =>
          logger.warn(s"Unable to retrieve partition assignment for Kafka Consumer on topics $topics: retries exhausted")
          Map.empty
        case _ => waitForAssignment(retries - 1)
      }
    }

    val queryForTimestamp = waitForAssignment(50)
      .mapValues(long2Long)
      .asJava
    val offsetsForTimes = kafkaConsumer.offsetsForTimes(queryForTimestamp)

    offsetsForTimes.asScala.foreach {
      case (tp, null) => logger.warn(s"No offset found for timestamp for tp $tp")
      case (tp, t)    => kafkaConsumer.seek(tp, t.offset)
    }
  }

  override def close(): Unit = {
    logger.debug("Going to close Kafka Consumer")
    kafkaConsumer.close(duration2JavaDuration(1.second))
  }

  override def pause(): Unit = {
    logger.debug("Going to pause Kafka Consumer")
    kafkaConsumer.pause(kafkaConsumer.assignment)
  }

  override def resume(): Unit = {
    logger.debug("Going to resume Kafka Consumer")
    kafkaConsumer.resume(kafkaConsumer.paused)
  }

}
