package it.bitrock.dvs.api.kafka

import java.util.{Properties, UUID}

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import it.bitrock.dvs.api.config.KafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._

object KafkaConsumerWrapper {
  sealed trait MessageUpdate
  final case object NoMessage                 extends MessageUpdate
  final case object FlightListUpdate          extends MessageUpdate
  final case object TotalFlightUpdate         extends MessageUpdate
  final case object TotalAirlineUpdate        extends MessageUpdate
  final case object TopArrivalAirportUpdate   extends MessageUpdate
  final case object TopDepartureAirportUpdate extends MessageUpdate
  final case object TopSpeedUpdate            extends MessageUpdate
  final case object TopAirlineUpdate          extends MessageUpdate
}

trait KafkaConsumerWrapper {

  def uuid: String = UUID.randomUUID.toString

  def getKafkaConsumer[K: Deserializer, V: Deserializer](
      conf: KafkaConfig,
      topics: Seq[String]
  ): KafkaConsumer[K, V] = {
    val properties = new Properties
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"${conf.groupId}-$uuid") // Every consumer should be independent
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, implicitly[Deserializer[K]].getClass)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, implicitly[Deserializer[V]].getClass)
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, conf.schemaRegistryUrl.toString)
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true.toString)
    if (conf.enableInterceptors) {
      properties.put(
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      )
    }
    val kafkaConsumer = new KafkaConsumer[K, V](properties)

    kafkaConsumer.subscribe(topics.asJava)
    kafkaConsumer
  }

  def pollMessages(): Unit

  def moveTo(epoch: Long): Unit

  def close(): Unit

  def pause(): Unit

  def resume(): Unit

}
