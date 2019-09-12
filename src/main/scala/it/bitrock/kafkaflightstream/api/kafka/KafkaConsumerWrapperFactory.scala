package it.bitrock.kafkaflightstream.api.kafka

import akka.actor.ActorRef
import it.bitrock.kafkaflightstream.model._
import it.bitrock.kafkaflightstream.api.config.KafkaConfig
import it.bitrock.kafkaflightstream.api.definitions.DefinitionsConversions._
import it.bitrock.kafkageostream.kafkacommons.serialization.AvroSerdes.serdeFrom
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serdes}

trait KafkaConsumerWrapperFactory {
  def build(processor: ActorRef, topics: Seq[String]): KafkaConsumerWrapper
}

object KafkaConsumerWrapperFactory {
  lazy val byteArrayDeserializer: Deserializer[Array[Byte]] = Serdes.ByteArray.deserializer

  def flightKafkaConsumerFactory(kafkaConfig: KafkaConfig): KafkaConsumerWrapperFactory =
    (processor: ActorRef, topics: Seq[String]) =>
      new KafkaConsumerWrapperImpl(
        kafkaConfig,
        processor,
        topics,
        (record: FlightEnrichedEvent) => record.toFlightReceived
      )(byteArrayDeserializer, serdeFrom[FlightEnrichedEvent](kafkaConfig.schemaRegistryUrl).deserializer)

  def topsKafkaConsumerFactory(kafkaConfig: KafkaConfig): KafkaConsumerWrapperFactory =
    (processor: ActorRef, topics: Seq[String]) =>
      new KafkaConsumerWrapperImpl(
        kafkaConfig,
        processor,
        topics,
        (r: SpecificRecord) =>
          r match {
            case arrivalAirport: TopArrivalAirportList     => arrivalAirport.toTopArrivalAirportList
            case departureAirport: TopDepartureAirportList => departureAirport.toTopDepartureAirportList
            case speed: TopSpeedList                       => speed.toTopSpeedList
          }
      )(byteArrayDeserializer, serdeFrom[SpecificRecord](kafkaConfig.schemaRegistryUrl).deserializer)

}
