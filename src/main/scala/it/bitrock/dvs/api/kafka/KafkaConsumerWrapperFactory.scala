package it.bitrock.dvs.api.kafka

import akka.actor.ActorRef
import it.bitrock.dvs.api.config.KafkaConfig
import it.bitrock.dvs.api.definitions.DefinitionsConversions._
import it.bitrock.dvs.model.avro._
import it.bitrock.kafkacommons.serialization.AvroSerdes.serdeFrom
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serdes}

trait KafkaConsumerWrapperFactory {
  def build(processor: ActorRef, topics: Seq[String]): KafkaConsumerWrapper
}

object KafkaConsumerWrapperFactory {
  lazy val byteArrayDeserializer: Deserializer[Array[Byte]] = Serdes.ByteArray.deserializer

  def flightListKafkaConsumerFactory(kafkaConfig: KafkaConfig): KafkaConsumerWrapperFactory =
    (processor: ActorRef, topics: Seq[String]) =>
      new KafkaConsumerWrapperImpl(
        kafkaConfig,
        processor,
        topics,
        (record: FlightReceivedList) => record.toFlightReceivedList
      )(byteArrayDeserializer, serdeFrom[FlightReceivedList](kafkaConfig.schemaRegistryUrl).deserializer)

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
            case airline: TopAirlineList                   => airline.toTopAirlineList
          }
      )(byteArrayDeserializer, serdeFrom[SpecificRecord](kafkaConfig.schemaRegistryUrl).deserializer)

  def totalsKafkaConsumerFactory(kafkaConfig: KafkaConfig): KafkaConsumerWrapperFactory =
    (processor: ActorRef, topics: Seq[String]) =>
      new KafkaConsumerWrapperImpl(
        kafkaConfig,
        processor,
        topics,
        (r: SpecificRecord) =>
          r match {
            case countFlight: CountFlight   => countFlight.toCountFlight
            case countAirline: CountAirline => countAirline.toCountAirline
          }
      )(byteArrayDeserializer, serdeFrom[SpecificRecord](kafkaConfig.schemaRegistryUrl).deserializer)

}
