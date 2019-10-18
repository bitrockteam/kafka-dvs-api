package it.bitrock.kafkaflightstream.api.config

import java.net.URI

import scala.concurrent.duration.FiniteDuration

final case class KafkaConfig(
    bootstrapServers: String,
    schemaRegistryUrl: URI,
    groupId: String,
    flightReceivedTopic: String,
    flightReceivedListTopic: String,
    topArrivalAirportTopic: String,
    topDepartureAirportTopic: String,
    topSpeedTopic: String,
    topAirlineTopic: String,
    totalFlightTopic: String,
    totalAirlineTopic: String,
    consumer: ConsumerConfig
)

final case class ConsumerConfig(
    pollInterval: FiniteDuration,
    startupRewind: FiniteDuration
)
